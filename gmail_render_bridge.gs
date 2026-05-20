const CONFIG = {
  renderBaseUrl: 'https://YOUR-RENDER-SERVICE.onrender.com',
  triggerSecret: 'YOUR_TRIGGER_SECRET',
  labelName: 'Reddit',
  processedLabelName: 'Reddit/Processed'
};

function processRedditCommandEmails() {
  const sourceLabelId = getOrCreateLabelId_(CONFIG.labelName);
  const processedLabelId = getOrCreateLabelId_(CONFIG.processedLabelName);
  const response = Gmail.Users.Messages.list('me', {
    labelIds: [sourceLabelId],
    maxResults: 20
  });
  const messages = response.messages || [];

  for (const messageRef of messages) {
    const message = Gmail.Users.Messages.get('me', messageRef.id, { format: 'full' });
    const plainBody = extractPlainText_(message.payload).trim();

    if (!plainBody) {
      markMessageProcessed_(message.id, sourceLabelId, processedLabelId);
      continue;
    }

    const renderResponse = UrlFetchApp.fetch(`${CONFIG.renderBaseUrl}/command`, {
      method: 'post',
      contentType: 'application/json',
      headers: {
        Authorization: `Bearer ${CONFIG.triggerSecret}`
      },
      payload: JSON.stringify({
        command: plainBody,
        gmailMessageId: message.id,
        gmailThreadId: message.threadId
      }),
      muteHttpExceptions: true
    });

    const statusCode = renderResponse.getResponseCode();
    if (statusCode >= 200 && statusCode < 300) {
      markMessageProcessed_(message.id, sourceLabelId, processedLabelId);
    } else {
      console.error(`Render /command failed for message ${message.id}: ${statusCode} ${renderResponse.getContentText()}`);
    }
  }
}

function runRenderSubscriptions() {
  const response = UrlFetchApp.fetch(`${CONFIG.renderBaseUrl}/subscriptions/run`, {
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: `Bearer ${CONFIG.triggerSecret}`
    },
    payload: JSON.stringify({ source: 'google_apps_script' }),
    muteHttpExceptions: true
  });

  if (response.getResponseCode() < 200 || response.getResponseCode() >= 300) {
    throw new Error(`Render /subscriptions/run failed: ${response.getResponseCode()} ${response.getContentText()}`);
  }
}

function getOrCreateLabelId_(labelName) {
  const labels = Gmail.Users.Labels.list('me').labels || [];
  const existing = labels.find((label) => label.name === labelName);
  if (existing) {
    return existing.id;
  }

  const created = Gmail.Users.Labels.create({
    name: labelName,
    labelListVisibility: 'labelShow',
    messageListVisibility: 'show'
  }, 'me');

  return created.id;
}

function markMessageProcessed_(messageId, sourceLabelId, processedLabelId) {
  Gmail.Users.Messages.modify({
    removeLabelIds: [sourceLabelId, 'UNREAD'],
    addLabelIds: [processedLabelId]
  }, 'me', messageId);
}

function extractPlainText_(payload) {
  if (!payload) {
    return '';
  }

  if (payload.mimeType === 'text/plain' && payload.body && payload.body.data) {
    return decodeBase64Url_(payload.body.data);
  }

  const parts = payload.parts || [];
  for (const part of parts) {
    const text = extractPlainText_(part);
    if (text) {
      return text;
    }
  }

  return '';
}

function decodeBase64Url_(value) {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/');
  return Utilities.newBlob(Utilities.base64Decode(normalized)).getDataAsString();
}

function setupTriggers() {
  ScriptApp.newTrigger('processRedditCommandEmails')
    .timeBased()
    .everyMinutes(5)
    .create();

  ScriptApp.newTrigger('runRenderSubscriptions')
    .timeBased()
    .everyHours(1)
    .create();
}
