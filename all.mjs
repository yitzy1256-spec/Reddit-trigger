import dotenv from "dotenv";
dotenv.config();

import axios from "axios";
import express from "express";
import { google } from "googleapis";
import { execFile } from "child_process";
import { promisify } from "util";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import limit from "p-limit";

const execFileAsync = (file, args, options = {}) =>
  promisify(execFile)(file, args, {
    windowsHide: true,
    ...options
  });
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Configuration
const GMAIL_CLIENT_ID = process.env.GMAIL_CLIENT_ID || "";
const GMAIL_CLIENT_SECRET = process.env.GMAIL_CLIENT_SECRET || "";
const GMAIL_REFRESH_TOKEN = process.env.GMAIL_REFRESH_TOKEN || "";
const GMAIL_SENDER = process.env.GMAIL_SENDER || process.env.GMAIL_USER || "";
const GMAIL_RECIPIENT = process.env.GMAIL_RECIPIENT || GMAIL_SENDER;
const RENDER_BASE_URL = process.env.RENDER_BASE_URL || "";

// Email size limit (20MB to be safe)
const EMAIL_SIZE_LIMIT = 25 * 1024 * 1024;
const AUTO_CHECK_INTERVAL_MS = Math.max(parseInt(process.env.AUTO_CHECK_INTERVAL_MS || "0", 10), 0);
const ENABLE_HTTP_SERVER = process.env.ENABLE_HTTP_SERVER !== "false";
const PORT = parseInt(process.env.PORT || "3000", 10);
const TRIGGER_SECRET = process.env.TRIGGER_SECRET || "";
const RSS_MIN_REQUEST_INTERVAL_MS = Math.max(parseInt(process.env.RSS_MIN_REQUEST_INTERVAL_MS || "20000", 10), 0);
const RSS_CACHE_TTL_MS = Math.max(parseInt(process.env.RSS_CACHE_TTL_MS || "300000", 10), 0);
const REDDIT_RSS_USER_AGENT = process.env.REDDIT_RSS_USER_AGENT || "reddit-media-downloader/1.0";

// Concurrent download limit
const CONCURRENT_DOWNLOADS = 3;
const downloadLimiter = limit(CONCURRENT_DOWNLOADS);

// Tracking file path
const TRACKING_FILE = path.join(__dirname, "reddit_tracker.json");
const TEMP_DIR = path.join(__dirname, ".temp");

// Automation state
let isProcessing = false;
let tempFilesCreated = [];
let lastTriggerStartedAt = null;
let lastTriggerCompletedAt = null;
let rssRequestQueue = Promise.resolve();
const rssFeedCache = new Map();

// User agents for rotation
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
];

function getRandomUserAgent() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

// ==================== UTILITY FUNCTIONS ====================

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function ensureTempDir() {
  if (!fs.existsSync(TEMP_DIR)) {
    fs.mkdirSync(TEMP_DIR, { recursive: true });
  }
}

function getTempFilePath(filename) {
  const filepath = path.join(TEMP_DIR, filename);
  tempFilesCreated.push(filepath);
  return filepath;
}

function cleanupFile(filepath) {
  try {
    if (filepath && fs.existsSync(filepath)) {
      fs.unlinkSync(filepath);
    }
  } catch (error) {
    console.error(`⚠️  Could not delete ${path.basename(filepath)}: ${error.message}`);
  }
}

function cleanupTempFiles() {
  console.log("🧹 Cleaning up temporary files...");
  
  // Get unique files
  const uniqueFiles = [...new Set(tempFilesCreated)];
  
  for (const file of uniqueFiles) {
    cleanupFile(file);
  }
  
  tempFilesCreated = [];

  // Try to remove temp directory if empty
  try {
    if (fs.existsSync(TEMP_DIR)) {
      const files = fs.readdirSync(TEMP_DIR);
      if (files.length === 0) {
        fs.rmdirSync(TEMP_DIR);
        console.log("✓ Temp directory removed");
      }
    }
  } catch (error) {
    // Directory not empty or other error - that's ok
  }
}

function buildStatusPayload() {
  return {
    ok: true,
    isProcessing,
    mailTransport: "gmail_api",
    autoCheckIntervalMs: AUTO_CHECK_INTERVAL_MS,
    lastTriggerStartedAt,
    lastTriggerCompletedAt,
    sender: GMAIL_SENDER,
    recipient: GMAIL_RECIPIENT,
    renderBaseUrl: RENDER_BASE_URL || null,
    uptimeSeconds: Math.round(process.uptime())
  };
}

function isAuthorizedTrigger(req) {
  if (!TRIGGER_SECRET) {
    return true;
  }

  const authHeader = req.headers.authorization || "";
  const bearerToken = authHeader.startsWith("Bearer ")
    ? authHeader.slice("Bearer ".length)
    : "";
  const queryToken = typeof req.query.secret === "string" ? req.query.secret : "";
  const headerToken = typeof req.headers["x-trigger-secret"] === "string"
    ? req.headers["x-trigger-secret"]
    : "";

  return bearerToken === TRIGGER_SECRET
    || queryToken === TRIGGER_SECRET
    || headerToken === TRIGGER_SECRET;
}

// ==================== TRACKING FUNCTIONS ====================


function initializeTracking() {
  if (!fs.existsSync(TRACKING_FILE)) {
    const trackingData = {
      subreddits: {},
      subscriptions: [],
      lastUpdated: new Date().toISOString()
    };
    fs.writeFileSync(TRACKING_FILE, JSON.stringify(trackingData, null, 2));
    console.log(`✓ Created tracking file: ${TRACKING_FILE}`);
  }
}

function normalizeTrackingData(trackingData) {
  const normalizedTracking = trackingData && typeof trackingData === "object"
    ? trackingData
    : {};

  if (!normalizedTracking.subreddits || typeof normalizedTracking.subreddits !== "object") {
    normalizedTracking.subreddits = {};
  }

  if (!Array.isArray(normalizedTracking.subscriptions)) {
    normalizedTracking.subscriptions = [];
  }

  normalizedTracking.subscriptions = normalizedTracking.subscriptions
    .filter((subscription) => subscription && typeof subscription === "object" && subscription.subreddit)
    .map((subscription) => ({
      subreddit: subscription.subreddit,
      mediaTypeFilter: subscription.mediaTypeFilter || null,
      dailyPostLimit: Math.min(Math.max(parseInt(subscription.dailyPostLimit || subscription.postCount || 1, 10) || 1, 1), 10),
      sentTodayCount: Math.max(parseInt(subscription.sentTodayCount || 0, 10) || 0, 0),
      currentDay: typeof subscription.currentDay === "string" ? subscription.currentDay : null,
      lastSentAt: typeof subscription.lastSentAt === "string" ? subscription.lastSentAt : null,
      createdAt: subscription.createdAt || new Date().toISOString()
    }));

  if (!normalizedTracking.lastUpdated) {
    normalizedTracking.lastUpdated = new Date().toISOString();
  }

  return normalizedTracking;
}

function loadTracking() {
  try {
    if (fs.existsSync(TRACKING_FILE)) {
      const data = fs.readFileSync(TRACKING_FILE, "utf-8");
      return normalizeTrackingData(JSON.parse(data));
    }
  } catch (error) {
    console.error("Error loading tracking file:", error.message);
  }
  return normalizeTrackingData({ subreddits: {}, subscriptions: [], lastUpdated: new Date().toISOString() });
}

function saveTracking(trackingData) {
  try {
    const normalizedTracking = normalizeTrackingData(trackingData);
    normalizedTracking.lastUpdated = new Date().toISOString();
    fs.writeFileSync(TRACKING_FILE, JSON.stringify(normalizedTracking, null, 2));
  } catch (error) {
    console.error("Error saving tracking file:", error.message);
  }
}
function getSubredditTracking(subreddit) {
  const tracking = loadTracking();
  if (!tracking.subreddits[subreddit]) {
    tracking.subreddits[subreddit] = {
      sentPostIds: [],
      lastSentDate: null,
      totalSent: 0
    };
  }
  return tracking.subreddits[subreddit];
}

function hasBeenSent(subreddit, postId) {
  const tracking = getSubredditTracking(subreddit);
  return tracking.sentPostIds.includes(postId);
}

function extractPostIdFromLink(link) {
  const parts = link.split("/");
  const commentsIndex = parts.indexOf("comments");
  if (commentsIndex !== -1 && parts[commentsIndex + 1]) {
    return parts[commentsIndex + 1];
  }
  
  return crypto.createHash('md5').update(link).digest('hex').substring(0, 8);
}
function extractRedditLinks(text) {
  const urlRegex = /(https?:\/\/(?:www\.)?reddit\.com\/r\/[A-Za-z0-9_]+\/comments\/[A-Za-z0-9_]+[^\s]*)/gi;
  const shortRegex = /(https?:\/\/(?:v|i)\.redd\.it\/[A-Za-z0-9_]+[^\s]*)/gi;

  const links = [];
  let match;

  while ((match = urlRegex.exec(text)) !== null) {
    links.push(match[1]);
  }
  while ((match = shortRegex.exec(text)) !== null) {
    links.push(match[1]);
  }

  return [...new Set(links)];
}

function updateSubredditTracking(subreddit, postIds) {
  const tracking = loadTracking();
  
  if (!tracking.subreddits[subreddit]) {
    tracking.subreddits[subreddit] = {
      sentPostIds: [],
      lastSentDate: null,
      totalSent: 0
    };
  }

  const subTracking = tracking.subreddits[subreddit];
  
  const newIds = postIds.filter(id => !subTracking.sentPostIds.includes(id));
  subTracking.sentPostIds = [...subTracking.sentPostIds, ...newIds].slice(-1000);
  
  subTracking.lastSentDate = new Date().toISOString();
  subTracking.totalSent = (subTracking.totalSent || 0) + newIds.length;

  tracking.lastUpdated = new Date().toISOString();
  saveTracking(tracking);

  if (newIds.length > 0) {
    console.log(`✓ Updated tracking for r/${subreddit}: ${newIds.length} new post(s) sent (Total: ${subTracking.totalSent})`);
  }
}

function listSubscriptions() {
  const tracking = loadTracking();
  return tracking.subscriptions || [];
}

function addSubscription(subreddit, mediaTypeFilter = null, dailyPostLimit = 1) {
  const tracking = loadTracking();
  const subscriptionKey = getSubscriptionKey(subreddit, mediaTypeFilter);
  const existingSubscription = (tracking.subscriptions || []).find(
    (subscription) => getSubscriptionKey(subscription.subreddit, subscription.mediaTypeFilter) === subscriptionKey
  );

  if (existingSubscription) {
    const normalizedDailyPostLimit = Math.min(Math.max(parseInt(dailyPostLimit, 10) || 1, 1), 10);

    if (existingSubscription.dailyPostLimit !== normalizedDailyPostLimit) {
      existingSubscription.dailyPostLimit = normalizedDailyPostLimit;
      saveTracking(tracking);
      return { added: false, updated: true, reason: "updated_limit", subscription: existingSubscription };
    }

    return { added: false, updated: false, reason: "already_exists", subscription: existingSubscription };
  }

  const subscription = {
    subreddit,
    mediaTypeFilter,
    dailyPostLimit: Math.min(Math.max(parseInt(dailyPostLimit, 10) || 1, 1), 10),
    sentTodayCount: 0,
    currentDay: null,
    lastSentAt: null,
    createdAt: new Date().toISOString()
  };

  tracking.subscriptions.push(subscription);
  saveTracking(tracking);
  return { added: true, subscription };
}

function removeSubscription(subreddit, mediaTypeFilter = null) {
  const tracking = loadTracking();
  const subscriptionKey = getSubscriptionKey(subreddit, mediaTypeFilter);
  const originalCount = tracking.subscriptions.length;

  tracking.subscriptions = tracking.subscriptions.filter(
    (subscription) => getSubscriptionKey(subscription.subreddit, subscription.mediaTypeFilter) !== subscriptionKey
  );

  if (tracking.subscriptions.length === originalCount) {
    return { removed: false, reason: "not_found" };
  }

  saveTracking(tracking);
  return { removed: true };
}

function getUtcDateKey(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

function resetSubscriptionDayIfNeeded(subscription, date = new Date()) {
  const dateKey = getUtcDateKey(date);

  if (subscription.currentDay !== dateKey) {
    subscription.currentDay = dateKey;
    subscription.sentTodayCount = 0;
  }

  return subscription;
}

function getScheduledPostsAvailableNow(subscription, date = new Date()) {
  const dailyPostLimit = Math.min(Math.max(parseInt(subscription.dailyPostLimit || 1, 10) || 1, 1), 10);
  const startOfDayUtc = Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
    0,
    0,
    0,
    0
  );
  const elapsedMs = date.getTime() - startOfDayUtc;
  const slotLengthMs = (24 * 60 * 60 * 1000) / dailyPostLimit;

  return Math.min(dailyPostLimit, Math.floor(elapsedMs / slotLengthMs) + 1);
}

function markSubscriptionSent(subscriptionKey, sentCount = 1, date = new Date()) {
  const tracking = loadTracking();
  const subscription = tracking.subscriptions.find(
    (item) => getSubscriptionKey(item.subreddit, item.mediaTypeFilter) === subscriptionKey
  );

  if (!subscription) {
    return;
  }

  resetSubscriptionDayIfNeeded(subscription, date);
  subscription.sentTodayCount = Math.max(parseInt(subscription.sentTodayCount || 0, 10) || 0, 0) + sentCount;
  subscription.lastSentAt = date.toISOString();
  saveTracking(tracking);
}

function shouldProcessSubscription(subscription, date = new Date()) {
  resetSubscriptionDayIfNeeded(subscription, date);

  const dailyPostLimit = Math.min(Math.max(parseInt(subscription.dailyPostLimit || 1, 10) || 1, 1), 10);
  if ((subscription.sentTodayCount || 0) >= dailyPostLimit) {
    return false;
  }

  const scheduledPostsAvailable = getScheduledPostsAvailableNow(subscription, date);
  return (subscription.sentTodayCount || 0) < scheduledPostsAvailable;
}
// ==================== GMAIL API FUNCTIONS ====================

function getGmailClient() {
  if (!GMAIL_CLIENT_ID || !GMAIL_CLIENT_SECRET || !GMAIL_REFRESH_TOKEN || !GMAIL_SENDER) {
    throw new Error("GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN, and GMAIL_SENDER must be set");
  }

  const auth = new google.auth.OAuth2(GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET);
  auth.setCredentials({ refresh_token: GMAIL_REFRESH_TOKEN });

  return google.gmail({ version: "v1", auth });
}

function wrapBase64(base64Value) {
  return base64Value.replace(/(.{76})/g, "$1\r\n");
}

function toBase64Url(value) {
  return Buffer.from(value, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function buildMimeMessage({ from, to, subject, text, attachments }) {
  const boundary = "reddit-media-" + Date.now() + "-" + Math.random().toString(16).slice(2);
  const lines = [
    `From: ${from}`,
    `To: ${to}`,
    `Subject: ${subject}`,
    "MIME-Version: 1.0",
    `Content-Type: multipart/mixed; boundary=\"${boundary}\"`,
    "",
    `--${boundary}`,
    "Content-Type: text/plain; charset=\"UTF-8\"",
    "Content-Transfer-Encoding: 7bit",
    "",
    text,
    ""
  ];

  for (const attachment of attachments) {
    lines.push(`--${boundary}`);
    lines.push(`Content-Type: ${attachment.contentType || "application/octet-stream"}; name=\"${attachment.filename}\"`);
    lines.push("Content-Transfer-Encoding: base64");
    lines.push(`Content-Disposition: attachment; filename=\"${attachment.filename}\"`);
    lines.push("");
    lines.push(wrapBase64(attachment.content.toString("base64")));
    lines.push("");
  }

  lines.push(`--${boundary}--`, "");
  return lines.join("\r\n");
}

async function sendViaGmailApi({ subject, text, attachments }) {
  const gmail = getGmailClient();
  const raw = buildMimeMessage({
    from: GMAIL_SENDER,
    to: GMAIL_RECIPIENT,
    subject,
    text,
    attachments
  });

  await gmail.users.messages.send({
    userId: "me",
    requestBody: {
      raw: toBase64Url(raw)
    }
  });
}

async function runSubscriptionChecks() {
  const subscriptions = listSubscriptions();
  let processedSubscriptions = 0;

  for (const subscription of subscriptions) {
    await processSubscription(subscription);
    processedSubscriptions += 1;
  }

  return { processedSubscriptions };
}

async function processCommandText(commandText) {
  const command = parseRequestCommand(commandText);
  if (!command || !command.subreddit) {
    return { handled: false, error: "invalid_command" };
  }

  return handleParsedCommand(command);
}
function sanitizeFilename(filename) {
  return filename
    .replace(/[^a-zA-Z0-9\s\-_]/g, "")
    .replace(/\s+/g, "_")
    .substring(0, 200);
}

function normalizeMediaTypeFilter(value) {
  const normalizedValue = (value || "").toLowerCase();

  if (["video", "videos", "mp4"].includes(normalizedValue)) {
    return "video";
  }

  if (["image", "images"].includes(normalizedValue)) {
    return "image";
  }

  if (["png", "jpg", "jpeg", "gif"].includes(normalizedValue)) {
    return normalizedValue;
  }

  return null;
}

function getSubscriptionKey(subreddit, mediaTypeFilter = null) {
  return `${subreddit.toLowerCase()}::${mediaTypeFilter || "all"}`;
}

function parseRequestCommand(body) {
  const normalizedBody = (body || "").replace(/\s+/g, " ").trim();
  if (!normalizedBody) {
    return null;
  }

  let action = "request";
  let commandBody = normalizedBody;
  const actionMatch = normalizedBody.match(/^(start|stop)\s+(.+)$/i);

  if (actionMatch) {
    action = actionMatch[1].toLowerCase();
    commandBody = actionMatch[2].trim();
  }

  commandBody = commandBody.replace(/^subreddit\s+/i, "").trim();

  let mediaTypeFilter = null;
  const leadingMediaMatch = commandBody.match(/^(image|images|video|videos|mp4|png|jpg|jpeg|gif)\s+(.+)$/i);
  if (leadingMediaMatch) {
    mediaTypeFilter = normalizeMediaTypeFilter(leadingMediaMatch[1]);
    commandBody = leadingMediaMatch[2].trim();
  }

  let match = commandBody.match(/(?:^|\b)r\/([A-Za-z0-9_]+)\b(.*)$/i);
  if (!match) {
    match = commandBody.match(/(?:^|\b)([A-Za-z0-9_]+)\b(.*)$/i);
  }

  if (!match) {
    return null;
  }

  const subreddit = match[1];
  const remainder = (match[2] || "").trim();
  let postCount = 1;

  if (remainder) {
    const tokens = remainder.split(/\s+/);

    for (const token of tokens) {
      const normalizedToken = token.toLowerCase();

      if (/^\d+$/.test(normalizedToken)) {
        postCount = Math.min(parseInt(normalizedToken, 10), 10);
        continue;
      }

      const parsedMediaType = normalizeMediaTypeFilter(normalizedToken);
      if (parsedMediaType) {
        mediaTypeFilter = parsedMediaType;
      }
    }
  }

  return {
    action,
    subreddit,
    mediaTypeFilter,
    postCount
  };
}

async function processSubredditRequest(subreddit, mediaTypeFilter = null, postCount = 1, fetchCount = null) {
  const effectiveFetchCount = fetchCount || Math.max(postCount * 10, 10);
  const posts = await findLatestPostsRSS(subreddit, mediaTypeFilter, effectiveFetchCount);

  if (!posts || posts.length === 0) {
    const filterMsg = mediaTypeFilter ? ` with type ${mediaTypeFilter}` : "";
    console.log("✗ No media found in r/" + subreddit + filterMsg);
    return { success: false, sentPostIds: [], attachments: [] };
  }

  console.log(`✓ Found ${posts.length} post(s) in RSS feed`);

  const attachments = [];
  const sentPostIds = [];
  let newPostsFound = 0;

  for (let i = 0; i < posts.length && newPostsFound < postCount; i++) {
    const post = posts[i];

    if (hasBeenSent(subreddit, post.id)) {
      console.log(`⊘ Skipping already sent post: ${post.title}`);
      continue;
    }

    console.log(`\nProcessing new post ${newPostsFound + 1}/${postCount}: ${post.title}`);

    const attachment = await downloadLimiter(() => downloadAndPreparePost(post));
    if (attachment) {
      attachments.push(attachment);
      sentPostIds.push(post.id);
      newPostsFound++;
    }
  }

  if (attachments.length === 0) {
    console.log("✗ Failed to download any new posts");
    return { success: false, sentPostIds: [], attachments: [] };
  }

  console.log(`\n✓ Successfully prepared ${attachments.length} new attachment(s)`);

  const emailResult = await sendEmailsWithAttachments(subreddit, attachments);
  if (!emailResult || emailResult.sentEmailCount === 0) {
    console.log("âœ— No email batches were sent successfully; tracking will not be updated");
    return { success: false, sentPostIds: [], attachments };
  }

  updateSubredditTracking(subreddit, sentPostIds);
  return { success: true, sentPostIds, attachments };
}

async function primeSubscription(subreddit, mediaTypeFilter = null) {
  const baselinePosts = await findLatestPostsRSS(subreddit, mediaTypeFilter, 25);
  if (baselinePosts && baselinePosts.length > 0) {
    updateSubredditTracking(subreddit, baselinePosts.map((post) => post.id));
  }
}

async function handleParsedCommand(command) {
  if (!command || !command.subreddit) {
    return { handled: false };
  }

  if (command.action === "start") {
    const result = addSubscription(command.subreddit, command.mediaTypeFilter, command.postCount);
    if (result.added) {
      await primeSubscription(command.subreddit, command.mediaTypeFilter);
      console.log(`Started subscription for r/${command.subreddit}${command.mediaTypeFilter ? ` (${command.mediaTypeFilter})` : ""} with ${command.postCount} post(s) per day`);
    } else if (result.updated) {
      console.log(`Updated subscription for r/${command.subreddit}${command.mediaTypeFilter ? ` (${command.mediaTypeFilter})` : ""} to ${command.postCount} post(s) per day`);
    } else {
      console.log(`Subscription already exists for r/${command.subreddit}${command.mediaTypeFilter ? ` (${command.mediaTypeFilter})` : ""} at ${result.subscription.dailyPostLimit} post(s) per day`);
    }
    return { handled: true, type: "subscription_start", success: true };
  }

  if (command.action === "stop") {
    const result = removeSubscription(command.subreddit, command.mediaTypeFilter);
    if (result.removed) {
      console.log(`Stopped subscription for r/${command.subreddit}${command.mediaTypeFilter ? ` (${command.mediaTypeFilter})` : ""}`);
    } else {
      console.log(`No subscription found for r/${command.subreddit}${command.mediaTypeFilter ? ` (${command.mediaTypeFilter})` : ""}`);
    }
    return { handled: true, type: "subscription_stop", success: result.removed };
  }

  console.log(`Found subreddit: ${command.subreddit}`);
  if (command.mediaTypeFilter) {
    console.log(`Media type filter: ${command.mediaTypeFilter}`);
  }
  console.log(`Requesting ${command.postCount} new post(s)`);

  const result = await processSubredditRequest(
    command.subreddit,
    command.mediaTypeFilter,
    command.postCount,
    command.postCount * 10
  );

  return { handled: true, type: "request", success: result.success };
}

async function processSubscription(subscription) {
  const label = `r/${subscription.subreddit}${subscription.mediaTypeFilter ? ` (${subscription.mediaTypeFilter})` : ""}`;
  const now = new Date();

  if (!shouldProcessSubscription(subscription, now)) {
    console.log(`Subscription ${label} is not due yet (${subscription.sentTodayCount || 0}/${subscription.dailyPostLimit || 1} sent today)`);
    return;
  }

  console.log(`Checking subscription ${label}`);

  const result = await processSubredditRequest(
    subscription.subreddit,
    subscription.mediaTypeFilter,
    1,
    Math.max((subscription.dailyPostLimit || 1) * 10, 10)
  );

  if (result.success) {
    markSubscriptionSent(
      getSubscriptionKey(subscription.subreddit, subscription.mediaTypeFilter),
      result.sentPostIds.length,
      now
    );
    console.log(`Subscription ${label} sent ${result.sentPostIds.length} new post(s)`);
  } else {
    console.log(`No new sendable posts for subscription ${label}`);
  }
}

async function processMessage(parsed, messageUid) {
  try {
    const body = parsed.text || "";
    console.log("Processing email with body:", body.substring(0, 200));

    // 1. Extract Reddit links
    const links = extractRedditLinks(body);

    if (links.length > 0) {
      console.log(`Found ${links.length} Reddit link(s)`);

      const attachments = [];

      for (const link of links) {
        const attachment = await downloadLimiter(() => processRedditLink(link));
        if (attachment) attachments.push(attachment);
      }

      if (attachments.length > 0) {
        await sendEmailsWithAttachments("reddit-links", attachments);
      }

      return messageUid;
    }

    // 2. Fallback to your existing command system
    const command = parseRequestCommand(body);
    if (!command || !command.subreddit) {
      console.log("No subreddit or links found in email");
      return messageUid;
    }

    await handleParsedCommand(command);
    return messageUid;

  } catch (error) {
    console.error("Error in processMessage:", error.message);
    return messageUid;
  }
}
async function processRedditLink(url) {
  // 1. Fetch the HTML
  const response = await axios.get(url, {
    headers: { "User-Agent": REDDIT_RSS_USER_AGENT }
  });

  const html = response.data;

  // 2. Detect media
  const videoMatch = html.match(/https:\/\/v\.redd\.it\/[A-Za-z0-9]+/);
  const imageMatch = html.match(/https:\/\/i\.redd\.it\/[A-Za-z0-9]+\.(png|jpg|jpeg|gif)/i);

  if (videoMatch) {
    return await downloadAndPreparePost({
      url: videoMatch[0],
      title: "Reddit Video",
      mediaType: "video",
      id: crypto.randomUUID()
    });
  }

  if (imageMatch) {
    return await downloadAndPreparePost({
      url: imageMatch[0],
      title: "Reddit Image",
      mediaType: imageMatch[1].toLowerCase(),
      id: crypto.randomUUID()
    });
  }

  return null;
}
// ==================== RSS & POST FUNCTIONS ====================

function getRetryDelayMs(retryAfterHeader, attemptNumber) {
  const retryAfterSeconds = parseInt(retryAfterHeader || "", 10);
  if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
    return retryAfterSeconds * 1000;
  }

  return Math.min(30000, RSS_MIN_REQUEST_INTERVAL_MS * Math.max(attemptNumber + 1, 1));
}

async function runQueuedRssRequest(task) {
  const previousQueue = rssRequestQueue;
  let releaseQueue;
  rssRequestQueue = new Promise((resolve) => {
    releaseQueue = resolve;
  });

  await previousQueue;

  try {
    return await task();
  } finally {
    if (RSS_MIN_REQUEST_INTERVAL_MS > 0) {
      await sleep(RSS_MIN_REQUEST_INTERVAL_MS);
    }
    releaseQueue();
  }
}

async function fetchSubredditRssXml(subreddit) {
  const cacheKey = subreddit.toLowerCase();
  const cachedFeed = rssFeedCache.get(cacheKey);
  const now = Date.now();

  if (cachedFeed?.xml && cachedFeed.expiresAt > now) {
    return cachedFeed.xml;
  }

  if (cachedFeed?.promise) {
    return cachedFeed.promise;
  }

  const requestPromise = runQueuedRssRequest(async () => {
    let lastStatus = null;
    let lastError = null;

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const response = await axios.get(`https://www.reddit.com/r/${subreddit}/new/.rss`, {
          headers: {
            "User-Agent": REDDIT_RSS_USER_AGENT,
            "Accept": "application/atom+xml,application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
          },
          timeout: 15000,
          validateStatus: () => true
        });

        if (response.status >= 200 && response.status < 300 && typeof response.data === "string" && response.data.trim()) {
          rssFeedCache.set(cacheKey, {
            xml: response.data,
            expiresAt: Date.now() + RSS_CACHE_TTL_MS
          });
          return response.data;
        }

        lastStatus = response.status;
        lastError = new Error(`Request failed with status code ${response.status}`);

        if (response.status === 429 && attempt < 2) {
          const delayMs = getRetryDelayMs(response.headers["retry-after"], attempt);
          console.warn(`RSS rate-limited for r/${subreddit}; retrying in ${delayMs}ms`);
          await sleep(delayMs);
          continue;
        }

        break;
      } catch (error) {
        lastError = error;

        if (attempt < 2) {
          const delayMs = getRetryDelayMs(null, attempt);
          console.warn(`RSS fetch attempt ${attempt + 1} failed for r/${subreddit}; retrying in ${delayMs}ms`);
          await sleep(delayMs);
          continue;
        }
      }
    }

    if (lastStatus === 429) {
      throw new Error(`Request failed with status code 429 for r/${subreddit}. Increase RSS pacing or reduce trigger bursts.`);
    }

    throw lastError || new Error(`Unable to fetch RSS feed for r/${subreddit}`);
  });

  rssFeedCache.set(cacheKey, {
    xml: cachedFeed?.xml || null,
    expiresAt: cachedFeed?.expiresAt || 0,
    promise: requestPromise
  });

  try {
    return await requestPromise;
  } finally {
    const currentCache = rssFeedCache.get(cacheKey);
    if (currentCache?.promise === requestPromise) {
      if (currentCache.xml && currentCache.expiresAt > Date.now()) {
        rssFeedCache.set(cacheKey, {
          xml: currentCache.xml,
          expiresAt: currentCache.expiresAt
        });
      } else {
        rssFeedCache.delete(cacheKey);
      }
    }
  }
}

async function findLatestPostsRSS(subreddit, mediaTypeFilter = null, count = 1) {
  try {
    const xml = await fetchSubredditRssXml(subreddit);
    const posts = [];
    const entryRegex = /<entry>([\s\S]*?)<\/entry>/g;
    let entryMatch;

    while ((entryMatch = entryRegex.exec(xml)) !== null) {
      const entry = entryMatch[1];

      const titleMatch = entry.match(/<title>([^<]+)<\/title>/);
      const publishedMatch = entry.match(/<published>([^<]+)<\/published>/);
      const linkMatch = entry.match(/<link href="([^"]+)"/);

      if (!titleMatch) continue;

      const title = titleMatch[1]
        .replace(/&amp;/g, "&")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'");

      const releaseDate = publishedMatch
        ? formatDate(publishedMatch[1])
        : "Unknown";

      const postLink = linkMatch ? linkMatch[1] : "";
      const postId = extractPostIdFromLink(postLink);

      const vredditMatch = entry.match(/https:\/\/v\.redd\.it\/[A-Za-z0-9]+/);
      const pngMatch = entry.match(/https:\/\/[^\s<]+\.png/i);
      const jpgMatch = entry.match(/https:\/\/[^\s<]+\.jpg/i);
      const jpegMatch = entry.match(/https:\/\/[^\s<]+\.jpeg/i);
      const gifMatch = entry.match(/https:\/\/[^\s<]+\.gif/i);

      let post = null;

      if (mediaTypeFilter === "video" || mediaTypeFilter === "mp4") {
        if (vredditMatch) {
          post = {
            url: vredditMatch[0],
            title: title,
            mediaType: "video",
            releaseDate: releaseDate,
            id: postId
          };
        }
      } else if (mediaTypeFilter === "image") {
        if (pngMatch) {
          post = {
            url: pngMatch[0],
            title: title,
            mediaType: "png",
            releaseDate: releaseDate,
            id: postId
          };
        } else if (jpgMatch) {
          post = {
            url: jpgMatch[0],
            title: title,
            mediaType: "jpg",
            releaseDate: releaseDate,
            id: postId
          };
        } else if (jpegMatch) {
          post = {
            url: jpegMatch[0],
            title: title,
            mediaType: "jpeg",
            releaseDate: releaseDate,
            id: postId
          };
        } else if (gifMatch) {
          post = {
            url: gifMatch[0],
            title: title,
            mediaType: "gif",
            releaseDate: releaseDate,
            id: postId
          };
        }
      } else if (mediaTypeFilter === "png") {
        if (pngMatch) {
          post = {
            url: pngMatch[0],
            title: title,
            mediaType: "png",
            releaseDate: releaseDate,
            id: postId
          };
        }
      } else if (mediaTypeFilter === "jpg") {
        if (jpgMatch) {
          post = {
            url: jpgMatch[0],
            title: title,
            mediaType: "jpg",
            releaseDate: releaseDate,
            id: postId
          };
        }
      } else if (mediaTypeFilter === "jpeg") {
        if (jpegMatch) {
          post = {
            url: jpegMatch[0],
            title: title,
            mediaType: "jpeg",
            releaseDate: releaseDate,
            id: postId
          };
        }
      } else if (mediaTypeFilter === "gif") {
        if (gifMatch) {
          post = {
            url: gifMatch[0],
            title: title,
            mediaType: "gif",
            releaseDate: releaseDate,
            id: postId
          };
        }
      } else {
        if (vredditMatch) {
          post = {
            url: vredditMatch[0],
            title: title,
            mediaType: "video",
            releaseDate: releaseDate,
            id: postId
          };
        } else if (pngMatch) {
          post = {
            url: pngMatch[0],
            title: title,
            mediaType: "png",
            releaseDate: releaseDate,
            id: postId
          };
        } else if (jpgMatch) {
          post = {
            url: jpgMatch[0],
            title: title,
            mediaType: "jpg",
            releaseDate: releaseDate,
            id: postId
          };
        } else if (jpegMatch) {
          post = {
            url: jpegMatch[0],
            title: title,
            mediaType: "jpeg",
            releaseDate: releaseDate,
            id: postId
          };
        } else if (gifMatch) {
          post = {
            url: gifMatch[0],
            title: title,
            mediaType: "gif",
            releaseDate: releaseDate,
            id: postId
          };
        }
      }

      if (post) {
        posts.push(post);
        if (posts.length >= count) {
          break;
        }
      }
    }

    return posts.length > 0 ? posts : null;
  } catch (error) {
    console.error("Error fetching RSS:", error.message);
    return null;
  }
}

function formatDate(dateString) {
  const date = new Date(dateString);

  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true
  }).format(date) + " EDT";
}
// ==================== VIDEO FUNCTIONS ====================

async function getDashPlaylist(id) {
  try {
    const url = `https://v.redd.it/${id}/DASHPlaylist.mpd`;

    const response = await axios.get(url, {
      headers: {
        "User-Agent": getRandomUserAgent(),
        "Accept": "application/xml,text/xml,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
      },
      timeout: 10000
    });

    return response.data;
  } catch (error) {
    console.error("Error fetching DASH playlist:", error.message);
    return null;
  }
}

function getBestVideo(xml, id) {
  try {
    const regex = /<Representation[^>]*mimeType="video\/mp4"[^>]*>[\s\S]*?<BaseURL>([^<]+)<\/BaseURL>/g;
    const matches = [];
    let match;

    while ((match = regex.exec(xml)) !== null) {
      matches.push({ file: match[1] });
    }

    if (matches.length === 0) {
      return null;
    }

    const best = matches[matches.length - 1];
    return `https://v.redd.it/${id}/${best.file}`;
  } catch (error) {
    console.error("Error parsing video streams:", error.message);
    return null;
  }
}

function getAudio(xml, id) {
  try {
    const match = xml.match(/<Representation[^>]*mimeType="audio\/mp4"[\s\S]*?<BaseURL>([^<]+)<\/BaseURL>/);

    if (!match) {
      return null;
    }

    const audioFile = match[1];
    return `https://v.redd.it/${id}/${audioFile}`;
  } catch (error) {
    console.error("Error parsing audio stream:", error.message);
    return null;
  }
}

// ==================== DOWNLOAD FUNCTIONS ====================

async function downloadAndPreparePost(post) {
  const tempFilesToClean = [];
  
  try {
    const { url, title, mediaType, releaseDate } = post;

    if (mediaType === "video") {
      if (!url.includes("v.redd.it")) {
        console.log("✗ Not a v.redd.it video");
        return null;
      }

      const id = url.split("/")[3];
      const dash = await getDashPlaylist(id);

      if (!dash) {
        console.log("✗ No DASH playlist found");
        return null;
      }

      const videoUrl = getBestVideo(dash, id);
      const audioUrl = getAudio(dash, id);

      if (!videoUrl) {
        console.log("✗ No video stream found");
        return null;
      }

      let mp4Blob = null;

      if (videoUrl && audioUrl) {
        console.log("  → Downloading video and audio...");
        const videoFile = await downloadFile(videoUrl, `video_${Date.now()}.mp4`);
        const audioFile = await downloadFile(audioUrl, `audio_${Date.now()}.m4a`);

        if (videoFile && audioFile) {
          tempFilesToClean.push(videoFile);
          tempFilesToClean.push(audioFile);

          console.log("  → Merging with FFmpeg...");
          const outputFile = getTempFilePath(`merged_${Date.now()}.mp4`);
          
          try {
            await mergeVideoAudio(videoFile, audioFile, outputFile);

            // ✅ KEY FIX: Wait for file to be fully written
            await new Promise(r => setTimeout(r, 500));

            const blob = fs.readFileSync(outputFile);
            
            if (blob.length === 0) {
              console.log("✗ Merged file is empty");
              return null;
            }

            mp4Blob = {
              data: blob,
              contentType: "video/mp4",
              filename: sanitizeFilename(title) + ".mp4",
              releaseDate: releaseDate,
              title: title
            };
          } catch (mergeError) {
            console.error("✗ Failed to merge video and audio:", mergeError.message);
            return null;
          } finally {
            // Clean up temp files after reading
            cleanupFile(videoFile);
            cleanupFile(audioFile);
            cleanupFile(outputFile);
          }
        }
      } else if (videoUrl) {
        console.log("  → Downloading video only (no audio)...");
        const videoFile = await downloadFile(videoUrl, `video_${Date.now()}.mp4`);
        if (videoFile) {
          tempFilesToClean.push(videoFile);
          
          const blob = fs.readFileSync(videoFile);
          
          if (blob.length === 0) {
            console.log("✗ Video file is empty");
            return null;
          }

          mp4Blob = {
            data: blob,
            contentType: "video/mp4",
            filename: sanitizeFilename(title) + ".mp4",
            releaseDate: releaseDate,
            title: title
          };

          cleanupFile(videoFile);
        }
      }

      return mp4Blob;
    } else if (["image", "png", "jpg", "jpeg", "gif"].includes(mediaType)) {
      console.log("  → Downloading image...");

      const imageFile = await downloadFileWithValidation(url, mediaType);

      if (imageFile && imageFile.blob) {
        const contentTypeMap = {
          png: "image/png",
          jpg: "image/jpeg",
          jpeg: "image/jpeg",
          gif: "image/gif",
          image: "image/jpeg"
        };

        return {
          data: imageFile.blob,
          contentType: imageFile.contentType || contentTypeMap[mediaType] || "image/jpeg",
          filename: sanitizeFilename(title) + "." + (imageFile.extension || mediaType),
          releaseDate: releaseDate,
          title: title
        };
      }
    }

    return null;
  } catch (error) {
    console.error("Error downloading post:", error.message);
    return null;
  } finally {
    // Final cleanup pass
    tempFilesToClean.forEach(cleanupFile);
  }
}

async function downloadFile(url, filename) {
  try {
    const response = await axios.get(url, {
      responseType: "arraybuffer",
      headers: {
        "User-Agent": getRandomUserAgent(),
        "Referer": "https://www.reddit.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
      },
      timeout: 60000,
      maxRedirects: 10
    });

    if (response.status !== 200) {
      console.log(`✗ Download failed (HTTP ${response.status})`);
      return null;
    }

    if (!response.data || response.data.length === 0) {
      console.log(`✗ Download returned empty data`);
      return null;
    }

    const filepath = getTempFilePath(filename);
    fs.writeFileSync(filepath, response.data);

    const sizeInMB = (response.data.length / 1024 / 1024).toFixed(2);
    console.log(`  → Downloaded ${filename} (${sizeInMB} MB)`);

    return filepath;
  } catch (error) {
    console.error(`✗ Error downloading ${filename}: ${error.message}`);
    return null;
  }
}

function normalizeImageUrl(url) {
  return (url || "")
    .replace(/&amp;/g, "&")
    .replace(/[\?&](width|height|crop|format|auto|quality|q)=([^&]*)/gi, "")
    .replace(/[?&]$/, "");
}

function buildImageCandidateUrls(url) {
  const normalized = normalizeImageUrl(url);
  const candidates = [];
  const addCandidate = (value) => {
    if (value && !candidates.includes(value)) {
      candidates.push(value);
    }
  };

  addCandidate(normalized);

  try {
    const parsed = new URL(normalized);

    if (parsed.hostname === "preview.redd.it") {
      addCandidate(`${parsed.protocol}//i.redd.it${parsed.pathname}`);
    }

    if (parsed.hostname.endsWith("redd.it")) {
      addCandidate(`${parsed.protocol}//${parsed.host}${parsed.pathname}`);
    }
  } catch (error) {
    // Ignore malformed URLs and use the original candidate only.
  }

  return candidates;
}

function getImageRequestProfiles() {
  return [
    {
      headers: {
        "User-Agent": getRandomUserAgent(),
        "Referer": "https://www.reddit.com/",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
      }
    },
    {
      headers: {
        "User-Agent": getRandomUserAgent(),
        "Referer": "https://old.reddit.com/",
        "Accept": "image/webp,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control": "max-age=0"
      }
    },
    {
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://i.redd.it/"
      }
    }
  ];
}

function getImageExtension(contentType, expectedType) {
  const normalizedType = (contentType || "").toLowerCase();

  if (normalizedType.includes("jpeg")) {
    return "jpg";
  }
  if (normalizedType.includes("png")) {
    return "png";
  }
  if (normalizedType.includes("gif")) {
    return "gif";
  }
  if (normalizedType.includes("webp")) {
    return "webp";
  }

  return expectedType === "image" ? "jpg" : expectedType;
}

async function downloadFileWithValidation(url, expectedType) {
  const candidateUrls = buildImageCandidateUrls(url);
  const requestProfiles = getImageRequestProfiles();
  let lastErrorMessage = null;

  for (const candidateUrl of candidateUrls) {
    for (const profile of requestProfiles) {
      try {
        const response = await axios.get(candidateUrl, {
          responseType: "arraybuffer",
          headers: profile.headers,
          timeout: 60000,
          maxRedirects: 10,
          validateStatus: () => true
        });

        if (response.status === 304) {
          console.log(`??  Image not modified (HTTP 304) - skipping`);
          return null;
        }

        if (response.status === 403 || response.status === 429 || response.status >= 500) {
          lastErrorMessage = `HTTP ${response.status}`;
          continue;
        }

        if (response.status !== 200) {
          lastErrorMessage = `HTTP ${response.status}`;
          continue;
        }

        if (!response.data || response.data.length === 0) {
          lastErrorMessage = "empty image response";
          continue;
        }

        const contentType = response.headers["content-type"];
        if (!contentType || !contentType.toLowerCase().includes("image")) {
          lastErrorMessage = `invalid content-type: ${contentType}`;
          continue;
        }

        const fileSize = response.data.length;
        console.log(`  ? Downloaded image (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);

        if (fileSize > EMAIL_SIZE_LIMIT) {
          console.log(`? Image exceeds size limit`);
          return null;
        }

        return {
          blob: Buffer.from(response.data),
          contentType: contentType.split(";")[0],
          extension: getImageExtension(contentType, expectedType)
        };
      } catch (error) {
        lastErrorMessage = error.message;
      }
    }
  }

  console.error(`? Error downloading image: ${lastErrorMessage || "all image fetch attempts failed"}`);
  return null;
}
async function mergeVideoAudio(videoFile, audioFile, outputFile) {
  try {
    await execFileAsync("ffmpeg", [
      "-i", videoFile,
      "-i", audioFile,
      "-c:v", "copy",
      "-c:a", "copy",
      "-y",
      outputFile
    ]);
  } catch (error) {
    console.error("Error merging video and audio:", error.message);
    throw error;
  }
}

// ==================== EMAIL FUNCTIONS ====================

function buildEmailBody(batch, emailIndex, totalEmails, totalAttachments) {
  let body = `📦 Reddit Media Package\n`;
  body += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (totalEmails > 1) {
    body += `📧 Email ${emailIndex + 1} of ${totalEmails}\n`;
    body += `📊 Total Attachments: ${totalAttachments}\n\n`;
  }

  body += `📋 Contents:\n`;
  body += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  batch.forEach((att, index) => {
    const sizeInMB = (att.data.length / 1024 / 1024).toFixed(2);
    const mediaType = att.contentType.includes("video") ? "🎬 VIDEO" : "🖼️  IMAGE";

    body += `${index + 1}. ${mediaType}\n`;
    body += `   📄 Name: ${att.filename}\n`;
    body += `   📏 Size: ${sizeInMB} MB\n`;
    body += `   📅 Released: ${att.releaseDate}\n`;
    body += `   📝 Title: ${att.title}\n\n`;
  });

  body += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`;
  body += `✅ Ready to view!\n`;

  return body;
}

function validateEmailSubject(subject) {
  // SMTP line length limit is 998 characters
  if (subject.length > 998) {
    return subject.substring(0, 990) + "...";
  }
  return subject;
}

async function sendEmailsWithAttachments(subreddit, allAttachments) {
  const emailBatches = [];
  let currentBatch = [];
  let currentSize = 0;
  let sentEmailCount = 0;

  for (const attachment of allAttachments) {
    const attachmentSize = attachment.data.length;

    if (currentSize + attachmentSize > EMAIL_SIZE_LIMIT && currentBatch.length > 0) {
      emailBatches.push(currentBatch);
      currentBatch = [];
      currentSize = 0;
    }

    currentBatch.push(attachment);
    currentSize += attachmentSize;
  }

  if (currentBatch.length > 0) {
    emailBatches.push(currentBatch);
  }

  console.log(`\nSending ${emailBatches.length} email(s) with ${allAttachments.length} attachment(s) total`);

  for (let emailIndex = 0; emailIndex < emailBatches.length; emailIndex++) {
    const batch = emailBatches[emailIndex];
    const attachmentsList = batch.map((att) => ({
      filename: att.filename,
      content: att.data,
      contentType: att.contentType || "application/octet-stream"
    }));

    let emailSubject = emailBatches.length > 1
      ? `Latest media from r/${subreddit} (Part ${emailIndex + 1}/${emailBatches.length})`
      : `Latest media from r/${subreddit}`;

    emailSubject = validateEmailSubject(emailSubject);
    const emailBody = buildEmailBody(batch, emailIndex, emailBatches.length, allAttachments.length);

    try {
      await sendViaGmailApi({
        subject: emailSubject,
        text: emailBody,
        attachments: attachmentsList
      });
      sentEmailCount++;
      console.log(`Email ${emailIndex + 1}/${emailBatches.length} sent successfully with ${batch.length} attachment(s)`);
    } catch (error) {
      console.error(`Error sending email ${emailIndex + 1}:`, error.message);
    }
  }

  return {
    sentEmailCount,
    totalEmails: emailBatches.length,
    allSucceeded: sentEmailCount === emailBatches.length
  };
}

// ==================== AUTOMATION FUNCTIONS ====================

async function processRedditLabel() {
  if (isProcessing) {
    console.log(`[${new Date().toLocaleTimeString()}] Work already in progress, skipping...`);
    return { started: false, reason: "already_processing" };
  }

  isProcessing = true;
  lastTriggerStartedAt = new Date().toISOString();
  console.log(`\n[${new Date().toLocaleTimeString()}] Running scheduled subscription check...`);

  try {
    const result = await runSubscriptionChecks();
    console.log(`[${new Date().toLocaleTimeString()}] Scheduled subscription check completed`);
    lastTriggerCompletedAt = new Date().toISOString();
    return { started: true, completed: true, ...result };
  } catch (error) {
    console.error(`[${new Date().toLocaleTimeString()}] Error during scheduled run:`, error.message);
    lastTriggerCompletedAt = new Date().toISOString();
    return { started: true, completed: false, error: error.message };
  } finally {
    isProcessing = false;
  }
}

function startAutomatedChecks() {
  console.log(`Automatic subscription checks enabled every ${AUTO_CHECK_INTERVAL_MS / 1000} seconds`);
  processRedditLabel();
  setInterval(processRedditLabel, AUTO_CHECK_INTERVAL_MS);
}

function startHttpServer() {
  const app = express();
  app.use(express.json({ limit: "1mb" }));

  app.get("/", (_req, res) => {
    res.json({
      service: "reddit-media-downloader",
      ...buildStatusPayload()
    });
  });

  app.get("/health", (_req, res) => {
    res.json(buildStatusPayload());
  });

  app.get("/trigger", async (req, res) => {
    if (!isAuthorizedTrigger(req)) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return;
    }

    const result = await processRedditLabel();

    res.json({
      ok: true,
      message: result.started ? "trigger_completed" : "trigger_skipped",
      result,
      status: buildStatusPayload()
    });
  });

  app.post("/command", async (req, res) => {
    if (!isAuthorizedTrigger(req)) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return;
    }

    const commandText = typeof req.body?.command === "string"
      ? req.body.command
      : typeof req.body?.body === "string"
        ? req.body.body
        : "";

    if (!commandText.trim()) {
      res.status(400).json({ ok: false, error: "missing_command" });
      return;
    }

    try {
      const result = await processCommandText(commandText);
      res.json({ ok: true, result, status: buildStatusPayload() });
    } catch (error) {
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  app.post("/subscriptions/run", async (req, res) => {
    if (!isAuthorizedTrigger(req)) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return;
    }

    try {
      const result = await processRedditLabel();
      res.json({ ok: true, result, status: buildStatusPayload() });
    } catch (error) {
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  app.listen(PORT, () => {
    console.log(`HTTP trigger server listening on port ${PORT}`);
    console.log("Health URL: /health");
    console.log("Trigger URL: /trigger");
  });
}

// ==================== MAIN ====================

async function main() {
  if (!GMAIL_CLIENT_ID || !GMAIL_CLIENT_SECRET || !GMAIL_REFRESH_TOKEN || !GMAIL_SENDER) {
    console.error(
      "Error: GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN, and GMAIL_SENDER must be set"
    );
    process.exit(1);
  }

  ensureTempDir();
  initializeTracking();
  console.log(`Tracking file: ${TRACKING_FILE}`);
  console.log(`Temp directory: ${TEMP_DIR}`);

  console.log(`Starting Reddit Media Downloader for ${GMAIL_SENDER}`);
  console.log("=========================================");
  console.log("Mode: Render webhook + Gmail API");
  if (ENABLE_HTTP_SERVER) {
    startHttpServer();
  } else {
    console.log("HTTP trigger server disabled");
  }

  if (AUTO_CHECK_INTERVAL_MS > 0) {
    startAutomatedChecks();
  } else {
    console.log("Automatic polling disabled; use Google Apps Script and/or the HTTP endpoints to run checks");
  }
}
process.on("SIGINT", () => {
  console.log("\n\nShutting down gracefully...");
  cleanupTempFiles();
  process.exit(0);
});
main()
  .then(() => {
    console.log("=========================================");
    console.log("✓ Script completed");
  })
  .catch((err) => {
    console.error("=========================================");
    console.error("✗ Fatal error:", err.message);
    process.exit(1);
  });










