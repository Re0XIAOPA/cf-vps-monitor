var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// worker.js
function getAdminConfig(env) {
  return {
    USERNAME: env.USERNAME || "admin",
    PASSWORD: env.PASSWORD || "monitor2025!"
  };
}
__name(getAdminConfig, "getAdminConfig");
function getSecurityConfig(env) {
  if (!env.JWT_SECRET || env.JWT_SECRET === "default-jwt-secret-please-set-in-worker-variables") {
    throw new Error("JWT_SECRET must be set in environment variables for security");
  }
  return {
    JWT_SECRET: env.JWT_SECRET,
    TOKEN_EXPIRY: 2 * 60 * 60 * 1e3,
    // 2小时
    MAX_LOGIN_ATTEMPTS: 5,
    LOGIN_ATTEMPT_WINDOW: 15 * 60 * 1e3,
    // 15分钟
    API_RATE_LIMIT: 60,
    // 每分钟60次
    MIN_PASSWORD_LENGTH: 8,
    ALLOWED_ORIGINS: env.ALLOWED_ORIGINS ? env.ALLOWED_ORIGINS.split(",").map((o) => o.trim()) : []
  };
}
__name(getSecurityConfig, "getSecurityConfig");
var rateLimitStore = /* @__PURE__ */ new Map();
var loginAttemptStore = /* @__PURE__ */ new Map();
var VpsBatchProcessor = class {
  static {
    __name(this, "VpsBatchProcessor");
  }
  constructor() {
    this.batchBuffer = [];
    this.lastBatch = Math.floor(Date.now() / 1e3);
    this.maxBatchSize = 100;
  }
  // 添加VPS上报数据到批量缓冲区
  addReport(serverId, reportData, batchInterval) {
    this.batchBuffer.push({
      serverId,
      timestamp: reportData.timestamp,
      cpu: JSON.stringify(reportData.cpu),
      memory: JSON.stringify(reportData.memory),
      disk: JSON.stringify(reportData.disk),
      network: JSON.stringify(reportData.network),
      uptime: reportData.uptime
    });
    const now = Math.floor(Date.now() / 1e3);
    if (now - this.lastBatch >= batchInterval || this.batchBuffer.length >= this.maxBatchSize) {
      return true;
    }
    return false;
  }
  // 获取并清空批量数据
  getBatchData() {
    const data = [...this.batchBuffer];
    this.batchBuffer = [];
    this.lastBatch = Math.floor(Date.now() / 1e3);
    return data;
  }
  // 检查是否需要定时刷新
  shouldFlush(batchInterval) {
    const now = Math.floor(Date.now() / 1e3);
    return this.batchBuffer.length > 0 && now - this.lastBatch >= batchInterval;
  }
};
var vpsBatchProcessor = new VpsBatchProcessor();
async function flushVpsBatchData(env) {
  const batchData = vpsBatchProcessor.getBatchData();
  if (batchData.length === 0) return;
  try {
    const statements = batchData.map(
      (report) => env.DB.prepare(`
        REPLACE INTO metrics (server_id, timestamp, cpu, memory, disk, network, uptime)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).bind(
        report.serverId,
        report.timestamp,
        report.cpu,
        report.memory,
        report.disk,
        report.network,
        report.uptime
      )
    );
    await env.DB.batch(statements);
    console.log(`\u6279\u91CF\u5199\u5165${batchData.length}\u6761VPS\u6570\u636E`);
  } catch (error) {
    console.error("\u6279\u91CF\u5199\u5165VPS\u6570\u636E\u5931\u8D25:", error);
    vpsBatchProcessor.batchBuffer.unshift(...batchData);
    throw error;
  }
}
__name(flushVpsBatchData, "flushVpsBatchData");
async function scheduleVpsBatchFlush(env, ctx) {
  try {
    const batchInterval = await getVpsReportInterval(env);
    if (vpsBatchProcessor.shouldFlush(batchInterval)) {
      ctx.waitUntil(flushVpsBatchData(env));
    }
  } catch (error) {
    if (vpsBatchProcessor.shouldFlush(60)) {
      ctx.waitUntil(flushVpsBatchData(env));
    }
  }
}
__name(scheduleVpsBatchFlush, "scheduleVpsBatchFlush");
var ConfigCache = class {
  static {
    __name(this, "ConfigCache");
  }
  constructor() {
    this.cache = /* @__PURE__ */ new Map();
    this.CACHE_TTL = {
      TELEGRAM: 5 * 60 * 1e3,
      // 5分钟
      MONITORING: 5 * 60 * 1e3,
      // 5分钟
      SERVERS: 2 * 60 * 1e3
      // 2分钟
    };
  }
  set(key, value, ttl) {
    this.cache.set(key, {
      value,
      timestamp: Date.now(),
      ttl
    });
  }
  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > entry.ttl) {
      this.cache.delete(key);
      return null;
    }
    return entry.value;
  }
  async getTelegramConfig(db) {
    const cached = this.get("telegram_config");
    if (cached) return cached;
    const config = await db.prepare(
      "SELECT bot_token, chat_id, enable_notifications FROM telegram_config WHERE id = 1"
    ).first();
    if (config) {
      this.set("telegram_config", config, this.CACHE_TTL.TELEGRAM);
    }
    return config;
  }
  async getMonitoringSettings(db) {
    const cached = this.get("monitoring_settings");
    if (cached) return cached;
    const settings = await db.prepare(
      'SELECT * FROM app_config WHERE key IN ("vps_report_interval", "site_check_interval")'
    ).all();
    if (settings?.results) {
      this.set("monitoring_settings", settings.results, this.CACHE_TTL.MONITORING);
      return settings.results;
    }
    return [];
  }
  async getServerList(db, isAdmin = false) {
    const cacheKey = isAdmin ? "servers_admin" : "servers_public";
    const cached = this.get(cacheKey);
    if (cached) return cached;
    let query = "SELECT id, name, description FROM servers";
    if (!isAdmin) {
      query += " WHERE is_public = 1";
    }
    query += " ORDER BY sort_order ASC NULLS LAST, name ASC";
    const { results } = await db.prepare(query).all();
    const servers = results || [];
    this.set(cacheKey, servers, this.CACHE_TTL.SERVERS);
    return servers;
  }
  clear() {
    this.cache.clear();
  }
  clearKey(key) {
    this.cache.delete(key);
  }
};
var configCache = new ConfigCache();
var taskCounter = 0;
var dbInitialized = false;
function validateSqlIdentifier(value, type) {
  const whitelist = {
    column: ["id", "name", "url", "description", "sort_order", "is_public", "last_checked", "last_status", "timestamp", "cpu", "memory", "disk", "network", "uptime"],
    table: ["servers", "monitored_sites", "metrics", "site_status_history"],
    order: ["ASC", "DESC"]
  };
  const allowed = whitelist[type];
  if (!allowed || !allowed.includes(value)) {
    throw new Error(`Invalid ${type}: ${value}`);
  }
  return value;
}
__name(validateSqlIdentifier, "validateSqlIdentifier");
function maskSensitive(value, type = "key") {
  if (!value || typeof value !== "string") return value;
  return type === "key" && value.length > 8 ? value.substring(0, 8) + "***" : "***";
}
__name(maskSensitive, "maskSensitive");
var revokedTokens = /* @__PURE__ */ new Map();
function revokeToken(token) {
  revokedTokens.set(token, Date.now());
  jwtCache.delete(token);
  if (Math.random() < 0.01) {
    const expireTime = Date.now() - 24 * 60 * 60 * 1e3;
    for (const [revokedToken, revokeTime] of revokedTokens.entries()) {
      if (revokeTime < expireTime) {
        revokedTokens.delete(revokedToken);
      }
    }
  }
}
__name(revokeToken, "revokeToken");
function isTokenRevoked(token) {
  return revokedTokens.has(token);
}
__name(isTokenRevoked, "isTokenRevoked");
async function parseJsonSafely(request, maxSize = 1024 * 1024) {
  const contentLength = request.headers.get("content-length");
  if (contentLength && parseInt(contentLength) > maxSize) {
    throw new Error("Request body too large");
  }
  const text = await request.text();
  if (text.length > maxSize) {
    throw new Error("Request body too large");
  }
  return JSON.parse(text);
}
__name(parseJsonSafely, "parseJsonSafely");
async function authenticateAdmin(request, env) {
  const user = await authenticateRequest(request, env);
  if (!user) return null;
  const adminUser = await env.DB.prepare(
    "SELECT username, locked_until FROM admin_credentials WHERE username = ?"
  ).bind(user.username).first();
  if (!adminUser || adminUser.locked_until && Date.now() < adminUser.locked_until) {
    return null;
  }
  return user;
}
__name(authenticateAdmin, "authenticateAdmin");
function extractPathSegment(path, index) {
  const segments = path.split("/");
  if (index < 0) {
    index = segments.length + index;
  }
  if (index < 0 || index >= segments.length) return null;
  const segment = segments[index];
  return segment && /^[a-zA-Z0-9_-]{1,50}$/.test(segment) ? segment : null;
}
__name(extractPathSegment, "extractPathSegment");
function extractAndValidateServerId(path) {
  return extractPathSegment(path, -1);
}
__name(extractAndValidateServerId, "extractAndValidateServerId");
function validateInput(input, type, maxLength = 255) {
  if (!input || typeof input !== "string" || input.length > maxLength) {
    return false;
  }
  const cleaned = input.trim();
  const validators = {
    serverName: /* @__PURE__ */ __name(() => {
      if (!/^[\w\s\u4e00-\u9fa5.-]{2,50}$/.test(cleaned)) return false;
      const sqlKeywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "SCRIPT", "UNION", "OR", "AND"];
      return !sqlKeywords.some((keyword) => cleaned.toUpperCase().includes(keyword));
    }, "serverName"),
    description: /* @__PURE__ */ __name(() => {
      if (cleaned.length > 500) return false;
      return !/<[^>]*>|javascript:|on\w+\s*=|<script/i.test(cleaned);
    }, "description"),
    direction: /* @__PURE__ */ __name(() => ["up", "down"].includes(input), "direction"),
    url: /* @__PURE__ */ __name(() => {
      try {
        const url = new URL(input);
        if (!["http:", "https:"].includes(url.protocol)) return false;
        const hostname = url.hostname.toLowerCase();
        if (hostname === "localhost" || hostname === "0.0.0.0" || hostname.startsWith("127.") || hostname.startsWith("10.") || hostname.startsWith("192.168.") || hostname.startsWith("169.254.") || hostname.startsWith("172.") && parseInt(hostname.split(".")[1]) >= 16 && parseInt(hostname.split(".")[1]) <= 31) {
          return false;
        }
        if (hostname.includes(":")) {
          const cleanHostname = hostname.replace(/^\[|\]$/g, "");
          if (cleanHostname === "::1" || cleanHostname.startsWith("fc") || cleanHostname.startsWith("fd") || cleanHostname.startsWith("fe80")) {
            return false;
          }
        }
        const blockedDomains = ["internal", "local", "intranet", "corp"];
        if (blockedDomains.some((domain) => hostname.includes(domain))) {
          return false;
        }
        const port = url.port;
        if (port && !["80", "443", "8080", "8443"].includes(port)) {
          return false;
        }
        return input.length <= 2048;
      } catch {
        return false;
      }
    }, "url")
  };
  return validators[type] ? validators[type]() : cleaned.length > 0;
}
__name(validateInput, "validateInput");
function createApiResponse(data, status = 200, corsHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders }
  });
}
__name(createApiResponse, "createApiResponse");
function createErrorResponse(error, message, status = 500, corsHeaders = {}, details = null) {
  const errorData = {
    error,
    message,
    timestamp: Date.now()
  };
  if (details) errorData.details = details;
  return createApiResponse(errorData, status, corsHeaders);
}
__name(createErrorResponse, "createErrorResponse");
function createSuccessResponse(data, corsHeaders = {}) {
  return createApiResponse({ success: true, ...data }, 200, corsHeaders);
}
__name(createSuccessResponse, "createSuccessResponse");
async function validateServerAuth(path, request, env) {
  const serverId = extractAndValidateServerId(path);
  if (!serverId) {
    return { error: "Invalid server ID", message: "\u65E0\u6548\u7684\u670D\u52A1\u5668ID\u683C\u5F0F" };
  }
  const apiKey = request.headers.get("X-API-Key");
  if (!apiKey) {
    return { error: "API key required", message: "\u9700\u8981API\u5BC6\u94A5" };
  }
  try {
    const serverData = await env.DB.prepare(
      "SELECT id, name, api_key FROM servers WHERE id = ?"
    ).bind(serverId).first();
    if (!serverData || serverData.api_key !== apiKey) {
      return { error: "Invalid credentials", message: "\u65E0\u6548\u7684\u670D\u52A1\u5668ID\u6216API\u5BC6\u94A5" };
    }
    return { success: true, serverId, serverData };
  } catch (error) {
    return { error: "Database error", message: "\u6570\u636E\u5E93\u67E5\u8BE2\u5931\u8D25" };
  }
}
__name(validateServerAuth, "validateServerAuth");
function handleDbError(error, corsHeaders, operation = "database operation") {
  if (error.message.includes("no such table")) {
    return createErrorResponse(
      "Database table missing",
      "\u6570\u636E\u5E93\u8868\u4E0D\u5B58\u5728\uFF0C\u8BF7\u91CD\u8BD5",
      503,
      corsHeaders
    );
  }
  return createErrorResponse(
    "Internal server error",
    "\u7CFB\u7EDF\u6682\u65F6\u4E0D\u53EF\u7528\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5",
    500,
    corsHeaders
  );
}
__name(handleDbError, "handleDbError");
var vpsIntervalCache = {
  value: null,
  timestamp: 0,
  ttl: 6e4
  // 1分钟缓存
};
async function getVpsReportInterval(env) {
  const now = Date.now();
  if (vpsIntervalCache.value !== null && now - vpsIntervalCache.timestamp < vpsIntervalCache.ttl) {
    return vpsIntervalCache.value;
  }
  try {
    const result = await env.DB.prepare(
      "SELECT value FROM app_config WHERE key = ?"
    ).bind("vps_report_interval_seconds").first();
    const interval = result?.value ? parseInt(result.value, 10) : 60;
    if (!isNaN(interval) && interval > 0) {
      vpsIntervalCache.value = interval;
      vpsIntervalCache.timestamp = now;
      return interval;
    }
  } catch (error) {
  }
  vpsIntervalCache.value = 60;
  vpsIntervalCache.timestamp = now;
  return 60;
}
__name(getVpsReportInterval, "getVpsReportInterval");
var VPS_DATA_DEFAULTS = {
  cpu: { usage_percent: 0, load_avg: [0, 0, 0] },
  memory: { total: 0, used: 0, free: 0, usage_percent: 0 },
  disk: { total: 0, used: 0, free: 0, usage_percent: 0 },
  network: { upload_speed: 0, download_speed: 0, total_upload: 0, total_download: 0 }
};
function validateAndFixVpsField(data, field) {
  if (!data || typeof data !== "object") return VPS_DATA_DEFAULTS[field];
  const converted = {};
  for (const [key, value] of Object.entries(data)) {
    converted[key] = typeof value === "string" ? parseFloat(value) || 0 : value || 0;
  }
  return converted;
}
__name(validateAndFixVpsField, "validateAndFixVpsField");
function validateAndFixVpsData(reportData) {
  const requiredFields = ["timestamp", "cpu", "memory", "disk", "network", "uptime"];
  for (const field of requiredFields) {
    if (!reportData[field]) {
      return { error: "Invalid data format", message: `\u7F3A\u5C11\u5B57\u6BB5: ${field}` };
    }
  }
  ["cpu", "memory", "disk", "network"].forEach((field) => {
    reportData[field] = validateAndFixVpsField(reportData[field], field);
  });
  reportData.timestamp = parseInt(reportData.timestamp) || Math.floor(Date.now() / 1e3);
  reportData.uptime = parseInt(reportData.uptime) || 0;
  return { success: true, data: reportData };
}
__name(validateAndFixVpsData, "validateAndFixVpsData");
async function hashPassword(password) {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const saltHex = Array.from(salt).map((b) => b.toString(16).padStart(2, "0")).join("");
  const encoder = new TextEncoder();
  let hash = encoder.encode(password + saltHex);
  for (let i = 0; i < 1e3; i++) {
    hash = new Uint8Array(await crypto.subtle.digest("SHA-256", hash));
  }
  const hashHex = Array.from(hash).map((b) => b.toString(16).padStart(2, "0")).join("");
  return `${saltHex}$${hashHex}`;
}
__name(hashPassword, "hashPassword");
async function verifyPassword(password, hashedPassword) {
  if (hashedPassword.includes("$")) {
    const [saltHex, expectedHash] = hashedPassword.split("$");
    const encoder = new TextEncoder();
    let hash = encoder.encode(password + saltHex);
    for (let i = 0; i < 1e3; i++) {
      hash = new Uint8Array(await crypto.subtle.digest("SHA-256", hash));
    }
    const computedHash = Array.from(hash).map((b) => b.toString(16).padStart(2, "0")).join("");
    return computedHash === expectedHash;
  } else {
    const encoder = new TextEncoder();
    const data = encoder.encode(password);
    const hashBuffer = await crypto.subtle.digest("SHA-256", data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const computedHash = hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
    return computedHash === hashedPassword;
  }
}
__name(verifyPassword, "verifyPassword");
var jwtCache = /* @__PURE__ */ new Map();
var JWT_CACHE_TTL = 6e4;
var MAX_CACHE_SIZE = 1e3;
function cleanupJWTCache() {
  const now = Date.now();
  for (const [key, value] of jwtCache.entries()) {
    if (now - value.timestamp > JWT_CACHE_TTL) {
      jwtCache.delete(key);
    }
  }
  if (jwtCache.size > MAX_CACHE_SIZE) {
    const entries = Array.from(jwtCache.entries());
    entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
    const toDelete = entries.slice(0, jwtCache.size - MAX_CACHE_SIZE);
    toDelete.forEach(([key]) => jwtCache.delete(key));
  }
}
__name(cleanupJWTCache, "cleanupJWTCache");
async function createJWT(payload, env) {
  const config = getSecurityConfig(env);
  const header = { alg: "HS256", typ: "JWT" };
  const now = Date.now();
  const jwtPayload = { ...payload, iat: now, exp: now + config.TOKEN_EXPIRY };
  const encodedHeader = btoa(JSON.stringify(header));
  const encodedPayload = btoa(JSON.stringify(jwtPayload));
  const data = encodedHeader + "." + encodedPayload;
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(config.JWT_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  const encodedSignature = btoa(String.fromCharCode(...new Uint8Array(signature)));
  return data + "." + encodedSignature;
}
__name(createJWT, "createJWT");
async function verifyJWTCached(token, env) {
  if (isTokenRevoked(token)) {
    jwtCache.delete(token);
    return null;
  }
  const cached = jwtCache.get(token);
  if (cached && Date.now() - cached.timestamp < JWT_CACHE_TTL) {
    if (cached.payload.exp && Date.now() > cached.payload.exp) {
      jwtCache.delete(token);
      return null;
    }
    if (isTokenRevoked(token)) {
      jwtCache.delete(token);
      return null;
    }
    return cached.payload;
  }
  const payload = await verifyJWT(token, env);
  if (payload && !isTokenRevoked(token)) {
    if (Math.random() < 0.01) {
      cleanupJWTCache();
    }
    jwtCache.set(token, {
      payload,
      timestamp: Date.now()
    });
  }
  return payload;
}
__name(verifyJWTCached, "verifyJWTCached");
async function verifyJWT(token, env) {
  try {
    if (isTokenRevoked(token)) return null;
    const config = getSecurityConfig(env);
    const [encodedHeader, encodedPayload, encodedSignature] = token.split(".");
    if (!encodedHeader || !encodedPayload || !encodedSignature) return null;
    const data = encodedHeader + "." + encodedPayload;
    const encoder = new TextEncoder();
    const key = await crypto.subtle.importKey(
      "raw",
      encoder.encode(config.JWT_SECRET),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["verify"]
    );
    const signature = Uint8Array.from(atob(encodedSignature), (c) => c.charCodeAt(0));
    const isValid = await crypto.subtle.verify("HMAC", key, signature, encoder.encode(data));
    if (!isValid) return null;
    const payload = JSON.parse(atob(encodedPayload));
    if (payload.exp && Date.now() > payload.exp) return null;
    const tokenAge = Date.now() - payload.iat;
    const halfLife = config.TOKEN_EXPIRY / 2;
    if (tokenAge > halfLife) {
      payload.shouldRefresh = true;
    }
    return payload;
  } catch (error) {
    return null;
  }
}
__name(verifyJWT, "verifyJWT");
function checkRateLimit(clientIP, endpoint, env) {
  const config = getSecurityConfig(env);
  const key = `${clientIP}:${endpoint}`;
  const now = Date.now();
  const windowStart = now - 6e4;
  if (!rateLimitStore.has(key)) {
    rateLimitStore.set(key, []);
  }
  const requests = rateLimitStore.get(key);
  const validRequests = requests.filter((timestamp) => timestamp > windowStart);
  if (validRequests.length >= config.API_RATE_LIMIT) {
    return false;
  }
  validRequests.push(now);
  rateLimitStore.set(key, validRequests);
  return true;
}
__name(checkRateLimit, "checkRateLimit");
function checkLoginAttempts(clientIP, env) {
  const config = getSecurityConfig(env);
  const now = Date.now();
  const windowStart = now - config.LOGIN_ATTEMPT_WINDOW;
  if (!loginAttemptStore.has(clientIP)) {
    loginAttemptStore.set(clientIP, []);
  }
  const attempts = loginAttemptStore.get(clientIP);
  const validAttempts = attempts.filter((timestamp) => timestamp > windowStart);
  return validAttempts.length < config.MAX_LOGIN_ATTEMPTS;
}
__name(checkLoginAttempts, "checkLoginAttempts");
function recordLoginAttempt(clientIP) {
  const now = Date.now();
  if (!loginAttemptStore.has(clientIP)) {
    loginAttemptStore.set(clientIP, []);
  }
  loginAttemptStore.get(clientIP).push(now);
}
__name(recordLoginAttempt, "recordLoginAttempt");
function getClientIP(request) {
  return request.headers.get("CF-Connecting-IP") || request.headers.get("X-Forwarded-For") || request.headers.get("X-Real-IP") || "127.0.0.1";
}
__name(getClientIP, "getClientIP");
var D1_SCHEMAS = {
  admin_credentials: `
    CREATE TABLE IF NOT EXISTS admin_credentials (
      username TEXT PRIMARY KEY,
      password_hash TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      last_login INTEGER,
      failed_attempts INTEGER DEFAULT 0,
      locked_until INTEGER DEFAULT NULL,
      must_change_password INTEGER DEFAULT 0,
      password_changed_at INTEGER DEFAULT NULL
    );`,
  servers: `
    CREATE TABLE IF NOT EXISTS servers (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      api_key TEXT NOT NULL UNIQUE,
      created_at INTEGER NOT NULL,
      sort_order INTEGER,
      last_notified_down_at INTEGER DEFAULT NULL,
      is_public INTEGER DEFAULT 1
    );`,
  metrics: `
    CREATE TABLE IF NOT EXISTS metrics (
      server_id TEXT PRIMARY KEY,
      timestamp INTEGER,
      cpu TEXT,
      memory TEXT,
      disk TEXT,
      network TEXT,
      uptime INTEGER,
      FOREIGN KEY(server_id) REFERENCES servers(id) ON DELETE CASCADE
    );`,
  monitored_sites: `
    CREATE TABLE IF NOT EXISTS monitored_sites (
      id TEXT PRIMARY KEY,
      url TEXT NOT NULL UNIQUE,
      name TEXT,
      added_at INTEGER NOT NULL,
      last_checked INTEGER,
      last_status TEXT DEFAULT 'PENDING',
      last_status_code INTEGER,
      last_response_time_ms INTEGER,
      sort_order INTEGER,
      last_notified_down_at INTEGER DEFAULT NULL,
      is_public INTEGER DEFAULT 1
    );`,
  site_status_history: `
    CREATE TABLE IF NOT EXISTS site_status_history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      site_id TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      status TEXT NOT NULL,
      status_code INTEGER,
      response_time_ms INTEGER,
      FOREIGN KEY(site_id) REFERENCES monitored_sites(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_site_status_history_site_id_timestamp ON site_status_history (site_id, timestamp DESC);`,
  telegram_config: `
    CREATE TABLE IF NOT EXISTS telegram_config (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      bot_token TEXT,
      chat_id TEXT,
      enable_notifications INTEGER DEFAULT 0,
      updated_at INTEGER
    );
    INSERT OR IGNORE INTO telegram_config (id, bot_token, chat_id, enable_notifications, updated_at) VALUES (1, NULL, NULL, 0, NULL);`,
  app_config: `
    CREATE TABLE IF NOT EXISTS app_config (
      key TEXT PRIMARY KEY,
      value TEXT
    );
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('vps_report_interval_seconds', '60');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('custom_background_enabled', 'false');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('custom_background_url', '');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('page_opacity', '80');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('show_server_section', 'true');
    INSERT OR IGNORE INTO app_config (key, value) VALUES ('show_site_section', 'true');`
};
async function ensureTablesExist(db, env) {
  try {
    const createTableStatements = Object.values(D1_SCHEMAS).map((sql) => db.prepare(sql));
    await db.batch(createTableStatements);
  } catch (error) {
  }
  await createDefaultAdmin(db, env);
  await applySchemaAlterations(db);
}
__name(ensureTablesExist, "ensureTablesExist");
async function applySchemaAlterations(db) {
  const alterStatements = [
    "ALTER TABLE monitored_sites ADD COLUMN last_notified_down_at INTEGER DEFAULT NULL",
    "ALTER TABLE servers ADD COLUMN last_notified_down_at INTEGER DEFAULT NULL",
    "ALTER TABLE metrics ADD COLUMN uptime INTEGER DEFAULT NULL",
    "ALTER TABLE admin_credentials ADD COLUMN password_hash TEXT",
    "ALTER TABLE admin_credentials ADD COLUMN created_at INTEGER",
    "ALTER TABLE admin_credentials ADD COLUMN last_login INTEGER",
    "ALTER TABLE admin_credentials ADD COLUMN failed_attempts INTEGER DEFAULT 0",
    "ALTER TABLE admin_credentials ADD COLUMN locked_until INTEGER DEFAULT NULL",
    "ALTER TABLE admin_credentials ADD COLUMN must_change_password INTEGER DEFAULT 0",
    "ALTER TABLE admin_credentials ADD COLUMN password_changed_at INTEGER DEFAULT NULL",
    "ALTER TABLE servers ADD COLUMN is_public INTEGER DEFAULT 1",
    "ALTER TABLE monitored_sites ADD COLUMN is_public INTEGER DEFAULT 1"
  ];
  for (const alterSql of alterStatements) {
    try {
      await db.exec(alterSql);
    } catch (e) {
    }
  }
}
__name(applySchemaAlterations, "applySchemaAlterations");
async function isUsingDefaultPassword(username, password, env) {
  const adminConfig = getAdminConfig(env);
  return username === adminConfig.USERNAME && password === adminConfig.PASSWORD;
}
__name(isUsingDefaultPassword, "isUsingDefaultPassword");
async function createDefaultAdmin(db, env) {
  try {
    const adminConfig = getAdminConfig(env);
    const adminExists = await db.prepare(
      "SELECT username FROM admin_credentials WHERE username = ?"
    ).bind(adminConfig.USERNAME).first();
    if (!adminExists) {
      const adminPasswordHash = await hashPassword(adminConfig.PASSWORD);
      const now = Math.floor(Date.now() / 1e3);
      await db.prepare(`
        INSERT INTO admin_credentials (username, password_hash, created_at, failed_attempts, must_change_password)
        VALUES (?, ?, ?, 0, 0)
      `).bind(adminConfig.USERNAME, adminPasswordHash, now).run();
    }
  } catch (error) {
    if (!error.message.includes("no such table")) {
      throw error;
    }
  }
}
__name(createDefaultAdmin, "createDefaultAdmin");
async function authenticateRequest(request, env) {
  const authHeader = request.headers.get("Authorization");
  if (!authHeader?.startsWith("Bearer ")) return null;
  const token = authHeader.substring(7);
  const payload = await verifyJWTCached(token, env);
  if (!payload) return null;
  if (payload.shouldRefresh) {
    const user = await env.DB.prepare(
      "SELECT username, locked_until FROM admin_credentials WHERE username = ?"
    ).bind(payload.username).first();
    if (!user || user.locked_until && Date.now() < user.locked_until) {
      return null;
    }
  }
  return payload;
}
__name(authenticateRequest, "authenticateRequest");
async function authenticateRequestOptional(request, env) {
  try {
    return await authenticateRequest(request, env);
  } catch (error) {
    return null;
  }
}
__name(authenticateRequestOptional, "authenticateRequestOptional");
function getSecureCorsHeaders(origin, env) {
  const config = getSecurityConfig(env);
  const allowedOrigins = config.ALLOWED_ORIGINS;
  let allowedOrigin = "null";
  if (allowedOrigins.length > 0 && origin) {
    if (allowedOrigins.includes(origin)) {
      allowedOrigin = origin;
    } else {
      for (const allowed of allowedOrigins) {
        if (allowed.startsWith("*.")) {
          const domain = allowed.substring(2);
          if (origin === domain || origin.endsWith(`.${domain}`)) {
            allowedOrigin = origin;
            break;
          }
        }
      }
    }
  }
  return {
    "Access-Control-Allow-Origin": allowedOrigin,
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-API-Key",
    "Access-Control-Allow-Credentials": allowedOrigin !== "null" ? "true" : "false",
    "Access-Control-Max-Age": "86400",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-XSS-Protection": "1; mode=block",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Content-Security-Policy": "default-src 'self'; script-src 'self' https://cdn.jsdelivr.net; style-src 'self' https://cdn.jsdelivr.net; img-src 'self' data: https:; font-src 'self' https://cdn.jsdelivr.net; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none';"
  };
}
__name(getSecureCorsHeaders, "getSecureCorsHeaders");
async function handleAuthRoutes(path, method, request, env, corsHeaders, clientIP) {
  if (path === "/api/auth/login" && method === "POST") {
    try {
      if (!checkLoginAttempts(clientIP, env)) {
        return createErrorResponse(
          "Too many login attempts",
          "\u767B\u5F55\u5C1D\u8BD5\u6B21\u6570\u8FC7\u591A\uFF0C\u8BF715\u5206\u949F\u540E\u518D\u8BD5",
          429,
          corsHeaders
        );
      }
      const { username, password } = await parseJsonSafely(request);
      if (!username || !password) {
        recordLoginAttempt(clientIP);
        return createErrorResponse(
          "Missing credentials",
          "\u7528\u6237\u540D\u548C\u5BC6\u7801\u4E0D\u80FD\u4E3A\u7A7A",
          400,
          corsHeaders
        );
      }
      const user = await env.DB.prepare(
        "SELECT username, password_hash, locked_until, failed_attempts FROM admin_credentials WHERE username = ?"
      ).bind(username).first();
      if (!user) {
        recordLoginAttempt(clientIP);
        return createErrorResponse(
          "Invalid credentials",
          "\u7528\u6237\u540D\u6216\u5BC6\u7801\u9519\u8BEF",
          401,
          corsHeaders
        );
      }
      if (user.locked_until && Date.now() < user.locked_until) {
        return createErrorResponse(
          "Account locked",
          "\u8D26\u6237\u5DF2\u88AB\u9501\u5B9A\uFF0C\u8BF7\u7A0D\u540E\u518D\u8BD5",
          423,
          corsHeaders
        );
      }
      const isValidPassword = await verifyPassword(password, user.password_hash);
      if (!isValidPassword) {
        recordLoginAttempt(clientIP);
        const newFailedAttempts = (user.failed_attempts || 0) + 1;
        const config = getSecurityConfig(env);
        let lockedUntil = null;
        if (newFailedAttempts >= config.MAX_LOGIN_ATTEMPTS) {
          lockedUntil = Date.now() + config.LOGIN_ATTEMPT_WINDOW;
        }
        await env.DB.prepare(
          "UPDATE admin_credentials SET failed_attempts = ?, locked_until = ? WHERE username = ?"
        ).bind(newFailedAttempts, lockedUntil, username).run();
        return createErrorResponse(
          "Invalid credentials",
          "\u7528\u6237\u540D\u6216\u5BC6\u7801\u9519\u8BEF",
          401,
          corsHeaders
        );
      }
      await env.DB.prepare(
        "UPDATE admin_credentials SET failed_attempts = 0, locked_until = NULL, last_login = ? WHERE username = ?"
      ).bind(Date.now(), username).run();
      const isUsingDefault = await isUsingDefaultPassword(username, password, env);
      const token = await createJWT({ username, usingDefaultPassword: isUsingDefault }, env);
      return createSuccessResponse({
        token,
        user: { username, usingDefaultPassword: isUsingDefault }
      }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u767B\u5F55");
    }
  }
  if (path === "/api/auth/status" && method === "GET") {
    try {
      const user = await authenticateRequest(request, env);
      if (!user) {
        return createApiResponse({ authenticated: false }, 200, corsHeaders);
      }
      const dbUser = await env.DB.prepare(
        "SELECT username FROM admin_credentials WHERE username = ?"
      ).bind(user.username).first();
      if (!dbUser) {
        return createApiResponse({ authenticated: false }, 200, corsHeaders);
      }
      return createApiResponse({
        authenticated: true,
        user: {
          username: user.username,
          usingDefaultPassword: user.usingDefaultPassword || false
        }
      }, 200, corsHeaders);
    } catch (error) {
      return createApiResponse({ authenticated: false }, 200, corsHeaders);
    }
  }
  if (path === "/api/auth/change-password" && method === "POST") {
    try {
      const user = await authenticateRequest(request, env);
      if (!user) {
        return createErrorResponse("Unauthorized", "\u9700\u8981\u767B\u5F55", 401, corsHeaders);
      }
      const { current_password, new_password } = await parseJsonSafely(request);
      if (!current_password || !new_password) {
        return createErrorResponse(
          "Missing fields",
          "\u5F53\u524D\u5BC6\u7801\u548C\u65B0\u5BC6\u7801\u4E0D\u80FD\u4E3A\u7A7A",
          400,
          corsHeaders
        );
      }
      const config = getSecurityConfig(env);
      if (new_password.length < config.MIN_PASSWORD_LENGTH) {
        return createErrorResponse(
          "Password too short",
          `\u5BC6\u7801\u957F\u5EA6\u81F3\u5C11\u4E3A${config.MIN_PASSWORD_LENGTH}\u4F4D`,
          400,
          corsHeaders
        );
      }
      const dbUser = await env.DB.prepare(
        "SELECT password_hash FROM admin_credentials WHERE username = ?"
      ).bind(user.username).first();
      if (!dbUser || !await verifyPassword(current_password, dbUser.password_hash)) {
        return createErrorResponse(
          "Invalid current password",
          "\u5F53\u524D\u5BC6\u7801\u9519\u8BEF",
          400,
          corsHeaders
        );
      }
      const newPasswordHash = await hashPassword(new_password);
      await env.DB.prepare(
        "UPDATE admin_credentials SET password_hash = ?, password_changed_at = ?, must_change_password = 0 WHERE username = ?"
      ).bind(newPasswordHash, Date.now(), user.username).run();
      const authHeader = request.headers.get("Authorization");
      if (authHeader && authHeader.startsWith("Bearer ")) {
        const currentToken = authHeader.substring(7);
        revokeToken(currentToken);
      }
      return createSuccessResponse({
        message: "\u5BC6\u7801\u4FEE\u6539\u6210\u529F\uFF0C\u8BF7\u91CD\u65B0\u767B\u5F55",
        requireReauth: true
      }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u4FEE\u6539\u5BC6\u7801");
    }
  }
  return null;
}
__name(handleAuthRoutes, "handleAuthRoutes");
async function handleServerRoutes(path, method, request, env, corsHeaders) {
  if (path === "/api/servers" && method === "GET") {
    try {
      const user = await authenticateRequestOptional(request, env);
      const isAdmin = user !== null;
      const servers = await configCache.getServerList(env.DB, isAdmin);
      return createApiResponse({ servers }, 200, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u83B7\u53D6\u670D\u52A1\u5668\u5217\u8868");
    }
  }
  if (path === "/api/admin/servers" && method === "GET") {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse("Unauthorized", "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650", 401, corsHeaders);
    }
    try {
      const { results } = await env.DB.prepare(`
        SELECT s.id, s.name, s.description, s.created_at, s.sort_order,
               s.last_notified_down_at, s.api_key, s.is_public, m.timestamp as last_report
        FROM servers s
        LEFT JOIN metrics m ON s.id = m.server_id
        ORDER BY s.sort_order ASC NULLS LAST, s.name ASC
      `).all();
      const url = new URL(request.url);
      const showFullKey = url.searchParams.get("full_key") === "true";
      const servers = (results || []).map((server) => ({
        ...server,
        api_key: showFullKey ? server.api_key : maskSensitive(server.api_key)
      }));
      return createApiResponse({ servers }, 200, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u83B7\u53D6\u7BA1\u7406\u5458\u670D\u52A1\u5668\u5217\u8868");
    }
  }
  if (path === "/api/admin/servers" && method === "POST") {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse("Unauthorized", "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650", 401, corsHeaders);
    }
    try {
      const { name, description } = await parseJsonSafely(request);
      if (!validateInput(name, "serverName")) {
        return createErrorResponse(
          "Invalid server name",
          "\u670D\u52A1\u5668\u540D\u79F0\u683C\u5F0F\u65E0\u6548",
          400,
          corsHeaders
        );
      }
      const serverId = Math.random().toString(36).substring(2, 8);
      const apiKey = Array.from(crypto.getRandomValues(new Uint8Array(32)), (b) => b.toString(16).padStart(2, "0")).join("");
      const now = Math.floor(Date.now() / 1e3);
      await env.DB.prepare(`
        INSERT INTO servers (id, name, description, api_key, created_at, sort_order, is_public)
        VALUES (?, ?, ?, ?, ?, 0, 1)
      `).bind(serverId, name, description || "", apiKey, now).run();
      configCache.clearKey("servers_admin");
      configCache.clearKey("servers_public");
      return createSuccessResponse({
        server: {
          id: serverId,
          name,
          description: description || "",
          api_key: maskSensitive(apiKey),
          created_at: now
        }
      }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u6DFB\u52A0\u670D\u52A1\u5668");
    }
  }
  if (path.match(/\/api\/admin\/servers\/[^\/]+$/) && method === "PUT") {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse("Unauthorized", "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650", 401, corsHeaders);
    }
    try {
      const serverId = extractAndValidateServerId(path);
      if (!serverId) {
        return createErrorResponse(
          "Invalid server ID",
          "\u65E0\u6548\u7684\u670D\u52A1\u5668ID\u683C\u5F0F",
          400,
          corsHeaders
        );
      }
      const { name, description } = await request.json();
      if (!validateInput(name, "serverName")) {
        return createErrorResponse(
          "Invalid server name",
          "\u670D\u52A1\u5668\u540D\u79F0\u683C\u5F0F\u65E0\u6548",
          400,
          corsHeaders
        );
      }
      const info = await env.DB.prepare(`
        UPDATE servers SET name = ?, description = ? WHERE id = ?
      `).bind(name, description || "", serverId).run();
      if (info.changes === 0) {
        return createErrorResponse("Server not found", "\u670D\u52A1\u5668\u4E0D\u5B58\u5728", 404, corsHeaders);
      }
      configCache.clearKey("servers_admin");
      configCache.clearKey("servers_public");
      return createSuccessResponse({
        id: serverId,
        name,
        description: description || "",
        message: "\u670D\u52A1\u5668\u66F4\u65B0\u6210\u529F"
      }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u66F4\u65B0\u670D\u52A1\u5668");
    }
  }
  if (path.match(/\/api\/admin\/servers\/[^\/]+$/) && method === "DELETE") {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse("Unauthorized", "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650", 401, corsHeaders);
    }
    try {
      const serverId = extractAndValidateServerId(path);
      if (!serverId) {
        return createErrorResponse(
          "Invalid server ID",
          "\u65E0\u6548\u7684\u670D\u52A1\u5668ID\u683C\u5F0F",
          400,
          corsHeaders
        );
      }
      const url = new URL(request.url);
      const confirmed = url.searchParams.get("confirm") === "true";
      if (!confirmed) {
        return createErrorResponse(
          "Confirmation required",
          "\u5220\u9664\u64CD\u4F5C\u9700\u8981\u786E\u8BA4\uFF0C\u8BF7\u6DFB\u52A0 ?confirm=true \u53C2\u6570",
          400,
          corsHeaders
        );
      }
      const info = await env.DB.prepare("DELETE FROM servers WHERE id = ?").bind(serverId).run();
      if (info.changes === 0) {
        return createErrorResponse("Server not found", "\u670D\u52A1\u5668\u4E0D\u5B58\u5728", 404, corsHeaders);
      }
      await env.DB.prepare("DELETE FROM metrics WHERE server_id = ?").bind(serverId).run();
      configCache.clearKey("servers_admin");
      configCache.clearKey("servers_public");
      return createSuccessResponse({ message: "\u670D\u52A1\u5668\u5DF2\u5220\u9664" }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u5220\u9664\u670D\u52A1\u5668");
    }
  }
  return null;
}
__name(handleServerRoutes, "handleServerRoutes");
async function handleVpsRoutes(path, method, request, env, corsHeaders, ctx) {
  if (path.startsWith("/api/config/") && method === "GET") {
    try {
      const authResult = await validateServerAuth(path, request, env);
      if (!authResult.success) {
        return createErrorResponse(
          authResult.error,
          authResult.message,
          authResult.error === "Invalid server ID" ? 400 : 401,
          corsHeaders
        );
      }
      const { serverId, serverData } = authResult;
      const reportInterval = await getVpsReportInterval(env);
      const configData = {
        success: true,
        config: {
          report_interval: reportInterval,
          enabled_metrics: ["cpu", "memory", "disk", "network", "uptime"],
          server_info: {
            id: serverData.id,
            name: serverData.name,
            description: serverData.description || ""
          }
        },
        timestamp: Math.floor(Date.now() / 1e3)
      };
      return createApiResponse(configData, 200, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u914D\u7F6E\u83B7\u53D6");
    }
  }
  if (path.startsWith("/api/report/") && method === "POST") {
    try {
      const authResult = await validateServerAuth(path, request, env);
      if (!authResult.success) {
        return createErrorResponse(
          authResult.error,
          authResult.message,
          authResult.error === "Invalid server ID" ? 400 : 401,
          corsHeaders
        );
      }
      const { serverId } = authResult;
      let reportData;
      try {
        const rawBody = await request.text();
        reportData = JSON.parse(rawBody);
      } catch (parseError) {
        return createErrorResponse(
          "Invalid JSON format",
          `JSON\u89E3\u6790\u5931\u8D25: ${parseError.message}`,
          400,
          corsHeaders,
          "\u8BF7\u68C0\u67E5\u4E0A\u62A5\u7684JSON\u683C\u5F0F\u662F\u5426\u6B63\u786E"
        );
      }
      const validationResult = validateAndFixVpsData(reportData);
      if (!validationResult.success) {
        return createErrorResponse(
          validationResult.error,
          validationResult.message,
          400,
          corsHeaders,
          validationResult.details
        );
      }
      reportData = validationResult.data;
      const currentInterval = await getVpsReportInterval(env);
      const shouldFlush = vpsBatchProcessor.addReport(serverId, reportData, currentInterval);
      if (shouldFlush) {
        ctx.waitUntil(flushVpsBatchData(env));
      } else {
        if (vpsBatchProcessor.shouldFlush(currentInterval)) {
          ctx.waitUntil(flushVpsBatchData(env));
        }
      }
      return createSuccessResponse({ interval: currentInterval }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u6570\u636E\u4E0A\u62A5");
    }
  }
  if (path === "/api/status/batch" && method === "GET") {
    try {
      const { results } = await env.DB.prepare(`
        SELECT s.id, s.name, s.description,
               m.timestamp, m.cpu, m.memory, m.disk, m.network, m.uptime
        FROM servers s
        LEFT JOIN metrics m ON s.id = m.server_id
        WHERE s.is_public = 1
        ORDER BY s.sort_order ASC NULLS LAST, s.name ASC
      `).all();
      const servers = (results || []).map((row) => {
        const server = { id: row.id, name: row.name, description: row.description };
        let metrics = null;
        if (row.timestamp) {
          metrics = {
            timestamp: row.timestamp,
            uptime: row.uptime
          };
          try {
            if (row.cpu) metrics.cpu = JSON.parse(row.cpu);
            if (row.memory) metrics.memory = JSON.parse(row.memory);
            if (row.disk) metrics.disk = JSON.parse(row.disk);
            if (row.network) metrics.network = JSON.parse(row.network);
          } catch (parseError) {
          }
        }
        return { server, metrics, error: false };
      });
      return createApiResponse({ servers }, 200, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u6279\u91CFVPS\u72B6\u6001\u67E5\u8BE2");
    }
  }
  if (path.startsWith("/api/status/") && method === "GET") {
    try {
      const serverId = path.split("/")[3];
      if (!serverId) {
        return createErrorResponse("Invalid server ID", "\u65E0\u6548\u7684\u670D\u52A1\u5668ID", 400, corsHeaders);
      }
      const serverData = await env.DB.prepare(
        "SELECT id, name, description FROM servers WHERE id = ? AND is_public = 1"
      ).bind(serverId).first();
      if (!serverData) {
        return createErrorResponse("Server not found", "\u670D\u52A1\u5668\u4E0D\u5B58\u5728", 404, corsHeaders);
      }
      const metricsData = await env.DB.prepare(`
        SELECT * FROM metrics
        WHERE server_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
      `).bind(serverId).first();
      if (metricsData) {
        try {
          if (metricsData.cpu) metricsData.cpu = JSON.parse(metricsData.cpu);
          if (metricsData.memory) metricsData.memory = JSON.parse(metricsData.memory);
          if (metricsData.disk) metricsData.disk = JSON.parse(metricsData.disk);
          if (metricsData.network) metricsData.network = JSON.parse(metricsData.network);
        } catch (parseError) {
        }
      }
      const publicInfo = {
        server: serverData,
        metrics: metricsData || null,
        error: false
      };
      return createApiResponse(publicInfo, 200, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "VPS\u72B6\u6001\u67E5\u8BE2");
    }
  }
  if (path === "/api/notify/offline" && method === "POST") {
    try {
      const { serverId, serverName } = await request.json();
      const server = await env.DB.prepare("SELECT last_notified_down_at FROM servers WHERE id = ?").bind(serverId).first();
      if (server?.last_notified_down_at) {
        return createApiResponse({ success: true, message: "Already notified" }, 200, corsHeaders);
      }
      const message = `\u{1F534} VPS\u6545\u969C: \u670D\u52A1\u5668 *${serverName}* \u5DF2\u79BB\u7EBF\u8D85\u8FC75\u5206\u949F`;
      await env.DB.prepare("UPDATE servers SET last_notified_down_at = ? WHERE id = ?").bind(Math.floor(Date.now() / 1e3), serverId).run();
      ctx.waitUntil(sendTelegramNotificationOptimized(env.DB, message, "high"));
      return createApiResponse({ success: true }, 200, corsHeaders);
    } catch (error) {
      return createErrorResponse("Notification failed", "\u901A\u77E5\u53D1\u9001\u5931\u8D25", 500, corsHeaders);
    }
  }
  if (path === "/api/notify/recovery" && method === "POST") {
    try {
      const { serverId, serverName } = await request.json();
      const message = `\u2705 VPS\u6062\u590D: \u670D\u52A1\u5668 *${serverName}* \u5DF2\u6062\u590D\u5728\u7EBF`;
      await env.DB.prepare("UPDATE servers SET last_notified_down_at = NULL WHERE id = ?").bind(serverId).run();
      ctx.waitUntil(sendTelegramNotificationOptimized(env.DB, message, "high"));
      return createApiResponse({ success: true }, 200, corsHeaders);
    } catch (error) {
      return createErrorResponse("Notification failed", "\u901A\u77E5\u53D1\u9001\u5931\u8D25", 500, corsHeaders);
    }
  }
  return null;
}
__name(handleVpsRoutes, "handleVpsRoutes");
async function handleApiRequest(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;
  const method = request.method;
  const clientIP = getClientIP(request);
  const origin = request.headers.get("Origin");
  const corsHeaders = getSecureCorsHeaders(origin, env);
  if (method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders });
  }
  if (path !== "/api/auth/login" && !checkRateLimit(clientIP, path, env)) {
    return createErrorResponse(
      "Rate limit exceeded",
      "\u8BF7\u6C42\u8FC7\u4E8E\u9891\u7E41\uFF0C\u8BF7\u7A0D\u540E\u518D\u8BD5",
      429,
      corsHeaders
    );
  }
  if (path.startsWith("/api/auth/")) {
    const authResult = await handleAuthRoutes(path, method, request, env, corsHeaders, clientIP);
    if (authResult) return authResult;
  }
  if (path.startsWith("/api/servers") || path.startsWith("/api/admin/servers")) {
    const serverResult = await handleServerRoutes(path, method, request, env, corsHeaders);
    if (serverResult) return serverResult;
  }
  if (path.startsWith("/api/config/") || path.startsWith("/api/report/") || path.startsWith("/api/status/") || path.startsWith("/api/notify/")) {
    const vpsResult = await handleVpsRoutes(path, method, request, env, corsHeaders, ctx);
    if (vpsResult) return vpsResult;
  }
  if (path === "/api/init-db" && ["POST", "GET"].includes(method)) {
    try {
      await ensureTablesExist(env.DB, env);
      return createSuccessResponse({
        message: "\u6570\u636E\u5E93\u521D\u59CB\u5316\u5B8C\u6210"
      }, corsHeaders);
    } catch (error) {
      return createErrorResponse(
        "Database initialization failed",
        `\u6570\u636E\u5E93\u521D\u59CB\u5316\u5931\u8D25: ${error.message}`,
        500,
        corsHeaders
      );
    }
  }
  if (path === "/api/admin/servers/batch-reorder" && method === "POST") {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse("Unauthorized", "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650", 401, corsHeaders);
    }
    try {
      const { serverIds } = await request.json();
      if (!Array.isArray(serverIds) || serverIds.length === 0) {
        return new Response(JSON.stringify({
          error: "Invalid server IDs",
          message: "\u670D\u52A1\u5668ID\u6570\u7EC4\u65E0\u6548"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const updateStmts = serverIds.map(
        (serverId, index) => env.DB.prepare("UPDATE servers SET sort_order = ? WHERE id = ?").bind(index, serverId)
      );
      await env.DB.batch(updateStmts);
      return new Response(JSON.stringify({
        success: true,
        message: "\u6279\u91CF\u6392\u5E8F\u5B8C\u6210"
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/servers/auto-sort" && method === "POST") {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse("Unauthorized", "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650", 401, corsHeaders);
    }
    try {
      const { sortBy, order } = await request.json();
      const validSortFields = ["custom", "name", "status"];
      const validOrders = ["asc", "desc"];
      if (!validSortFields.includes(sortBy) || !validOrders.includes(order)) {
        return new Response(JSON.stringify({
          error: "Invalid sort parameters",
          message: "\u65E0\u6548\u7684\u6392\u5E8F\u53C2\u6570"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      if (sortBy === "custom") {
        return new Response(JSON.stringify({
          success: true,
          message: "\u5DF2\u8BBE\u7F6E\u4E3A\u81EA\u5B9A\u4E49\u6392\u5E8F"
        }), {
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const safeOrder = validateSqlIdentifier(order.toUpperCase(), "order");
      let orderClause = "";
      if (sortBy === "name") {
        orderClause = `ORDER BY name ${safeOrder}`;
      } else if (sortBy === "status") {
        orderClause = `ORDER BY (CASE WHEN m.timestamp IS NULL OR (strftime('%s', 'now') - m.timestamp) > 300 THEN 1 ELSE 0 END) ${safeOrder}, name ASC`;
      }
      const { results: servers } = await env.DB.prepare(`
        SELECT s.id FROM servers s
        LEFT JOIN metrics m ON s.id = m.server_id
        ${orderClause}
      `).all();
      const updateStmts = servers.map(
        (server, index) => env.DB.prepare("UPDATE servers SET sort_order = ? WHERE id = ?").bind(index, server.id)
      );
      await env.DB.batch(updateStmts);
      return new Response(JSON.stringify({
        success: true,
        message: `\u5DF2\u6309${sortBy}${order === "asc" ? "\u5347\u5E8F" : "\u964D\u5E8F"}\u6392\u5E8F`
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path.match(/\/api\/admin\/servers\/[^\/]+\/reorder$/) && method === "POST") {
    try {
      const serverId = extractPathSegment(path, 4);
      if (!serverId) {
        return new Response(JSON.stringify({
          error: "Invalid server ID",
          message: "\u65E0\u6548\u7684\u670D\u52A1\u5668ID\u683C\u5F0F"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const { direction } = await request.json();
      if (!["up", "down"].includes(direction)) {
        return new Response(JSON.stringify({
          error: "Invalid direction"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const results = await env.DB.batch([
        env.DB.prepare("SELECT id, sort_order FROM servers ORDER BY sort_order ASC NULLS LAST, name ASC")
      ]);
      const allServers = results[0].results;
      const currentIndex = allServers.findIndex((s) => s.id === serverId);
      if (currentIndex === -1) {
        return new Response(JSON.stringify({
          error: "Server not found"
        }), {
          status: 404,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      let targetIndex = -1;
      if (direction === "up" && currentIndex > 0) {
        targetIndex = currentIndex - 1;
      } else if (direction === "down" && currentIndex < allServers.length - 1) {
        targetIndex = currentIndex + 1;
      }
      if (targetIndex !== -1) {
        const currentServer = allServers[currentIndex];
        const targetServer = allServers[targetIndex];
        if (currentServer.sort_order === null || targetServer.sort_order === null) {
          const updateStmts = allServers.map(
            (server, index) => env.DB.prepare("UPDATE servers SET sort_order = ? WHERE id = ?").bind(index, server.id)
          );
          await env.DB.batch(updateStmts);
          const updatedResults = await env.DB.batch([
            env.DB.prepare("SELECT id, sort_order FROM servers ORDER BY sort_order ASC")
          ]);
          const updatedServers = updatedResults[0].results;
          const newCurrentIndex = updatedServers.findIndex((s) => s.id === serverId);
          let newTargetIndex = -1;
          if (direction === "up" && newCurrentIndex > 0) {
            newTargetIndex = newCurrentIndex - 1;
          } else if (direction === "down" && newCurrentIndex < updatedServers.length - 1) {
            newTargetIndex = newCurrentIndex + 1;
          }
          if (newTargetIndex !== -1) {
            const newCurrentOrder = updatedServers[newCurrentIndex].sort_order;
            const newTargetOrder = updatedServers[newTargetIndex].sort_order;
            await env.DB.batch([
              env.DB.prepare("UPDATE servers SET sort_order = ? WHERE id = ?").bind(newTargetOrder, serverId),
              env.DB.prepare("UPDATE servers SET sort_order = ? WHERE id = ?").bind(newCurrentOrder, updatedServers[newTargetIndex].id)
            ]);
          }
        } else {
          await env.DB.batch([
            env.DB.prepare("UPDATE servers SET sort_order = ? WHERE id = ?").bind(targetServer.sort_order, serverId),
            env.DB.prepare("UPDATE servers SET sort_order = ? WHERE id = ?").bind(currentServer.sort_order, targetServer.id)
          ]);
        }
      }
      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path.match(/^\/api\/admin\/servers\/([^\/]+)\/visibility$/) && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const serverId = path.split("/")[4];
      const { is_public } = await request.json();
      if (typeof is_public !== "boolean") {
        return new Response(JSON.stringify({
          error: "Invalid input",
          message: "\u663E\u793A\u72B6\u6001\u5FC5\u987B\u4E3A\u5E03\u5C14\u503C"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      await env.DB.prepare(`
        UPDATE servers SET is_public = ? WHERE id = ?
      `).bind(is_public ? 1 : 0, serverId).run();
      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/sites" && method === "GET") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const { results } = await env.DB.prepare(`
        SELECT id, name, url, added_at, last_checked, last_status, last_status_code,
               last_response_time_ms, sort_order, last_notified_down_at, is_public
        FROM monitored_sites
        ORDER BY sort_order ASC NULLS LAST, name ASC, url ASC
      `).all();
      return new Response(JSON.stringify({ sites: results || [] }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes("no such table")) {
        try {
          await env.DB.exec(D1_SCHEMAS.monitored_sites);
          return new Response(JSON.stringify({ sites: [] }), {
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        } catch (createError) {
        }
      }
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: "\u670D\u52A1\u5668\u5185\u90E8\u9519\u8BEF"
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/sites" && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const { url: url2, name } = await parseJsonSafely(request);
      if (!url2 || !isValidHttpUrl(url2)) {
        return new Response(JSON.stringify({
          error: "Valid URL is required",
          message: "\u8BF7\u8F93\u5165\u6709\u6548\u7684URL"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const siteId = Math.random().toString(36).substring(2, 12);
      const addedAt = Math.floor(Date.now() / 1e3);
      const maxOrderResult = await env.DB.prepare(
        "SELECT MAX(sort_order) as max_order FROM monitored_sites"
      ).first();
      const nextSortOrder = maxOrderResult?.max_order && typeof maxOrderResult.max_order === "number" ? maxOrderResult.max_order + 1 : 0;
      await env.DB.prepare(`
        INSERT INTO monitored_sites (id, url, name, added_at, last_status, sort_order)
        VALUES (?, ?, ?, ?, ?, ?)
      `).bind(siteId, url2, name || "", addedAt, "PENDING", nextSortOrder).run();
      const siteData = {
        id: siteId,
        url: url2,
        name: name || "",
        added_at: addedAt,
        last_status: "PENDING",
        sort_order: nextSortOrder
      };
      const newSiteForCheck = { id: siteId, url: url2, name: name || "" };
      if (ctx?.waitUntil) {
        ctx.waitUntil(checkWebsiteStatus(newSiteForCheck, env.DB, ctx));
      } else {
        checkWebsiteStatus(newSiteForCheck, env.DB, ctx).catch((e) => {
        });
      }
      return new Response(JSON.stringify({ site: siteData }), {
        status: 201,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes("UNIQUE constraint failed")) {
        return new Response(JSON.stringify({
          error: "URL already exists or ID conflict",
          message: "\u8BE5URL\u5DF2\u88AB\u76D1\u63A7\u6216ID\u51B2\u7A81"
        }), {
          status: 409,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      if (error.message.includes("no such table")) {
        try {
          await env.DB.exec(D1_SCHEMAS.monitored_sites);
          return new Response(JSON.stringify({
            error: "Database table created, please retry",
            message: "\u6570\u636E\u5E93\u8868\u5DF2\u521B\u5EFA\uFF0C\u8BF7\u91CD\u8BD5\u6DFB\u52A0\u64CD\u4F5C"
          }), {
            status: 503,
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        } catch (createError) {
        }
      }
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path.match(/\/api\/admin\/sites\/[^\/]+$/) && method === "PUT") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return createErrorResponse("Unauthorized", "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650", 401, corsHeaders);
    }
    try {
      const siteId = path.split("/").pop();
      if (!siteId) {
        return createErrorResponse("Invalid site ID", "\u65E0\u6548\u7684\u7F51\u7AD9ID", 400, corsHeaders);
      }
      const { url: url2, name } = await request.json();
      if (!url2 || !url2.trim()) {
        return createErrorResponse("Invalid URL", "URL\u4E0D\u80FD\u4E3A\u7A7A", 400, corsHeaders);
      }
      if (!url2.startsWith("http://") && !url2.startsWith("https://")) {
        return createErrorResponse("Invalid URL format", "URL\u5FC5\u987B\u4EE5http://\u6216https://\u5F00\u5934", 400, corsHeaders);
      }
      const info = await env.DB.prepare(`
        UPDATE monitored_sites SET url = ?, name = ? WHERE id = ?
      `).bind(url2.trim(), name?.trim() || "", siteId).run();
      if (info.changes === 0) {
        return createErrorResponse("Site not found", "\u7F51\u7AD9\u4E0D\u5B58\u5728", 404, corsHeaders);
      }
      return createSuccessResponse({
        id: siteId,
        url: url2.trim(),
        name: name?.trim() || "",
        message: "\u7F51\u7AD9\u66F4\u65B0\u6210\u529F"
      }, corsHeaders);
    } catch (error) {
      return handleDbError(error, corsHeaders, "\u66F4\u65B0\u76D1\u63A7\u7AD9\u70B9");
    }
  }
  if (path.match(/\/api\/admin\/sites\/[^\/]+$/) && method === "DELETE") {
    const user = await authenticateAdmin(request, env);
    if (!user) {
      return createErrorResponse("Unauthorized", "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650", 401, corsHeaders);
    }
    try {
      const siteId = extractAndValidateServerId(path);
      if (!siteId) {
        return createErrorResponse("Invalid site ID", "\u65E0\u6548\u7684\u7AD9\u70B9ID\u683C\u5F0F", 400, corsHeaders);
      }
      const url2 = new URL(request.url);
      const confirmed = url2.searchParams.get("confirm") === "true";
      if (!confirmed) {
        return createErrorResponse(
          "Confirmation required",
          "\u5220\u9664\u64CD\u4F5C\u9700\u8981\u786E\u8BA4\uFF0C\u8BF7\u6DFB\u52A0 ?confirm=true \u53C2\u6570",
          400,
          corsHeaders
        );
      }
      const info = await env.DB.prepare("DELETE FROM monitored_sites WHERE id = ?").bind(siteId).run();
      if (info.changes === 0) {
        return new Response(JSON.stringify({
          error: "Site not found"
        }), {
          status: 404,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/sites/batch-reorder" && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const { siteIds } = await request.json();
      if (!Array.isArray(siteIds) || siteIds.length === 0) {
        return new Response(JSON.stringify({
          error: "Invalid site IDs",
          message: "\u7AD9\u70B9ID\u6570\u7EC4\u65E0\u6548"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const updateStmts = siteIds.map(
        (siteId, index) => env.DB.prepare("UPDATE monitored_sites SET sort_order = ? WHERE id = ?").bind(index, siteId)
      );
      await env.DB.batch(updateStmts);
      return new Response(JSON.stringify({
        success: true,
        message: "\u6279\u91CF\u6392\u5E8F\u5B8C\u6210"
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/sites/auto-sort" && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const { sortBy, order } = await request.json();
      const validSortFields = ["custom", "name", "url", "status"];
      const validOrders = ["asc", "desc"];
      if (!validSortFields.includes(sortBy) || !validOrders.includes(order)) {
        return new Response(JSON.stringify({
          error: "Invalid sort parameters",
          message: "\u65E0\u6548\u7684\u6392\u5E8F\u53C2\u6570"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      if (sortBy === "custom") {
        return new Response(JSON.stringify({
          success: true,
          message: "\u5DF2\u8BBE\u7F6E\u4E3A\u81EA\u5B9A\u4E49\u6392\u5E8F"
        }), {
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const safeSortBy = validateSqlIdentifier(sortBy, "column");
      const safeOrder = validateSqlIdentifier(order.toUpperCase(), "order");
      const { results: sites } = await env.DB.prepare(`
        SELECT id FROM monitored_sites
        ORDER BY ${safeSortBy} ${safeOrder}
      `).all();
      const updateStmts = sites.map(
        (site, index) => env.DB.prepare("UPDATE monitored_sites SET sort_order = ? WHERE id = ?").bind(index, site.id)
      );
      await env.DB.batch(updateStmts);
      return new Response(JSON.stringify({
        success: true,
        message: `\u5DF2\u6309${sortBy}${order === "asc" ? "\u5347\u5E8F" : "\u964D\u5E8F"}\u6392\u5E8F`
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path.match(/\/api\/admin\/sites\/[^\/]+\/reorder$/) && method === "POST") {
    try {
      const siteId = extractPathSegment(path, 4);
      if (!siteId) {
        return new Response(JSON.stringify({
          error: "Invalid site ID",
          message: "\u65E0\u6548\u7684\u7AD9\u70B9ID\u683C\u5F0F"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const { direction } = await request.json();
      if (!["up", "down"].includes(direction)) {
        return new Response(JSON.stringify({
          error: "Invalid direction"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      const results = await env.DB.batch([
        env.DB.prepare("SELECT id, sort_order FROM monitored_sites ORDER BY sort_order ASC NULLS LAST, name ASC, url ASC")
      ]);
      const allSites = results[0].results;
      const currentIndex = allSites.findIndex((s) => s.id === siteId);
      if (currentIndex === -1) {
        return new Response(JSON.stringify({
          error: "Site not found"
        }), {
          status: 404,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      let targetIndex = -1;
      if (direction === "up" && currentIndex > 0) {
        targetIndex = currentIndex - 1;
      } else if (direction === "down" && currentIndex < allSites.length - 1) {
        targetIndex = currentIndex + 1;
      }
      if (targetIndex !== -1) {
        const currentSite = allSites[currentIndex];
        const targetSite = allSites[targetIndex];
        if (currentSite.sort_order === null || targetSite.sort_order === null) {
          const updateStmts = allSites.map(
            (site, index) => env.DB.prepare("UPDATE monitored_sites SET sort_order = ? WHERE id = ?").bind(index, site.id)
          );
          await env.DB.batch(updateStmts);
          const updatedResults = await env.DB.batch([
            env.DB.prepare("SELECT id, sort_order FROM monitored_sites ORDER BY sort_order ASC")
          ]);
          const updatedSites = updatedResults[0].results;
          const newCurrentIndex = updatedSites.findIndex((s) => s.id === siteId);
          let newTargetIndex = -1;
          if (direction === "up" && newCurrentIndex > 0) {
            newTargetIndex = newCurrentIndex - 1;
          } else if (direction === "down" && newCurrentIndex < updatedSites.length - 1) {
            newTargetIndex = newCurrentIndex + 1;
          }
          if (newTargetIndex !== -1) {
            const newCurrentOrder = updatedSites[newCurrentIndex].sort_order;
            const newTargetOrder = updatedSites[newTargetIndex].sort_order;
            await env.DB.batch([
              env.DB.prepare("UPDATE monitored_sites SET sort_order = ? WHERE id = ?").bind(newTargetOrder, siteId),
              env.DB.prepare("UPDATE monitored_sites SET sort_order = ? WHERE id = ?").bind(newCurrentOrder, updatedSites[newTargetIndex].id)
            ]);
          }
        } else {
          await env.DB.batch([
            env.DB.prepare("UPDATE monitored_sites SET sort_order = ? WHERE id = ?").bind(targetSite.sort_order, siteId),
            env.DB.prepare("UPDATE monitored_sites SET sort_order = ? WHERE id = ?").bind(currentSite.sort_order, targetSite.id)
          ]);
        }
      }
      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path.match(/^\/api\/admin\/sites\/([^\/]+)\/visibility$/) && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const siteId = path.split("/")[4];
      const { is_public } = await request.json();
      if (typeof is_public !== "boolean") {
        return new Response(JSON.stringify({
          error: "Invalid input",
          message: "\u663E\u793A\u72B6\u6001\u5FC5\u987B\u4E3A\u5E03\u5C14\u503C"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      await env.DB.prepare(`
        UPDATE monitored_sites SET is_public = ? WHERE id = ?
      `).bind(is_public ? 1 : 0, siteId).run();
      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/sites/status" && method === "GET") {
    try {
      const query = `
        SELECT id, name, last_checked, last_status, last_status_code, last_response_time_ms
        FROM monitored_sites
        WHERE is_public = 1
        ORDER BY sort_order ASC NULLS LAST, name ASC, id ASC
      `;
      const { results } = await env.DB.prepare(query).all();
      const sites = results || [];
      const nowSeconds = Math.floor(Date.now() / 1e3);
      const twentyFourHoursAgoSeconds = nowSeconds - 24 * 60 * 60;
      for (const site of sites) {
        try {
          const { results: historyResults } = await env.DB.prepare(`
            SELECT timestamp, status, status_code, response_time_ms
            FROM site_status_history
            WHERE site_id = ? AND timestamp >= ?
            ORDER BY timestamp DESC
          `).bind(site.id, twentyFourHoursAgoSeconds).all();
          site.history = historyResults || [];
        } catch (historyError) {
          site.history = [];
        }
      }
      return new Response(JSON.stringify({ sites }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes("no such table")) {
        try {
          await env.DB.exec(D1_SCHEMAS.monitored_sites);
          return new Response(JSON.stringify({ sites: [] }), {
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        } catch (createError) {
        }
      }
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/settings/vps-report-interval" && method === "GET") {
    try {
      const interval = await getVpsReportInterval(env);
      return new Response(JSON.stringify({ interval }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({ interval: 60 }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/settings/vps-report-interval" && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const { interval } = await request.json();
      if (typeof interval !== "number" || interval <= 0 || !Number.isInteger(interval)) {
        return new Response(JSON.stringify({
          error: "Invalid interval value. Must be a positive integer (seconds)."
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      await env.DB.prepare("REPLACE INTO app_config (key, value) VALUES (?, ?)").bind(
        "vps_report_interval_seconds",
        interval.toString()
      ).run();
      configCache.clearKey("monitoring_settings");
      vpsIntervalCache.value = null;
      return new Response(JSON.stringify({ success: true, interval }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/telegram-settings" && method === "GET") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const settings = await configCache.getTelegramConfig(env.DB);
      return new Response(JSON.stringify(
        settings || { bot_token: null, chat_id: null, enable_notifications: 0 }
      ), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes("no such table")) {
        try {
          await env.DB.exec(D1_SCHEMAS.telegram_config);
          return new Response(JSON.stringify({
            bot_token: null,
            chat_id: null,
            enable_notifications: 0
          }), {
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        } catch (createError) {
        }
      }
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/telegram-settings" && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const { bot_token, chat_id, enable_notifications } = await request.json();
      const updatedAt = Math.floor(Date.now() / 1e3);
      const enableNotifValue = enable_notifications === true || enable_notifications === 1 ? 1 : 0;
      await env.DB.prepare(`
        UPDATE telegram_config SET bot_token = ?, chat_id = ?, enable_notifications = ?, updated_at = ? WHERE id = 1
      `).bind(bot_token || null, chat_id || null, enableNotifValue, updatedAt).run();
      configCache.clearKey("telegram_config");
      if (enableNotifValue === 1 && bot_token && chat_id) {
        const testMessage = "\u2705 Telegram\u901A\u77E5\u5DF2\u5728\u6B64\u76D1\u63A7\u9762\u677F\u6FC0\u6D3B\u3002\u8FD9\u662F\u4E00\u6761\u6D4B\u8BD5\u6D88\u606F\u3002";
        if (ctx?.waitUntil) {
          ctx.waitUntil(sendTelegramNotificationOptimized(env.DB, testMessage, "high"));
        } else {
          sendTelegramNotificationOptimized(env.DB, testMessage, "high").catch((e) => {
          });
        }
      }
      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/display-settings" && method === "GET") {
    try {
      const { results } = await env.DB.prepare(`
        SELECT key, value FROM app_config
        WHERE key IN ('show_server_section', 'show_site_section')
      `).all();
      const settings = {
        showServerSection: true,
        showSiteSection: true
      };
      (results || []).forEach((row) => {
        if (row.key === "show_server_section") {
          settings.showServerSection = row.value !== "false";
        } else if (row.key === "show_site_section") {
          settings.showSiteSection = row.value !== "false";
        }
      });
      return new Response(JSON.stringify(settings), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        showServerSection: true,
        showSiteSection: true
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/display-settings" && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const { showServerSection, showSiteSection } = await request.json();
      if (typeof showServerSection !== "boolean" || typeof showSiteSection !== "boolean") {
        return new Response(JSON.stringify({
          error: "Invalid display settings",
          message: "\u663E\u793A\u8BBE\u7F6E\u5FC5\u987B\u662F\u5E03\u5C14\u503C"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      await env.DB.batch([
        env.DB.prepare("REPLACE INTO app_config (key, value) VALUES (?, ?)").bind(
          "show_server_section",
          showServerSection.toString()
        ),
        env.DB.prepare("REPLACE INTO app_config (key, value) VALUES (?, ?)").bind(
          "show_site_section",
          showSiteSection.toString()
        )
      ]);
      configCache.clearKey("monitoring_settings");
      return new Response(JSON.stringify({
        success: true,
        settings: { showServerSection, showSiteSection }
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/background-settings" && method === "GET") {
    try {
      const { results } = await env.DB.prepare(`
        SELECT key, value FROM app_config
        WHERE key IN ('custom_background_enabled', 'custom_background_url', 'page_opacity')
      `).all();
      const settings = {
        enabled: false,
        url: "",
        opacity: 80
      };
      results.forEach((row) => {
        switch (row.key) {
          case "custom_background_enabled":
            settings.enabled = row.value === "true";
            break;
          case "custom_background_url":
            settings.url = row.value || "";
            break;
          case "page_opacity":
            settings.opacity = parseInt(row.value, 10) || 80;
            break;
        }
      });
      return new Response(JSON.stringify(settings), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        enabled: false,
        url: "",
        opacity: 80
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path === "/api/admin/background-settings" && method === "POST") {
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(JSON.stringify({
        error: "Unauthorized",
        message: "\u9700\u8981\u7BA1\u7406\u5458\u6743\u9650"
      }), {
        status: 401,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
    try {
      const { enabled, url: url2, opacity } = await request.json();
      if (typeof enabled !== "boolean") {
        return new Response(JSON.stringify({
          error: "Invalid enabled value",
          message: "enabled\u5FC5\u987B\u662F\u5E03\u5C14\u503C"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      if (enabled && url2) {
        if (typeof url2 !== "string" || !url2.startsWith("https://")) {
          return new Response(JSON.stringify({
            error: "Invalid URL format",
            message: "\u80CC\u666F\u56FE\u7247URL\u5FC5\u987B\u4EE5https://\u5F00\u5934"
          }), {
            status: 400,
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        }
      }
      if (typeof opacity !== "number" || opacity < 0 || opacity > 100) {
        return new Response(JSON.stringify({
          error: "Invalid opacity value",
          message: "\u900F\u660E\u5EA6\u5FC5\u987B\u662F0-100\u4E4B\u95F4\u7684\u6570\u5B57"
        }), {
          status: 400,
          headers: { "Content-Type": "application/json", ...corsHeaders }
        });
      }
      await env.DB.batch([
        env.DB.prepare("REPLACE INTO app_config (key, value) VALUES (?, ?)").bind(
          "custom_background_enabled",
          enabled.toString()
        ),
        env.DB.prepare("REPLACE INTO app_config (key, value) VALUES (?, ?)").bind(
          "custom_background_url",
          url2 || ""
        ),
        env.DB.prepare("REPLACE INTO app_config (key, value) VALUES (?, ?)").bind(
          "page_opacity",
          opacity.toString()
        )
      ]);
      configCache.clearKey("monitoring_settings");
      return new Response(JSON.stringify({
        success: true,
        settings: { enabled, url: url2 || "", opacity }
      }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  if (path.match(/\/api\/sites\/[^\/]+\/history$/) && method === "GET") {
    try {
      const siteId = path.split("/")[3];
      const nowSeconds = Math.floor(Date.now() / 1e3);
      const twentyFourHoursAgoSeconds = nowSeconds - 24 * 60 * 60;
      const { results } = await env.DB.prepare(`
        SELECT timestamp, status, status_code, response_time_ms
        FROM site_status_history
        WHERE site_id = ? AND timestamp >= ?
        ORDER BY timestamp DESC
      `).bind(siteId, twentyFourHoursAgoSeconds).all();
      return new Response(JSON.stringify({ history: results || [] }), {
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    } catch (error) {
      if (error.message.includes("no such table")) {
        try {
          await env.DB.exec(D1_SCHEMAS.site_status_history);
          return new Response(JSON.stringify({ history: [] }), {
            headers: { "Content-Type": "application/json", ...corsHeaders }
          });
        } catch (createError) {
        }
      }
      return new Response(JSON.stringify({
        error: "Internal server error",
        message: error.message
      }), {
        status: 500,
        headers: { "Content-Type": "application/json", ...corsHeaders }
      });
    }
  }
  return new Response(JSON.stringify({ error: "API endpoint not found" }), {
    status: 404,
    headers: { "Content-Type": "application/json", ...corsHeaders }
  });
}
__name(handleApiRequest, "handleApiRequest");
async function checkWebsiteStatus(site, db, ctx) {
  const { id, url, name } = site;
  const startTime = Date.now();
  let newStatus = "PENDING";
  let newStatusCode = null;
  let newResponseTime = null;
  let previousStatus = "PENDING";
  let siteLastNotifiedDownAt = null;
  try {
    const siteDetailsStmt = db.prepare("SELECT last_status, last_notified_down_at FROM monitored_sites WHERE id = ?");
    const siteDetailsResult = await siteDetailsStmt.bind(id).first();
    if (siteDetailsResult) {
      previousStatus = siteDetailsResult.last_status || "PENDING";
      siteLastNotifiedDownAt = siteDetailsResult.last_notified_down_at;
    }
  } catch (error) {
  }
  const NOTIFICATION_INTERVAL_SECONDS = 1 * 60 * 60;
  try {
    const response = await fetch(url, { method: "HEAD", redirect: "follow", signal: AbortSignal.timeout(15e3) });
    newResponseTime = Date.now() - startTime;
    newStatusCode = response.status;
    if (response.ok || response.status >= 300 && response.status < 500) {
      newStatus = "UP";
    } else {
      newStatus = "DOWN";
    }
  } catch (error) {
    newResponseTime = Date.now() - startTime;
    if (error.name === "TimeoutError") {
      newStatus = "TIMEOUT";
    } else {
      newStatus = "ERROR";
    }
  }
  const checkTime = Math.floor(Date.now() / 1e3);
  const siteDisplayName = name || url;
  let newSiteLastNotifiedDownAt = siteLastNotifiedDownAt;
  if (["DOWN", "TIMEOUT", "ERROR"].includes(newStatus)) {
    const isFirstTimeDown = !["DOWN", "TIMEOUT", "ERROR"].includes(previousStatus);
    if (isFirstTimeDown) {
      const message = `\u{1F534} \u7F51\u7AD9\u6545\u969C: *${siteDisplayName}* \u5F53\u524D\u72B6\u6001 ${newStatus.toLowerCase()} (\u72B6\u6001\u7801: ${newStatusCode || "\u65E0"}).
\u7F51\u5740: ${url}`;
      ctx.waitUntil(sendTelegramNotificationOptimized(db, message));
      newSiteLastNotifiedDownAt = checkTime;
    } else {
      const shouldResend = siteLastNotifiedDownAt === null || checkTime - siteLastNotifiedDownAt > NOTIFICATION_INTERVAL_SECONDS;
      if (shouldResend) {
        const message = `\u{1F534} \u7F51\u7AD9\u6301\u7EED\u6545\u969C: *${siteDisplayName}* \u72B6\u6001 ${newStatus.toLowerCase()} (\u72B6\u6001\u7801: ${newStatusCode || "\u65E0"}).
\u7F51\u5740: ${url}`;
        ctx.waitUntil(sendTelegramNotificationOptimized(db, message));
        newSiteLastNotifiedDownAt = checkTime;
      }
    }
  } else if (newStatus === "UP" && ["DOWN", "TIMEOUT", "ERROR"].includes(previousStatus)) {
    const message = `\u2705 \u7F51\u7AD9\u6062\u590D: *${siteDisplayName}* \u5DF2\u6062\u590D\u5728\u7EBF!
\u7F51\u5740: ${url}`;
    ctx.waitUntil(sendTelegramNotificationOptimized(db, message));
    newSiteLastNotifiedDownAt = null;
  }
  try {
    const updateSiteStmt = db.prepare(
      "UPDATE monitored_sites SET last_checked = ?, last_status = ?, last_status_code = ?, last_response_time_ms = ?, last_notified_down_at = ? WHERE id = ?"
    );
    const recordHistoryStmt = db.prepare(
      "INSERT INTO site_status_history (site_id, timestamp, status, status_code, response_time_ms) VALUES (?, ?, ?, ?, ?)"
    );
    await db.batch([
      updateSiteStmt.bind(checkTime, newStatus, newStatusCode, newResponseTime, newSiteLastNotifiedDownAt, id),
      recordHistoryStmt.bind(id, checkTime, newStatus, newStatusCode, newResponseTime)
    ]);
  } catch (dbError) {
    if (String(dbError?.message || dbError).includes("site_status_history")) {
      try {
        await db.exec(D1_SCHEMAS.site_status_history);
        await db.prepare("INSERT INTO site_status_history (site_id, timestamp, status, status_code, response_time_ms) VALUES (?, ?, ?, ?, ?)").bind(id, checkTime, newStatus, newStatusCode, newResponseTime).run();
      } catch (historyRetryError) {
      }
    }
  }
}
__name(checkWebsiteStatus, "checkWebsiteStatus");
async function checkWebsiteStatusOptimized(site, db, ctx) {
  const { id, url, name } = site;
  const startTime = Date.now();
  let newStatus = "PENDING";
  let newStatusCode = null;
  let newResponseTime = null;
  let previousStatus = "PENDING";
  let siteLastNotifiedDownAt = null;
  try {
    const siteDetailsResult = await db.prepare(
      "SELECT last_status, last_notified_down_at FROM monitored_sites WHERE id = ?"
    ).bind(id).first();
    if (siteDetailsResult) {
      previousStatus = siteDetailsResult.last_status || "PENDING";
      siteLastNotifiedDownAt = siteDetailsResult.last_notified_down_at;
    }
  } catch (error) {
  }
  const NOTIFICATION_INTERVAL_SECONDS = 1 * 60 * 60;
  try {
    const response = await fetch(url, {
      method: "HEAD",
      redirect: "follow",
      signal: AbortSignal.timeout(1e4)
      // 10秒超时
    });
    newResponseTime = Date.now() - startTime;
    newStatusCode = response.status;
    if (response.ok || response.status >= 300 && response.status < 500) {
      newStatus = "UP";
    } else {
      newStatus = "DOWN";
    }
  } catch (error) {
    newResponseTime = Date.now() - startTime;
    if (error.name === "TimeoutError") {
      newStatus = "TIMEOUT";
    } else {
      newStatus = "ERROR";
    }
  }
  const checkTime = Math.floor(Date.now() / 1e3);
  const siteDisplayName = name || url;
  let newSiteLastNotifiedDownAt = siteLastNotifiedDownAt;
  if (["DOWN", "TIMEOUT", "ERROR"].includes(newStatus)) {
    const isFirstTimeDown = !["DOWN", "TIMEOUT", "ERROR"].includes(previousStatus);
    if (isFirstTimeDown) {
      const message = `\u{1F534} \u7F51\u7AD9\u6545\u969C: *${siteDisplayName}* \u5F53\u524D\u72B6\u6001 ${newStatus.toLowerCase()} (\u72B6\u6001\u7801: ${newStatusCode || "\u65E0"}).
\u7F51\u5740: ${url}`;
      ctx.waitUntil(sendTelegramNotificationOptimized(db, message));
      newSiteLastNotifiedDownAt = checkTime;
    } else {
      const shouldResend = siteLastNotifiedDownAt === null || checkTime - siteLastNotifiedDownAt > NOTIFICATION_INTERVAL_SECONDS;
      if (shouldResend) {
        const message = `\u{1F534} \u7F51\u7AD9\u6301\u7EED\u6545\u969C: *${siteDisplayName}* \u72B6\u6001 ${newStatus.toLowerCase()} (\u72B6\u6001\u7801: ${newStatusCode || "\u65E0"}).
\u7F51\u5740: ${url}`;
        ctx.waitUntil(sendTelegramNotificationOptimized(db, message));
        newSiteLastNotifiedDownAt = checkTime;
      }
    }
  } else if (newStatus === "UP" && ["DOWN", "TIMEOUT", "ERROR"].includes(previousStatus)) {
    const message = `\u2705 \u7F51\u7AD9\u6062\u590D: *${siteDisplayName}* \u5DF2\u6062\u590D\u5728\u7EBF!
\u7F51\u5740: ${url}`;
    ctx.waitUntil(sendTelegramNotificationOptimized(db, message));
    newSiteLastNotifiedDownAt = null;
  }
  try {
    await db.batch([
      db.prepare("UPDATE monitored_sites SET last_checked = ?, last_status = ?, last_status_code = ?, last_response_time_ms = ?, last_notified_down_at = ? WHERE id = ?").bind(checkTime, newStatus, newStatusCode, newResponseTime, newSiteLastNotifiedDownAt, id),
      db.prepare("INSERT INTO site_status_history (site_id, timestamp, status, status_code, response_time_ms) VALUES (?, ?, ?, ?, ?)").bind(id, checkTime, newStatus, newStatusCode, newResponseTime)
    ]);
  } catch (dbError) {
    if (String(dbError?.message || dbError).includes("site_status_history")) {
      try {
        await db.exec(D1_SCHEMAS.site_status_history);
        await db.prepare("INSERT INTO site_status_history (site_id, timestamp, status, status_code, response_time_ms) VALUES (?, ?, ?, ?, ?)").bind(id, checkTime, newStatus, newStatusCode, newResponseTime).run();
      } catch (historyRetryError) {
      }
    }
  }
}
__name(checkWebsiteStatusOptimized, "checkWebsiteStatusOptimized");
async function checkVpsOfflineReminder(env, ctx) {
  try {
    const telegramConfig = await configCache.getTelegramConfig(env.DB);
    if (!telegramConfig?.enable_notifications || !telegramConfig.bot_token || !telegramConfig.chat_id) {
      return;
    }
    const currentTime = Math.floor(Date.now() / 1e3);
    const offlineThreshold = 5 * 60;
    const reminderInterval = 60 * 60;
    const { results: offlineServers } = await env.DB.prepare(`
      SELECT s.id, s.name, s.last_notified_down_at, m.timestamp as last_report
      FROM servers s
      LEFT JOIN metrics m ON s.id = m.server_id
      WHERE s.last_notified_down_at IS NOT NULL
        AND (m.timestamp IS NULL OR m.timestamp < ?)
        AND s.last_notified_down_at < ?
    `).bind(currentTime - offlineThreshold, currentTime - reminderInterval).all();
    for (const server of offlineServers) {
      const serverDisplayName = server.name || server.id;
      const offlineHours = Math.floor((currentTime - server.last_notified_down_at) / 3600);
      const message = `\u{1F534} VPS\u6301\u7EED\u79BB\u7EBF: \u670D\u52A1\u5668 *${serverDisplayName}* \u5DF2\u79BB\u7EBF${offlineHours}\u5C0F\u65F6\uFF08\u6BCF\u5C0F\u65F6\u63D0\u9192\uFF09`;
      ctx.waitUntil(sendTelegramNotificationOptimized(env.DB, message));
      ctx.waitUntil(env.DB.prepare("UPDATE servers SET last_notified_down_at = ? WHERE id = ?").bind(currentTime, server.id).run());
    }
  } catch (error) {
  }
}
__name(checkVpsOfflineReminder, "checkVpsOfflineReminder");
async function sendTelegramNotificationOptimized(db, message, priority = "normal") {
  try {
    const telegramConfig = await configCache.getTelegramConfig(db);
    if (!telegramConfig?.enable_notifications || !telegramConfig.bot_token || !telegramConfig.chat_id) {
      return;
    }
    const telegramUrl = `https://api.telegram.org/bot${telegramConfig.bot_token}/sendMessage`;
    const payload = {
      chat_id: telegramConfig.chat_id,
      text: message,
      parse_mode: "Markdown"
    };
    const response = await fetch(telegramUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
  } catch (error) {
  }
}
__name(sendTelegramNotificationOptimized, "sendTelegramNotificationOptimized");
async function performDatabaseMaintenance(db) {
  const thirtyDaysAgo = Math.floor(Date.now() / 1e3) - 30 * 24 * 60 * 60;
  try {
    const result = await db.prepare(
      "DELETE FROM site_status_history WHERE timestamp < ?"
    ).bind(thirtyDaysAgo).run();
    cleanupJWTCache();
  } catch (error) {
  }
}
__name(performDatabaseMaintenance, "performDatabaseMaintenance");
var worker_default = {
  async fetch(request, env, ctx) {
    if (!dbInitialized) {
      try {
        await ensureTablesExist(env.DB, env);
        dbInitialized = true;
      } catch (error) {
      }
    }
    scheduleVpsBatchFlush(env, ctx);
    const url = new URL(request.url);
    const path = url.pathname;
    if (path.startsWith("/api/")) {
      return handleApiRequest(request, env, ctx);
    }
    if (path === "/install.sh") {
      return handleInstallScript(request, url, env);
    }
    return handleFrontendRequest(request, path);
  },
  async scheduled(event, env, ctx) {
    taskCounter++;
    ctx.waitUntil(
      (async () => {
        try {
          if (!dbInitialized || taskCounter % 10 === 1) {
            await ensureTablesExist(env.DB, env);
            dbInitialized = true;
          }
          const { results: sitesToCheck } = await env.DB.prepare(
            "SELECT id, url, name FROM monitored_sites"
          ).all();
          if (sitesToCheck?.length > 0) {
            const siteConcurrencyLimit = 5;
            const sitePromises = [];
            for (const site of sitesToCheck) {
              sitePromises.push(checkWebsiteStatusOptimized(site, env.DB, ctx));
              if (sitePromises.length >= siteConcurrencyLimit) {
                await Promise.all(sitePromises);
                sitePromises.length = 0;
              }
            }
            if (sitePromises.length > 0) {
              await Promise.all(sitePromises);
            }
          }
          await checkVpsOfflineReminder(env, ctx);
          if (taskCounter % 1440 === 0) {
            await performDatabaseMaintenance(env.DB);
          }
        } catch (error) {
        }
      })()
    );
  }
};
function isValidHttpUrl(string) {
  try {
    const url = new URL(string);
    return ["http:", "https:"].includes(url.protocol);
  } catch {
    return false;
  }
}
__name(isValidHttpUrl, "isValidHttpUrl");
async function handleInstallScript(request, url, env) {
  const baseUrl = url.origin;
  let vpsReportInterval = "60";
  try {
    if (D1_SCHEMAS?.app_config) {
      await env.DB.exec(D1_SCHEMAS.app_config);
    } else {
    }
    const interval = await getVpsReportInterval(env);
    vpsReportInterval = interval.toString();
  } catch (e) {
  }
  const script = `#!/bin/bash
# VPS\u76D1\u63A7\u811A\u672C - \u5B89\u88C5\u7A0B\u5E8F

# \u9ED8\u8BA4\u503C
API_KEY=""
SERVER_ID=""
WORKER_URL="${baseUrl}"
INSTALL_DIR="/opt/vps-monitor"
SERVICE_NAME="vps-monitor"

# \u89E3\u6790\u53C2\u6570
while [[ $# -gt 0 ]]; do
  case $1 in
    -k|--key)
      API_KEY="$2"
      shift 2
      ;;
    -s|--server)
      SERVER_ID="$2"
      shift 2
      ;;
    -u|--url)
      WORKER_URL="$2"
      shift 2
      ;;
    -d|--dir)
      INSTALL_DIR="$2"
      shift 2
      ;;
    *)
      echo "\u672A\u77E5\u53C2\u6570: $1"
      exit 1
      ;;
  esac
done

# \u68C0\u67E5\u5FC5\u8981\u53C2\u6570
if [ -z "$API_KEY" ] || [ -z "$SERVER_ID" ]; then
  echo "\u9519\u8BEF: API\u5BC6\u94A5\u548C\u670D\u52A1\u5668ID\u662F\u5FC5\u9700\u7684"
  echo "\u7528\u6CD5: $0 -k API_KEY -s SERVER_ID [-u WORKER_URL] [-d INSTALL_DIR]"
  exit 1
fi

# \u68C0\u67E5\u6743\u9650
if [ "$(id -u)" -ne 0 ]; then
  echo "\u9519\u8BEF: \u6B64\u811A\u672C\u9700\u8981root\u6743\u9650"
  exit 1
fi

echo "=== VPS\u76D1\u63A7\u811A\u672C\u5B89\u88C5\u7A0B\u5E8F ==="
echo "\u5B89\u88C5\u76EE\u5F55: $INSTALL_DIR"
echo "Worker URL: $WORKER_URL"

# \u521B\u5EFA\u5B89\u88C5\u76EE\u5F55
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR" || exit 1

# \u521B\u5EFA\u76D1\u63A7\u811A\u672C
cat > "$INSTALL_DIR/monitor.sh" << 'EOF'
#!/bin/bash

# \u914D\u7F6E
API_KEY="__API_KEY__"
SERVER_ID="__SERVER_ID__"
WORKER_URL="__WORKER_URL__"
INTERVAL=${vpsReportInterval}  # \u4E0A\u62A5\u95F4\u9694\uFF08\u79D2\uFF09

# \u65E5\u5FD7\u51FD\u6570
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# \u83B7\u53D6CPU\u4F7F\u7528\u7387
get_cpu_usage() {
  cpu_usage=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\\([0-9.]*\\)%* id.*/\\1/" | awk '{print 100 - $1}')
  cpu_load=$(cat /proc/loadavg | awk '{print $1","$2","$3}')
  echo "{"usage_percent":$cpu_usage,"load_avg":[$cpu_load]}"
}

# \u83B7\u53D6\u5185\u5B58\u4F7F\u7528\u60C5\u51B5
get_memory_usage() {
  total=$(free -k | grep Mem | awk '{print $2}')
  used=$(free -k | grep Mem | awk '{print $3}')
  free=$(free -k | grep Mem | awk '{print $4}')
  usage_percent=$(echo "scale=1; $used * 100 / $total" | bc)
  echo "{"total":$total,"used":$used,"free":$free,"usage_percent":$usage_percent}"
}

# \u83B7\u53D6\u786C\u76D8\u4F7F\u7528\u60C5\u51B5
get_disk_usage() {
  disk_info=$(df -k / | tail -1)
  total=$(echo "$disk_info" | awk '{print $2 / 1024 / 1024}')
  used=$(echo "$disk_info" | awk '{print $3 / 1024 / 1024}')
  free=$(echo "$disk_info" | awk '{print $4 / 1024 / 1024}')
  usage_percent=$(echo "$disk_info" | awk '{print $5}' | tr -d '%')
  echo "{"total":$total,"used":$used,"free":$free,"usage_percent":$usage_percent}"
}

# \u83B7\u53D6\u7F51\u7EDC\u4F7F\u7528\u60C5\u51B5
get_network_usage() {
  # \u68C0\u67E5\u662F\u5426\u5B89\u88C5\u4E86ifstat
  if ! command -v ifstat &> /dev/null; then
    log "ifstat\u672A\u5B89\u88C5\uFF0C\u65E0\u6CD5\u83B7\u53D6\u7F51\u7EDC\u901F\u5EA6"
    echo "{"upload_speed":0,"download_speed":0,"total_upload":0,"total_download":0}"
    return
  fi

  # \u83B7\u53D6\u7F51\u7EDC\u63A5\u53E3
  interface=$(ip route | grep default | awk '{print $5}')

  # \u83B7\u53D6\u7F51\u7EDC\u901F\u5EA6\uFF08KB/s\uFF09
  network_speed=$(ifstat -i "$interface" 1 1 | tail -1)
  download_speed=$(echo "$network_speed" | awk '{print $1 * 1024}')
  upload_speed=$(echo "$network_speed" | awk '{print $2 * 1024}')

  # \u83B7\u53D6\u603B\u6D41\u91CF
  rx_bytes=$(cat /proc/net/dev | grep "$interface" | awk '{print $2}')
  tx_bytes=$(cat /proc/net/dev | grep "$interface" | awk '{print $10}')

  echo "{"upload_speed":$upload_speed,"download_speed":$download_speed,"total_upload":$tx_bytes,"total_download":$rx_bytes}"
}

# \u4E0A\u62A5\u6570\u636E
report_metrics() {
  timestamp=$(date +%s)
  cpu=$(get_cpu_usage)
  memory=$(get_memory_usage)
  disk=$(get_disk_usage)
  network=$(get_network_usage)

  data="{"timestamp":$timestamp,"cpu":$cpu,"memory":$memory,"disk":$disk,"network":$network}"

  log "\u6B63\u5728\u4E0A\u62A5\u6570\u636E..."
  log "API\u5BC6\u94A5: $API_KEY"
  log "\u670D\u52A1\u5668ID: $SERVER_ID"
  log "Worker URL: $WORKER_URL"

  response=$(curl -s -X POST "$WORKER_URL/api/report/$SERVER_ID"     -H "Content-Type: application/json"     -H "X-API-Key: $API_KEY"     -d "$data")

  if [[ "$response" == *"success"* ]]; then
    log "\u6570\u636E\u4E0A\u62A5\u6210\u529F"
  else
    log "\u6570\u636E\u4E0A\u62A5\u5931\u8D25: $response"
  fi
}

# \u5B89\u88C5\u4F9D\u8D56
install_dependencies() {
  log "\u68C0\u67E5\u5E76\u5B89\u88C5\u4F9D\u8D56..."

  # \u68C0\u6D4B\u5305\u7BA1\u7406\u5668
  if command -v apt-get &> /dev/null; then
    PKG_MANAGER="apt-get"
  elif command -v yum &> /dev/null; then
    PKG_MANAGER="yum"
  else
    log "\u4E0D\u652F\u6301\u7684\u7CFB\u7EDF\uFF0C\u65E0\u6CD5\u81EA\u52A8\u5B89\u88C5\u4F9D\u8D56"
    return 1
  fi

  # \u5B89\u88C5\u4F9D\u8D56
  $PKG_MANAGER update -y
  $PKG_MANAGER install -y bc curl ifstat

  log "\u4F9D\u8D56\u5B89\u88C5\u5B8C\u6210"
  return 0
}

# \u4E3B\u51FD\u6570
main() {
  log "VPS\u76D1\u63A7\u811A\u672C\u542F\u52A8"

  # \u5B89\u88C5\u4F9D\u8D56
  install_dependencies

  # \u4E3B\u5FAA\u73AF
  while true; do
    report_metrics
    sleep $INTERVAL
  done
}

# \u542F\u52A8\u4E3B\u51FD\u6570
main
EOF

# \u66FF\u6362\u914D\u7F6E
sed -i "s|__API_KEY__|$API_KEY|g" "$INSTALL_DIR/monitor.sh"
sed -i "s|__SERVER_ID__|$SERVER_ID|g" "$INSTALL_DIR/monitor.sh"
sed -i "s|__WORKER_URL__|$WORKER_URL|g" "$INSTALL_DIR/monitor.sh"
# This line ensures the INTERVAL placeholder is replaced with the fetched value.
sed -i "s|^INTERVAL=.*|INTERVAL=${vpsReportInterval}|g" "$INSTALL_DIR/monitor.sh"

# \u8BBE\u7F6E\u6267\u884C\u6743\u9650
chmod +x "$INSTALL_DIR/monitor.sh"

# \u521B\u5EFAsystemd\u670D\u52A1
cat > "/etc/systemd/system/$SERVICE_NAME.service" << EOF
[Unit]
Description=VPS Monitor Service
After=network.target

[Service]
ExecStart=$INSTALL_DIR/monitor.sh
Restart=always
User=root
Group=root
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

[Install]
WantedBy=multi-user.target
EOF

# \u542F\u52A8\u670D\u52A1
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl start "$SERVICE_NAME"

echo "=== \u5B89\u88C5\u5B8C\u6210 ==="
echo "\u670D\u52A1\u5DF2\u542F\u52A8\u5E76\u8BBE\u7F6E\u4E3A\u5F00\u673A\u81EA\u542F"
echo "\u67E5\u770B\u670D\u52A1\u72B6\u6001: systemctl status $SERVICE_NAME"
echo "\u67E5\u770B\u670D\u52A1\u65E5\u5FD7: journalctl -u $SERVICE_NAME -f"
`;
  return new Response(script, {
    headers: {
      "Content-Type": "text/plain",
      "Content-Disposition": 'attachment; filename="install.sh"'
    }
  });
}
__name(handleInstallScript, "handleInstallScript");
function handleFrontendRequest(request, path) {
  const routes = {
    "/": /* @__PURE__ */ __name(() => new Response(getIndexHtml(), { headers: { "Content-Type": "text/html" } }), "/"),
    "": () => new Response(getIndexHtml(), { headers: { "Content-Type": "text/html" } }),
    "/login": /* @__PURE__ */ __name(() => new Response(getLoginHtml(), { headers: { "Content-Type": "text/html" } }), "/login"),
    "/login.html": /* @__PURE__ */ __name(() => new Response(getLoginHtml(), { headers: { "Content-Type": "text/html" } }), "/login.html"),
    "/admin": /* @__PURE__ */ __name(() => new Response(getAdminHtml(), { headers: { "Content-Type": "text/html" } }), "/admin"),
    "/admin.html": /* @__PURE__ */ __name(() => new Response(getAdminHtml(), { headers: { "Content-Type": "text/html" } }), "/admin.html"),
    "/css/style.css": /* @__PURE__ */ __name(() => new Response(getStyleCss(), { headers: { "Content-Type": "text/css" } }), "/css/style.css"),
    "/js/main.js": /* @__PURE__ */ __name(() => new Response(getMainJs(), { headers: { "Content-Type": "application/javascript" } }), "/js/main.js"),
    "/js/login.js": /* @__PURE__ */ __name(() => new Response(getLoginJs(), { headers: { "Content-Type": "application/javascript" } }), "/js/login.js"),
    "/js/admin.js": /* @__PURE__ */ __name(() => new Response(getAdminJs(), { headers: { "Content-Type": "application/javascript" } }), "/js/admin.js"),
    "/favicon.svg": /* @__PURE__ */ __name(() => new Response(getFaviconSvg(), { headers: { "Content-Type": "image/svg+xml" } }), "/favicon.svg")
  };
  const handler = routes[path];
  if (handler) {
    return handler();
  }
  return new Response("Not Found", {
    status: 404,
    headers: { "Content-Type": "text/plain" }
  });
}
__name(handleFrontendRequest, "handleFrontendRequest");
function getIndexHtml() {
  return `<!DOCTYPE html>
<html lang="zh-CN" data-bs-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VPS\u76D1\u63A7\u9762\u677F</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg">
    <script>
        // \u7ACB\u5373\u8BBE\u7F6E\u4E3B\u9898\uFF0C\u907F\u514D\u95EA\u70C1
        (function() {
            const theme = localStorage.getItem('vps-monitor-theme') || 'light';
            document.documentElement.setAttribute('data-bs-theme', theme);
        })();
    <\/script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN" crossorigin="anonymous">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css" rel="stylesheet" integrity="sha384-4LISF5TTJX/fLmGSxO53rV4miRxdg84mZsxmO8Rx5jGtp/LbrixFETvWa5a6sESd" crossorigin="anonymous">
    <link href="/css/style.css" rel="stylesheet">
    <style>
        .server-row {
            cursor: pointer; /* Indicate clickable rows */
        }
        .server-details-row {
            /* display: none; /* Initially hidden - controlled by JS */ */
        }
        .server-details-row td {
            padding: 1rem;
            background-color: rgba(248, 249, 250, var(--page-opacity, 0.8)); /* Light background for details with transparency */
        }
        .server-details-content {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }
        .detail-item {
            background-color: rgba(233, 236, 239, var(--page-opacity, 0.8));
            padding: 0.75rem;
            border-radius: 0.25rem;
            border: 1px solid rgba(0, 0, 0, 0.1);
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u8BE6\u7EC6\u4FE1\u606F\u9879 */
        [data-bs-theme="dark"] .detail-item {
            background-color: rgba(52, 58, 64, var(--page-opacity, 0.8));
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #e0e0e0;
        }
        .detail-item strong {
            display: block;
            margin-bottom: 0.25rem;
        }
        .history-bar-container {
            display: inline-flex; /* Changed to inline-flex for centering within td */
            flex-direction: row-reverse; /* Newest on the right */
            align-items: center;
            justify-content: center; /* Center the bars within this container */
            height: 25px; /* Increased height */
            gap: 2px; /* Space between bars */
        }
        .history-bar {
            width: 8px; /* Increased width of each bar */
            height: 100%;
            /* margin-left: 1px; /* Replaced by gap */
            border-radius: 1px;
        }
        .history-bar-up { background-color: #28a745; } /* Green */
        .history-bar-down { background-color: #dc3545; } /* Red */
        .history-bar-pending { background-color: #6c757d; } /* Gray */

        /* Default styling for progress bar text (light mode) */
        .progress span {
            color: #000000; /* Black text for progress bars by default */
            /* font-weight: bold; is handled by inline style in JS */
        }

        /* Center alignment for front-end monitoring tables */
        /* Front-end server monitoring table headers and data */
        .table > thead > tr > th:nth-child(1), /* \u540D\u79F0 */
        .table > thead > tr > th:nth-child(2), /* \u72B6\u6001 */
        .table > thead > tr > th:nth-child(3), /* CPU */
        .table > thead > tr > th:nth-child(4), /* \u5185\u5B58 */
        .table > thead > tr > th:nth-child(5), /* \u786C\u76D8 */
        .table > thead > tr > th:nth-child(6), /* \u4E0A\u4F20 */
        .table > thead > tr > th:nth-child(7), /* \u4E0B\u8F7D */
        .table > thead > tr > th:nth-child(8), /* \u603B\u4E0A\u4F20 */
        .table > thead > tr > th:nth-child(9), /* \u603B\u4E0B\u8F7D */
        .table > thead > tr > th:nth-child(10), /* \u8FD0\u884C\u65F6\u957F */
        .table > thead > tr > th:nth-child(11), /* \u6700\u540E\u66F4\u65B0 */
        #serverTableBody tr > td:nth-child(1), /* \u540D\u79F0 */
        #serverTableBody tr > td:nth-child(2), /* \u72B6\u6001 */
        #serverTableBody tr > td:nth-child(3), /* CPU */
        #serverTableBody tr > td:nth-child(4), /* \u5185\u5B58 */
        #serverTableBody tr > td:nth-child(5), /* \u786C\u76D8 */
        #serverTableBody tr > td:nth-child(6), /* \u4E0A\u4F20 */
        #serverTableBody tr > td:nth-child(7), /* \u4E0B\u8F7D */
        #serverTableBody tr > td:nth-child(8), /* \u603B\u4E0A\u4F20 */
        #serverTableBody tr > td:nth-child(9), /* \u603B\u4E0B\u8F7D */
        #serverTableBody tr > td:nth-child(10), /* \u8FD0\u884C\u65F6\u957F */
        #serverTableBody tr > td:nth-child(11) { /* \u6700\u540E\u66F4\u65B0 */
            text-align: center;
        }

        /* Front-end site monitoring table headers and data */
        .table > thead > tr > th:nth-child(1), /* \u540D\u79F0 (site table) */
        .table > thead > tr > th:nth-child(2), /* \u72B6\u6001 (site table) */
        .table > thead > tr > th:nth-child(3), /* \u72B6\u6001\u7801 (site table) */
        .table > thead > tr > th:nth-child(4), /* \u54CD\u5E94\u65F6\u95F4 (site table) */
        .table > thead > tr > th:nth-child(5), /* \u6700\u540E\u68C0\u67E5 (site table) */
        .table > thead > tr > th:nth-child(6), /* 24h\u8BB0\u5F55 (site table) */
        #siteStatusTableBody tr > td:nth-child(1), /* \u540D\u79F0 */
        #siteStatusTableBody tr > td:nth-child(2), /* \u72B6\u6001 */
        #siteStatusTableBody tr > td:nth-child(3), /* \u72B6\u6001\u7801 */
        #siteStatusTableBody tr > td:nth-child(4), /* \u54CD\u5E94\u65F6\u95F4 */
        #siteStatusTableBody tr > td:nth-child(5), /* \u6700\u540E\u68C0\u67E5 */
        #siteStatusTableBody tr > td:nth-child(6) { /* 24h\u8BB0\u5F55 */
            text-align: center;
        }

        /* Backend admin tables - center align headers and data columns */
        /* Admin server table headers */
        .table thead tr th:nth-child(2), /* ID */
        .table thead tr th:nth-child(3), /* \u540D\u79F0 */
        .table thead tr th:nth-child(4), /* \u63CF\u8FF0 */
        .table thead tr th:nth-child(5), /* \u72B6\u6001 */
        .table thead tr th:nth-child(6), /* \u6700\u540E\u66F4\u65B0 */
        .table thead tr th:nth-child(9), /* \u663E\u793A\u5F00\u5173 */
        /* Admin server table data */
        #serverTableBody tr > td:nth-child(2), /* ID */
        #serverTableBody tr > td:nth-child(3), /* \u540D\u79F0 */
        #serverTableBody tr > td:nth-child(4), /* \u63CF\u8FF0 */
        #serverTableBody tr > td:nth-child(5), /* \u72B6\u6001 */
        #serverTableBody tr > td:nth-child(6), /* \u6700\u540E\u66F4\u65B0 */
        #serverTableBody tr > td:nth-child(9) { /* \u663E\u793A\u5F00\u5173 */
            text-align: center;
        }

        /* Admin site table headers */
        .table thead tr th:nth-child(2), /* \u540D\u79F0 */
        .table thead tr th:nth-child(4), /* \u72B6\u6001 */
        .table thead tr th:nth-child(5), /* \u72B6\u6001\u7801 */
        .table thead tr th:nth-child(6), /* \u54CD\u5E94\u65F6\u95F4 */
        .table thead tr th:nth-child(7), /* \u6700\u540E\u68C0\u67E5 */
        .table thead tr th:nth-child(8), /* \u663E\u793A\u5F00\u5173 */
        /* Admin site table data */
        #siteTableBody tr > td:nth-child(2), /* \u540D\u79F0 */
        #siteTableBody tr > td:nth-child(4), /* \u72B6\u6001 */
        #siteTableBody tr > td:nth-child(5), /* \u72B6\u6001\u7801 */
        #siteTableBody tr > td:nth-child(6), /* \u54CD\u5E94\u65F6\u95F4 */
        #siteTableBody tr > td:nth-child(7), /* \u6700\u540E\u68C0\u67E5 */
        #siteTableBody tr > td:nth-child(8) { /* \u663E\u793A\u5F00\u5173 */
            text-align: center;
        }

        /* Dark Theme Adjustments */
        [data-bs-theme="dark"] body {
            background-color: #212529 !important; /* Bootstrap dark bg */
            color: #ffffff !important; /* White text for dark mode */
        }
        [data-bs-theme="dark"] h1, [data-bs-theme="dark"] h2, [data-bs-theme="dark"] h3, [data-bs-theme="dark"] h4, [data-bs-theme="dark"] h5, [data-bs-theme="dark"] h6 {
            color: #ffffff; /* White color for headings */
        }
        [data-bs-theme="dark"] a:not(.btn):not(.nav-link):not(.dropdown-item):not(.navbar-brand) {
            color: #87cefa; /* LightSkyBlue for general links, good contrast on dark */
        }
        [data-bs-theme="dark"] a:not(.btn):not(.nav-link):not(.dropdown-item):not(.navbar-brand):hover {
            color: #add8e6; /* Lighter blue on hover */
        }
        [data-bs-theme="dark"] .navbar-dark {
            background-color: #343a40 !important; /* Darker navbar */
        }
        [data-bs-theme="dark"] .table {
            color: #ffffff; /* White table text */
        }
        [data-bs-theme="dark"] .table-striped > tbody > tr:nth-of-type(odd) > * {
            --bs-table-accent-bg: rgba(255, 255, 255, 0.05); /* Darker stripe */
            color: #ffffff; /* Ensure text in striped rows is white */
        }
        [data-bs-theme="dark"] .table-hover > tbody > tr:hover > * {
            --bs-table-accent-bg: rgba(255, 255, 255, 0.075); /* Darker hover */
            color: #ffffff; /* Ensure text in hovered rows is white */
        }
        [data-bs-theme="dark"] .server-details-row td {
            background-color: rgba(33, 37, 41, var(--page-opacity, 0.8)); /* Darker details background with transparency */
            border-top: 1px solid #495057;
        }
        [data-bs-theme="dark"] .detail-item {
            background-color: rgba(52, 58, 64, var(--page-opacity, 0.8)); /* Darker detail item background with transparency */
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #ced4da; /* Consistent text color */
        }
        [data-bs-theme="dark"] .progress {
            background-color: #495057; /* Darker progress bar background */
        }
        [data-bs-theme="dark"] .progress span { /* Text on progress bar */
            color: #000000 !important; /* Black text for progress bars */
            text-shadow: none; /* Remove shadow for black text or use a very light one if needed */
        }
        [data-bs-theme="dark"] .footer.bg-light {
            background-color: #343a40 !important; /* Darker footer */
            border-top: 1px solid #495057;
        }
        /* \u5DF2\u79FB\u81F3\u7EDF\u4E00\u7684\u5E95\u90E8\u7248\u6743\u6837\u5F0F\u4E2D */
        [data-bs-theme="dark"] .alert-info {
            background-color: #17a2b8; /* Bootstrap info color, adjust if needed */
            color: #fff;
            border-color: #17a2b8;
        }
        [data-bs-theme="dark"] .btn-outline-light {
            color: #f8f9fa;
            border-color: #f8f9fa;
        }
        [data-bs-theme="dark"] .btn-outline-light:hover {
            color: #212529;
            background-color: #f8f9fa;
        }
        [data-bs-theme="dark"] .card {
            background-color: #343a40;
            border: 1px solid #495057;
        }
        [data-bs-theme="dark"] .card-header {
            background-color: #495057;
            border-bottom: 1px solid #5b6167;
        }
        [data-bs-theme="dark"] .modal-content {
            background-color: #343a40;
            color: #ffffff; /* White modal text */
        }
        [data-bs-theme="dark"] .modal-header {
            border-bottom-color: #495057;
        }
        [data-bs-theme="dark"] .modal-footer {
            border-top-color: #495057;
        }
        [data-bs-theme="dark"] .form-control {
            background-color: #495057;
            color: #ffffff; /* White form control text */
            border-color: #5b6167;
        }
        [data-bs-theme="dark"] .form-control:focus {
            background-color: #495057;
            color: #ffffff; /* White form control text on focus */
            border-color: #86b7fe; /* Bootstrap focus color */
            box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
        }
        [data-bs-theme="dark"] .form-label {
            color: #adb5bd;
        }
        [data-bs-theme="dark"] .text-danger { /* Ensure custom text-danger is visible */
            color: #d4d4d8 !important;
        }
        /* \u901A\u7528text-muted\u4E3B\u9898\u9002\u914D */
        .text-muted { color: #6c757d !important; }
        [data-bs-theme="dark"] .text-muted { color: #adb5bd !important; }
        [data-bs-theme="dark"] span[style*="color: #000"] { /* For inline styled black text */
            color: #ffffff !important; /* Change to white */
        }

        /* \u62D6\u62FD\u6392\u5E8F\u6837\u5F0F */
        .server-row-draggable, .site-row-draggable {
            transition: all 0.2s ease;
        }
        .server-row-draggable:hover, .site-row-draggable:hover {
            background-color: rgba(0, 123, 255, 0.1) !important;
        }
        .server-row-draggable.drag-over-top, .site-row-draggable.drag-over-top {
            border-top: 3px solid #007bff !important;
            background-color: rgba(0, 123, 255, 0.1) !important;
        }
        .server-row-draggable.drag-over-bottom, .site-row-draggable.drag-over-bottom {
            border-bottom: 3px solid #007bff !important;
            background-color: rgba(0, 123, 255, 0.1) !important;
        }
        .server-row-draggable[draggable="true"], .site-row-draggable[draggable="true"] {
            cursor: grab;
        }
        .server-row-draggable[draggable="true"]:active, .site-row-draggable[draggable="true"]:active {
            cursor: grabbing;
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u62D6\u62FD\u6837\u5F0F */
        [data-bs-theme="dark"] .server-row-draggable:hover,
        [data-bs-theme="dark"] .site-row-draggable:hover {
            background-color: rgba(13, 110, 253, 0.2) !important;
        }
        [data-bs-theme="dark"] .server-row-draggable.drag-over-top,
        [data-bs-theme="dark"] .site-row-draggable.drag-over-top {
            border-top: 3px solid #0d6efd !important;
            background-color: rgba(13, 110, 253, 0.2) !important;
        }
        [data-bs-theme="dark"] .server-row-draggable.drag-over-bottom,
        [data-bs-theme="dark"] .site-row-draggable.drag-over-bottom {
            border-bottom: 3px solid #0d6efd !important;
            background-color: rgba(13, 110, 253, 0.2) !important;
        }
    </style>
</head>
<body>
    <!-- Toast\u5BB9\u5668 -->
    <div id="toastContainer" class="toast-container"></div>

    <nav class="navbar navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">
                <span class="brand-mark"><i class="bi bi-activity"></i></span>
                <span class="brand-text">Monitor</span>
            </a>
            <div class="d-flex align-items-center">
                <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="btn btn-outline-light btn-sm me-2" title="GitHub Repository">
                    <i class="bi bi-github"></i>
                </a>
                <button id="themeToggler" class="btn btn-outline-light btn-sm me-2" title="\u5207\u6362\u4E3B\u9898">
                    <i class="bi bi-moon-stars-fill"></i>
                </button>
                <a class="nav-link text-light" id="adminAuthLink" href="/login.html" style="white-space: nowrap;" title="\u540E\u53F0">
                    <i class="bi bi-person-lock me-1"></i><span class="nav-link-label">\u540E\u53F0</span>
                </a>
            </div>
        </div>
    </nav>

    <main class="container monitor-shell">
    <!-- \u5355\u4E00\u4E3B\u5361\u7247\u5BB9\u5668 -->
    <div id="statusDashboardCard" class="dashboard-stack d-none">
                <!-- \u670D\u52A1\u5668\u76D1\u63A7\u90E8\u5206 -->
                <section id="serverStatusSection" class="dashboard-section mb-4 d-none">
                    <h5 class="card-title mb-3">
                        <i class="bi bi-server me-2"></i>\u670D\u52A1\u5668\u76D1\u63A7
                    </h5>

                    <div id="noServers" class="alert alert-info d-none">
                        \u6682\u65E0\u670D\u52A1\u5668\u6570\u636E\uFF0C\u8BF7\u5148\u767B\u5F55\u7BA1\u7406\u540E\u53F0\u6DFB\u52A0\u670D\u52A1\u5668\u3002
                    </div>

                    <!-- \u684C\u9762\u7AEF\u8868\u683C\u89C6\u56FE -->
                    <div class="table-responsive desktop-table-view">
                        <table class="table table-striped table-hover align-middle">
                            <thead>
                                <tr>
                                    <th>\u540D\u79F0</th>
                                    <th>\u72B6\u6001</th>
                                    <th>CPU</th>
                                    <th>\u5185\u5B58</th>
                                    <th>\u786C\u76D8</th>
                                    <th>\u4E0A\u4F20</th>
                                    <th>\u4E0B\u8F7D</th>
                                    <th>\u603B\u4E0A\u4F20</th>
                                    <th>\u603B\u4E0B\u8F7D</th>
                                    <th>\u8FD0\u884C\u65F6\u957F</th>
                                    <th>\u6700\u540E\u66F4\u65B0</th>
                                </tr>
                            </thead>
                            <tbody id="serverTableBody">
                                <tr>
                                    <td colspan="11" class="text-center">\u52A0\u8F7D\u4E2D...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- \u79FB\u52A8\u7AEF\u5361\u7247\u89C6\u56FE -->
                    <div class="mobile-card-container mobile-only-view" id="mobileServerContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">\u52A0\u8F7D\u4E2D...</span>
                            </div>
                            <div class="mt-2">\u52A0\u8F7D\u670D\u52A1\u5668\u6570\u636E\u4E2D...</div>
                        </div>
                    </div>
                </section>

                <!-- \u5206\u9694\u7EBF -->
                <hr id="statusSectionDivider" class="my-4 d-none">

                <!-- \u7F51\u7AD9\u76D1\u63A7\u90E8\u5206 -->
                <section id="siteStatusSection" class="dashboard-section d-none">
                    <h5 class="card-title mb-3">
                        <i class="bi bi-globe me-2"></i>\u7F51\u7AD9\u5728\u7EBF\u72B6\u6001
                    </h5>

                    <div id="noSites" class="alert alert-info d-none">
                        \u6682\u65E0\u76D1\u63A7\u7F51\u7AD9\u6570\u636E\u3002
                    </div>

                    <!-- \u684C\u9762\u7AEF\u8868\u683C\u89C6\u56FE -->
                    <div class="table-responsive desktop-table-view">
                        <table class="table table-striped table-hover align-middle">
                            <thead>
                                <tr>
                                    <th>\u540D\u79F0</th>
                                    <th>\u72B6\u6001</th>
                                    <th>\u72B6\u6001\u7801</th>
                                    <th>\u54CD\u5E94\u65F6\u95F4 (ms)</th>
                                    <th>\u6700\u540E\u68C0\u67E5</th>
                                    <th>24h\u8BB0\u5F55</th>
                                </tr>
                            </thead>
                            <tbody id="siteStatusTableBody">
                                <tr>
                                    <td colspan="6" class="text-center">\u52A0\u8F7D\u4E2D...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- \u79FB\u52A8\u7AEF\u5361\u7247\u89C6\u56FE -->
                    <div class="mobile-card-container mobile-only-view" id="mobileSiteContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">\u52A0\u8F7D\u4E2D...</span>
                            </div>
                            <div class="mt-2">\u52A0\u8F7D\u7F51\u7AD9\u6570\u636E\u4E2D...</div>
                        </div>
                    </div>
                </section>
    </div>
    </main>
    <!-- End Website Status Section -->

    <!-- Server Detailed row template (hidden by default) -->
    <template id="serverDetailsTemplate">
        <tr class="server-details-row d-none">
            <td colspan="11">
                <div class="server-details-content">
                    <!-- Detailed metrics will be populated here by JavaScript -->
                </div>
            </td>
        </tr>
    </template>

    <footer class="footer app-footer py-4">
        <div class="container text-center">
            <span class="text-muted small">VPS\u76D1\u63A7\u9762\u677F &copy; ${(/* @__PURE__ */ new Date()).getFullYear()}</span>
            <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="ms-3 text-muted" title="GitHub Repository">
                <i class="bi bi-github"></i>
            </a>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-C6RzsynM9kWDrMNeT87bh95OGNyZPhcTNXj1NW7RuBCsyN/o0jlpcV8Qyq46cDfL" crossorigin="anonymous"><\/script>
    <script src="/js/main.js"><\/script>
</body>
</html>`;
}
__name(getIndexHtml, "getIndexHtml");
function getLoginHtml() {
  return `<!DOCTYPE html>
<html lang="zh-CN" data-bs-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>\u767B\u5F55 - VPS\u76D1\u63A7\u9762\u677F</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg">
    <script>
        // \u7ACB\u5373\u8BBE\u7F6E\u4E3B\u9898\uFF0C\u907F\u514D\u95EA\u70C1
        (function() {
            const theme = localStorage.getItem('vps-monitor-theme') || 'light';
            document.documentElement.setAttribute('data-bs-theme', theme);
        })();
    <\/script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN" crossorigin="anonymous">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css" rel="stylesheet" integrity="sha384-4LISF5TTJX/fLmGSxO53rV4miRxdg84mZsxmO8Rx5jGtp/LbrixFETvWa5a6sESd" crossorigin="anonymous">
    <link href="/css/style.css" rel="stylesheet">
    <style>
        .server-row {
            cursor: pointer; /* Indicate clickable rows */
        }
        .server-details-row {
            /* display: none; /* Initially hidden - controlled by JS */ */
        }
        .server-details-row td {
            padding: 1rem;
            background-color: rgba(248, 249, 250, var(--page-opacity, 0.8)); /* Light background for details with transparency */
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u670D\u52A1\u5668\u8BE6\u7EC6\u4FE1\u606F\u884C */
        [data-bs-theme="dark"] .server-details-row td {
            background-color: rgba(33, 37, 41, var(--page-opacity, 0.8));
        }
        .server-details-content {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }
        .detail-item {
            background-color: rgba(233, 236, 239, var(--page-opacity, 0.8));
            padding: 0.75rem;
            border-radius: 0.25rem;
            border: 1px solid rgba(0, 0, 0, 0.1);
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u8BE6\u7EC6\u4FE1\u606F\u9879 */
        [data-bs-theme="dark"] .detail-item {
            background-color: rgba(52, 58, 64, var(--page-opacity, 0.8));
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #e0e0e0;
        }
        .detail-item strong {
            display: block;
            margin-bottom: 0.25rem;
        }
        .history-bar-container {
            display: inline-flex; /* Changed to inline-flex for centering within td */
            flex-direction: row-reverse; /* Newest on the right */
            align-items: center;
            justify-content: center; /* Center the bars within this container */
            height: 25px; /* Increased height */
            gap: 2px; /* Space between bars */
        }
        .history-bar {
            width: 8px; /* Increased width of each bar */
            height: 100%;
            /* margin-left: 1px; /* Replaced by gap */
            border-radius: 1px;
        }
        .history-bar-up { background-color: #28a745; } /* Green */
        .history-bar-down { background-color: #dc3545; } /* Red */
        .history-bar-pending { background-color: #6c757d; } /* Gray */

        /* Default styling for progress bar text (light mode) */
        .progress span {
            color: #000000; /* Black text for progress bars by default */
            /* font-weight: bold; is handled by inline style in JS */
        }

        /* Center the "24h\u8BB0\u5F55" (site table) and "\u4E0A\u4F20" (server table) headers and their data cells */
        .table > thead > tr > th:nth-child(6), /* Targets 6th header in both tables */
        #siteStatusTableBody tr > td:nth-child(6), /* Targets 6th data cell in site status table */
        #serverTableBody tr > td:nth-child(6) { /* Targets 6th data cell in server status table */
            text-align: center;
        }

        /* Dark Theme Adjustments */
        [data-bs-theme="dark"] body {
            background-color: #212529; /* Bootstrap dark bg */
            color: #ffffff; /* White text for dark mode */
        }
        [data-bs-theme="dark"] h1, [data-bs-theme="dark"] h2, [data-bs-theme="dark"] h3, [data-bs-theme="dark"] h4, [data-bs-theme="dark"] h5, [data-bs-theme="dark"] h6 {
            color: #ffffff; /* White color for headings */
        }
        [data-bs-theme="dark"] a:not(.btn):not(.nav-link):not(.dropdown-item):not(.navbar-brand) {
            color: #87cefa; /* LightSkyBlue for general links, good contrast on dark */
        }
        [data-bs-theme="dark"] a:not(.btn):not(.nav-link):not(.dropdown-item):not(.navbar-brand):hover {
            color: #add8e6; /* Lighter blue on hover */
        }
        [data-bs-theme="dark"] .navbar-dark {
            background-color: #343a40 !important; /* Darker navbar */
        }
        [data-bs-theme="dark"] .table {
            color: #ffffff; /* White table text */
        }
        [data-bs-theme="dark"] .table-striped > tbody > tr:nth-of-type(odd) > * {
            --bs-table-accent-bg: rgba(255, 255, 255, 0.05); /* Darker stripe */
            color: #ffffff; /* Ensure text in striped rows is white */
        }
        [data-bs-theme="dark"] .table-hover > tbody > tr:hover > * {
            --bs-table-accent-bg: rgba(255, 255, 255, 0.075); /* Darker hover */
            color: #ffffff; /* Ensure text in hovered rows is white */
        }
        [data-bs-theme="dark"] .server-details-row td {
            background-color: rgba(33, 37, 41, var(--page-opacity, 0.8)); /* Darker details background with transparency */
            border-top: 1px solid #495057;
        }
        [data-bs-theme="dark"] .detail-item {
            background-color: rgba(52, 58, 64, var(--page-opacity, 0.8)); /* Darker detail item background with transparency */
            border: 1px solid rgba(255, 255, 255, 0.1);
            color: #ced4da; /* Consistent text color */
        }
        [data-bs-theme="dark"] .progress {
            background-color: #495057; /* Darker progress bar background */
        }
        [data-bs-theme="dark"] .progress span { /* Text on progress bar */
            color: #000000 !important; /* Black text for progress bars */
            text-shadow: none; /* Remove shadow for black text or use a very light one if needed */
        }
        [data-bs-theme="dark"] .footer.bg-light {
            background-color: #343a40 !important; /* Darker footer */
            border-top: 1px solid #495057;
        }
        /* \u5DF2\u79FB\u81F3\u7EDF\u4E00\u7684\u5E95\u90E8\u7248\u6743\u6837\u5F0F\u4E2D */
        [data-bs-theme="dark"] .alert-info {
            background-color: #17a2b8; /* Bootstrap info color, adjust if needed */
            color: #fff;
            border-color: #17a2b8;
        }
        [data-bs-theme="dark"] .btn-outline-light {
            color: #f8f9fa;
            border-color: #f8f9fa;
        }
        [data-bs-theme="dark"] .btn-outline-light:hover {
            color: #212529;
            background-color: #f8f9fa;
        }
        [data-bs-theme="dark"] .card {
            background-color: #343a40;
            border: 1px solid #495057;
        }
        [data-bs-theme="dark"] .card-header {
            background-color: #495057;
            border-bottom: 1px solid #5b6167;
        }
        [data-bs-theme="dark"] .modal-content {
            background-color: #343a40;
            color: #ffffff; /* White modal text */
        }
        [data-bs-theme="dark"] .modal-header {
            border-bottom-color: #495057;
        }
        [data-bs-theme="dark"] .modal-footer {
            border-top-color: #495057;
        }
        [data-bs-theme="dark"] .form-control {
            background-color: #495057;
            color: #ffffff; /* White form control text */
            border-color: #5b6167;
        }
        [data-bs-theme="dark"] .form-control:focus {
            background-color: #495057;
            color: #ffffff; /* White form control text on focus */
            border-color: #86b7fe; /* Bootstrap focus color */
            box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
        }
        [data-bs-theme="dark"] .form-label {
            color: #adb5bd;
        }
        [data-bs-theme="dark"] .text-danger { /* Ensure custom text-danger is visible */
            color: #d4d4d8 !important;
        }
        /* \u5DF2\u79FB\u81F3\u7EDF\u4E00\u7684\u901A\u7528text-muted\u6837\u5F0F\u4E2D */
        [data-bs-theme="dark"] span[style*="color: #000"] { /* For inline styled black text */
            color: #ffffff !important; /* Change to white */
        }
    </style>
</head>
<body>
    <!-- Toast\u5BB9\u5668 -->
    <div id="toastContainer" class="toast-container"></div>

    <nav class="navbar navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">
                <svg class="me-2" width="32" height="32" viewBox="0 0 32 32">
                    <defs>
                        <radialGradient id="navBg2" cx="0.3" cy="0.3">
                            <stop offset="0%" stop-color="#fff" stop-opacity="0.9"/>
                            <stop offset="100%" stop-color="#0277bd" stop-opacity="0.8"/>
                        </radialGradient>
                        <linearGradient id="navEcg2" x1="0%" x2="100%">
                            <stop offset="0%" stop-color="#f08"/>
                            <stop offset="50%" stop-color="#0f8"/>
                            <stop offset="100%" stop-color="#80f"/>
                        </linearGradient>
                    </defs>
                    <circle cx="16" cy="16" r="15" fill="url(#navBg2)" stroke="#0277bd" stroke-width="1.5"/>
                    <circle cx="16" cy="16" r="13" fill="none" stroke="#fff" stroke-width="1" opacity="0.4"/>
                    <line x1="4" y1="16" x2="28" y2="16" stroke="#b3e5fc" stroke-width="0.5" opacity="0.8"/>
                    <path id="navP2" d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="url(#navEcg2)" stroke-width="2.8"/>
                    <path d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="#fff" stroke-width="1.2" opacity="0.7"/>
                    <circle r="1.5" fill="#fff">
                        <animateMotion dur="2s" repeatCount="indefinite">
                            <mpath href="#navP2"/>
                        </animateMotion>
                    </circle>
                    <circle cx="16" cy="16" r="8" fill="none" stroke="#f08" stroke-width="0.5" opacity="0.6">
                        <animate attributeName="r" values="8;12;8" dur="3s" repeatCount="indefinite"/>
                        <animate attributeName="opacity" values="0.6;0;0.6" dur="3s" repeatCount="indefinite"/>
                    </circle>
                </svg>
                VPS\u76D1\u63A7\u9762\u677F
            </a>
            <div class="d-flex align-items-center">
                <button id="themeToggler" class="btn btn-outline-light btn-sm me-2" title="\u5207\u6362\u4E3B\u9898">
                    <i class="bi bi-moon-stars-fill"></i>
                </button>
                <a class="nav-link text-light" href="/" style="white-space: nowrap;">\u8FD4\u56DE\u9996\u9875</a>
            </div>
        </div>
    </nav>

    <main class="container login-shell">
        <div class="login-panel-wrap">
            <div class="card login-card">
                <div class="card-header login-card-header">
                    <div class="login-icon"><i class="bi bi-shield-lock"></i></div>
                    <div>
                        <div class="login-kicker">Secure Access</div>
                        <h4 class="card-title mb-0">\u7BA1\u7406\u5458\u767B\u5F55</h4>
                    </div>
                </div>
                <div class="card-body login-card-body">

                    <form id="loginForm">
                        <div class="mb-3">
                            <label for="username" class="form-label">\u7528\u6237\u540D</label>
                            <input type="text" class="form-control" id="username" autocomplete="username" required>
                        </div>
                        <div class="mb-4">
                            <label for="password" class="form-label">\u5BC6\u7801</label>
                            <input type="password" class="form-control" id="password" autocomplete="current-password" required>
                        </div>
                        <div class="d-grid">
                            <button type="submit" class="btn btn-primary login-submit">
                                <i class="bi bi-arrow-right-circle"></i>
                                <span>\u767B\u5F55</span>
                            </button>
                        </div>
                    </form>
                </div>
                <div class="card-footer login-card-footer text-muted">
                    <i class="bi bi-info-circle"></i>
                    <small id="defaultCredentialsInfo">\u52A0\u8F7D\u9ED8\u8BA4\u51ED\u636E\u4FE1\u606F\u4E2D...</small>
                </div>
            </div>
        </div>
    </main>

    <footer class="footer app-footer py-4">
        <div class="container text-center">
            <span class="text-muted small">VPS\u76D1\u63A7\u9762\u677F &copy; ${(/* @__PURE__ */ new Date()).getFullYear()}</span>
            <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="ms-3 text-muted" title="GitHub Repository">
                <i class="bi bi-github"></i>
            </a>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-C6RzsynM9kWDrMNeT87bh95OGNyZPhcTNXj1NW7RuBCsyN/o0jlpcV8Qyq46cDfL" crossorigin="anonymous"><\/script>
    <script src="/js/login.js"><\/script>
</body>
</html>`;
}
__name(getLoginHtml, "getLoginHtml");
function getAdminHtml() {
  return `<!DOCTYPE html>
<html lang="zh-CN" data-bs-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>\u7BA1\u7406\u540E\u53F0 - VPS\u76D1\u63A7\u9762\u677F</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg">
    <script>
        // \u7ACB\u5373\u8BBE\u7F6E\u4E3B\u9898\uFF0C\u907F\u514D\u95EA\u70C1
        (function() {
            const theme = localStorage.getItem('vps-monitor-theme') || 'light';
            document.documentElement.setAttribute('data-bs-theme', theme);
        })();
    <\/script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN" crossorigin="anonymous">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css" rel="stylesheet" integrity="sha384-4LISF5TTJX/fLmGSxO53rV4miRxdg84mZsxmO8Rx5jGtp/LbrixFETvWa5a6sESd" crossorigin="anonymous">
    <link href="/css/style.css" rel="stylesheet">
</head>
<body>
    <!-- Toast\u5BB9\u5668 -->
    <div id="toastContainer" class="toast-container"></div>

    <nav class="navbar navbar-dark bg-primary">
        <div class="container">
            <a class="navbar-brand" href="/">
                <svg class="me-2" width="32" height="32" viewBox="0 0 32 32">
                    <defs>
                        <radialGradient id="navBg3" cx="0.3" cy="0.3">
                            <stop offset="0%" stop-color="#fff" stop-opacity="0.9"/>
                            <stop offset="100%" stop-color="#0277bd" stop-opacity="0.8"/>
                        </radialGradient>
                        <linearGradient id="navEcg3" x1="0%" x2="100%">
                            <stop offset="0%" stop-color="#f08"/>
                            <stop offset="50%" stop-color="#0f8"/>
                            <stop offset="100%" stop-color="#80f"/>
                        </linearGradient>
                    </defs>
                    <circle cx="16" cy="16" r="15" fill="url(#navBg3)" stroke="#0277bd" stroke-width="1.5"/>
                    <circle cx="16" cy="16" r="13" fill="none" stroke="#fff" stroke-width="1" opacity="0.4"/>
                    <line x1="4" y1="16" x2="28" y2="16" stroke="#b3e5fc" stroke-width="0.5" opacity="0.8"/>
                    <path id="navP3" d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="url(#navEcg3)" stroke-width="2.8"/>
                    <path d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="#fff" stroke-width="1.2" opacity="0.7"/>
                    <circle r="1.5" fill="#fff">
                        <animateMotion dur="2s" repeatCount="indefinite">
                            <mpath href="#navP3"/>
                        </animateMotion>
                    </circle>
                    <circle cx="16" cy="16" r="8" fill="none" stroke="#f08" stroke-width="0.5" opacity="0.6">
                        <animate attributeName="r" values="8;12;8" dur="3s" repeatCount="indefinite"/>
                        <animate attributeName="opacity" values="0.6;0;0.6" dur="3s" repeatCount="indefinite"/>
                    </circle>
                </svg>
                VPS\u76D1\u63A7\u9762\u677F
            </a>
            <div class="d-flex align-items-center flex-wrap">
                <a class="nav-link text-light me-2" href="/" style="white-space: nowrap;" title="\u8FD4\u56DE\u9996\u9875">
                    <i class="bi bi-house-door me-1"></i><span class="nav-link-label">\u9996\u9875</span>
                </a>

                <!-- PC\u7AEF\u76F4\u63A5\u663E\u793A\u7684\u6309\u94AE -->
                <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="btn btn-outline-light btn-sm me-2 desktop-only" title="GitHub Repository">
                    <i class="bi bi-github"></i>
                </a>

                <button id="themeToggler" class="btn btn-outline-light btn-sm me-2" title="\u5207\u6362\u4E3B\u9898">
                    <i class="bi bi-moon-stars-fill"></i>
                </button>

                <button class="btn btn-outline-light btn-sm me-1 desktop-only" id="changePasswordBtnDesktop" title="\u4FEE\u6539\u5BC6\u7801">
                    <i class="bi bi-key"></i>
                </button>

                <!-- \u79FB\u52A8\u7AEF\u4E0B\u62C9\u83DC\u5355 -->
                <div class="dropdown me-1 mobile-only">
                    <button class="btn btn-outline-light btn-sm dropdown-toggle" type="button" id="adminMenuDropdown" data-bs-toggle="dropdown" aria-expanded="false" title="\u66F4\u591A\u9009\u9879">
                        <i class="bi bi-three-dots"></i>
                    </button>
                    <ul class="dropdown-menu dropdown-menu-end" aria-labelledby="adminMenuDropdown">
                        <li><a class="dropdown-item" href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer">
                            <i class="bi bi-github me-2"></i>GitHub
                        </a></li>
                        <li><button class="dropdown-item" id="changePasswordBtn">
                            <i class="bi bi-key me-2"></i>\u4FEE\u6539\u5BC6\u7801
                        </button></li>
                    </ul>
                </div>

                <button id="logoutBtn" class="btn btn-outline-light btn-sm" title="\u9000\u51FA\u767B\u5F55" style="font-size: 0.75rem; padding: 0.25rem 0.5rem;">
                    <i class="bi bi-box-arrow-right"></i>
                </button>
            </div>
        </div>
    </nav>

    <main class="container monitor-shell admin-shell">
    <!-- \u5355\u4E00\u4E3B\u7BA1\u7406\u5361\u7247\u5BB9\u5668 -->
    <div>
        <div class="card shadow-sm">
            <div class="card-body">
                <!-- \u670D\u52A1\u5668\u7BA1\u7406\u90E8\u5206 -->
                <div class="mb-4">
                    <div class="admin-header-row mb-3">
                        <div class="admin-header-title">
                            <h5 class="card-title mb-0">
                                <i class="bi bi-server me-2"></i>\u670D\u52A1\u5668\u7BA1\u7406
                            </h5>
                            <div class="section-display-switch mt-2">
                                <div class="form-check form-switch">
                                    <input class="form-check-input" type="checkbox" id="showServerSectionToggle">
                                    <label class="form-check-label" for="showServerSectionToggle">
                                        <i class="bi bi-eye me-1"></i>\u9996\u9875\u5C55\u793A\u670D\u52A1\u5668\u76D1\u63A7
                                    </label>
                                </div>
                            </div>
                        </div>
                        <div class="admin-header-content">
                            <!-- VPS Data Update Frequency Form -->
                            <form id="globalSettingsFormPartial" class="admin-settings-form">
                                <div class="settings-group">
                                    <label for="vpsReportInterval" class="form-label">VPS\u6570\u636E\u66F4\u65B0\u9891\u7387 (\u79D2):</label>
                                    <div class="input-group">
                                        <input type="number" class="form-control form-control-sm" id="vpsReportInterval" placeholder="\u4F8B\u5982: 60" min="1" style="width: 100px;">
                                        <button type="button" id="saveVpsReportIntervalBtn" class="btn btn-info btn-sm">\u4FDD\u5B58\u9891\u7387</button>
                                    </div>
                                </div>
                            </form>

                            <!-- Action Buttons Group -->
                            <div class="admin-actions-group">
                                <!-- Server Auto Sort Dropdown -->
                                <div class="dropdown me-2">
                                    <button class="btn btn-outline-secondary dropdown-toggle" type="button" id="serverAutoSortDropdown" data-bs-toggle="dropdown" aria-expanded="false">
                                        <i class="bi bi-sort-alpha-down"></i> \u81EA\u52A8\u6392\u5E8F
                                    </button>
                                    <ul class="dropdown-menu" aria-labelledby="serverAutoSortDropdown">
                                        <li><a class="dropdown-item active" href="#" onclick="autoSortServers('custom')">\u81EA\u5B9A\u4E49\u6392\u5E8F</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortServers('name')">\u6309\u540D\u79F0\u6392\u5E8F</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortServers('status')">\u6309\u72B6\u6001\u6392\u5E8F</a></li>
                                    </ul>
                                </div>

                                <!-- Add Server Button -->
                                <button id="addServerBtn" class="btn btn-primary">
                                    <i class="bi bi-plus-circle"></i> \u6DFB\u52A0\u670D\u52A1\u5668
                                </button>
                            </div>
                        </div>
                    </div>



                    <!-- \u684C\u9762\u7AEF\u8868\u683C\u89C6\u56FE -->
                    <div class="table-responsive">
                        <table class="table table-striped table-hover">
                            <thead>
                                <tr>
                                    <th>\u6392\u5E8F</th>
                                    <th>ID</th>
                                    <th>\u540D\u79F0</th>
                                    <th>\u63CF\u8FF0</th>
                                    <th>\u72B6\u6001</th>
                                    <th>\u6700\u540E\u66F4\u65B0</th>
                                    <th>API\u5BC6\u94A5</th>
                                    <th>VPS\u811A\u672C</th>
                                    <th>\u663E\u793A <i class="bi bi-question-circle text-muted" data-bs-toggle="tooltip" data-bs-placement="top" data-bs-title="\u662F\u5426\u5BF9\u6E38\u5BA2\u5C55\u793A\u6B64\u670D\u52A1\u5668"></i></th>
                                    <th>\u64CD\u4F5C</th>
                                </tr>
                            </thead>
                            <tbody id="serverTableBody">
                                <tr>
                                    <td colspan="10" class="text-center">\u52A0\u8F7D\u4E2D...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- \u79FB\u52A8\u7AEF\u5361\u7247\u89C6\u56FE -->
                    <div class="mobile-card-container" id="mobileAdminServerContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">\u52A0\u8F7D\u4E2D...</span>
                            </div>
                            <div class="mt-2">\u52A0\u8F7D\u670D\u52A1\u5668\u6570\u636E\u4E2D...</div>
                        </div>
                    </div>
                </div>

                <!-- \u5206\u9694\u7EBF -->
                <hr class="my-4">

                <!-- \u7F51\u7AD9\u76D1\u63A7\u7BA1\u7406\u90E8\u5206 -->
                <div>
                    <div class="admin-header-row mb-3">
                        <div class="admin-header-title">
                            <h5 class="card-title mb-0">
                                <i class="bi bi-globe me-2"></i>\u7F51\u7AD9\u76D1\u63A7\u7BA1\u7406
                            </h5>
                            <div class="section-display-switch mt-2">
                                <div class="form-check form-switch">
                                    <input class="form-check-input" type="checkbox" id="showSiteSectionToggle">
                                    <label class="form-check-label" for="showSiteSectionToggle">
                                        <i class="bi bi-eye me-1"></i>\u9996\u9875\u5C55\u793A\u7F51\u7AD9\u5728\u7EBF\u72B6\u6001
                                    </label>
                                </div>
                            </div>
                        </div>
                        <div class="admin-header-content">
                            <!-- Action Buttons Group - \u684C\u9762\u7AEF\u9690\u85CF\uFF0C\u79FB\u52A8\u7AEF\u663E\u793A\u5C45\u4E2D\u6309\u94AE -->
                            <div class="admin-actions-group desktop-only">
                                <!-- Site Auto Sort Dropdown -->
                                <div class="dropdown me-2">
                                    <button class="btn btn-outline-secondary dropdown-toggle" type="button" id="siteAutoSortDropdown" data-bs-toggle="dropdown" aria-expanded="false">
                                        <i class="bi bi-sort-alpha-down"></i> \u81EA\u52A8\u6392\u5E8F
                                    </button>
                                    <ul class="dropdown-menu" aria-labelledby="siteAutoSortDropdown">
                                        <li><a class="dropdown-item active" href="#" onclick="autoSortSites('custom')">\u81EA\u5B9A\u4E49\u6392\u5E8F</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortSites('name')">\u6309\u540D\u79F0\u6392\u5E8F</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortSites('url')">\u6309URL\u6392\u5E8F</a></li>
                                        <li><a class="dropdown-item" href="#" onclick="autoSortSites('status')">\u6309\u72B6\u6001\u6392\u5E8F</a></li>
                                    </ul>
                                </div>

                                <button id="addSiteBtn" class="btn btn-success">
                                    <i class="bi bi-plus-circle"></i> \u6DFB\u52A0\u76D1\u63A7\u7F51\u7AD9
                                </button>
                            </div>
                        </div>
                    </div>


                    <!-- \u684C\u9762\u7AEF\u8868\u683C\u89C6\u56FE -->
                    <div class="table-responsive">
                        <table class="table table-striped table-hover">
                            <thead>
                                <tr>
                                    <th>\u6392\u5E8F</th>
                                    <th>\u540D\u79F0</th>
                                    <th>URL</th>
                                    <th>\u72B6\u6001</th>
                                    <th>\u72B6\u6001\u7801</th>
                                    <th>\u54CD\u5E94\u65F6\u95F4 (ms)</th>
                                    <th>\u6700\u540E\u68C0\u67E5</th>
                                    <th>\u663E\u793A <i class="bi bi-question-circle text-muted" data-bs-toggle="tooltip" data-bs-placement="top" data-bs-title="\u662F\u5426\u5BF9\u6E38\u5BA2\u5C55\u793A\u6B64\u7F51\u7AD9"></i></th>
                                    <th>\u64CD\u4F5C</th>
                                </tr>
                            </thead>
                            <tbody id="siteTableBody">
                                <tr>
                                    <td colspan="9" class="text-center">\u52A0\u8F7D\u4E2D...</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- \u79FB\u52A8\u7AEF\u5361\u7247\u89C6\u56FE -->
                    <div class="mobile-card-container" id="mobileAdminSiteContainer">
                        <div class="text-center p-3">
                            <div class="spinner-border text-primary" role="status">
                                <span class="visually-hidden">\u52A0\u8F7D\u4E2D...</span>
                            </div>
                            <div class="mt-2">\u52A0\u8F7D\u7F51\u7AD9\u6570\u636E\u4E2D...</div>
                        </div>
                    </div>
                </div>

                <!-- \u5206\u9694\u7EBF -->
                <hr class="my-4">

                <!-- Telegram \u901A\u77E5\u8BBE\u7F6E\u90E8\u5206 -->
                <div>
                    <h5 class="card-title mb-3">
                        <i class="bi bi-telegram me-2"></i>Telegram \u901A\u77E5\u8BBE\u7F6E
                    </h5>



                    <form id="telegramSettingsForm">
                        <div class="mb-3">
                            <label for="telegramBotToken" class="form-label">Bot Token</label>
                            <input type="text" class="form-control" id="telegramBotToken" placeholder="\u8BF7\u8F93\u5165 Telegram Bot Token">
                        </div>
                        <div class="mb-3">
                            <label for="telegramChatId" class="form-label">Chat ID</label>
                            <input type="text" class="form-control" id="telegramChatId" placeholder="\u8BF7\u8F93\u5165\u63A5\u6536\u901A\u77E5\u7684 Chat ID">
                        </div>
                        <div class="form-check mb-3">
                            <input class="form-check-input" type="checkbox" id="enableTelegramNotifications">
                            <label class="form-check-label" for="enableTelegramNotifications">
                                \u542F\u7528\u901A\u77E5
                            </label>
                        </div>
                        <button type="button" id="saveTelegramSettingsBtn" class="btn btn-info">\u4FDD\u5B58Telegram\u8BBE\u7F6E</button>
                    </form>
                </div>

                <!-- \u5206\u9694\u7EBF -->
                <hr class="my-4">

                <!-- \u80CC\u666F\u8BBE\u7F6E\u90E8\u5206 -->
                <div>
                    <h5 class="card-title mb-3">
                        <i class="bi bi-image me-2"></i>\u80CC\u666F\u8BBE\u7F6E
                    </h5>



                    <form id="backgroundSettingsForm">
                        <div class="form-check mb-3">
                            <input class="form-check-input" type="checkbox" id="enableCustomBackground">
                            <label class="form-check-label" for="enableCustomBackground">
                                \u542F\u7528\u81EA\u5B9A\u4E49\u80CC\u666F
                            </label>
                        </div>
                        <div class="mb-3">
                            <label for="backgroundImageUrl" class="form-label">\u80CC\u666F\u56FE\u7247URL</label>
                            <input type="url" class="form-control" id="backgroundImageUrl" placeholder="\u8BF7\u8F93\u5165\u80CC\u666F\u56FE\u7247URL (\u5FC5\u987B\u4EE5https://\u5F00\u5934)">
                            <div class="form-text">\u5EFA\u8BAE\u4F7F\u7528\u9AD8\u8D28\u91CF\u56FE\u7247\uFF0C\u652F\u6301JPG\u3001PNG\u683C\u5F0F</div>
                        </div>
                        <div class="mb-3">
                            <label for="pageOpacity" class="form-label">\u9875\u9762\u900F\u660E\u5EA6: <span id="opacityValue">80</span>%</label>
                            <input type="range" class="form-range" id="pageOpacity" min="0" max="100" value="80" step="1">
                            <div class="form-text">\u8C03\u6574\u9875\u9762\u5143\u7D20\u7684\u900F\u660E\u5EA6\uFF0C\u6570\u503C\u8D8A\u5C0F\u8D8A\u900F\u660E</div>
                        </div>
                        <button type="button" id="saveBackgroundSettingsBtn" class="btn btn-info">\u4FDD\u5B58\u80CC\u666F\u8BBE\u7F6E</button>
                    </form>
                </div>
            </div>
        </div>
    </div>
    </main>

    <!-- Global Settings Section (Now integrated above Server Management List) -->
    <!-- The form is now part of the header for Server Management -->
    <!-- End Global Settings Section -->


    <!-- \u670D\u52A1\u5668\u6A21\u6001\u6846 -->
    <div class="modal fade" id="serverModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="serverModalTitle">\u6DFB\u52A0\u670D\u52A1\u5668</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <form id="serverForm">
                        <input type="hidden" id="serverId">
                        <div class="mb-3">
                            <label for="serverName" class="form-label">\u670D\u52A1\u5668\u540D\u79F0</label>
                            <input type="text" class="form-control" id="serverName" required>
                        </div>
                        <div class="mb-3">
                            <label for="serverDescription" class="form-label">\u63CF\u8FF0\uFF08\u53EF\u9009\uFF09</label>
                            <textarea class="form-control" id="serverDescription" rows="2"></textarea>
                        </div>
                        <!-- Removed serverEnableFrequentNotifications checkbox -->

                        <div id="serverIdDisplayGroup" class="mb-3 d-none">
                            <label for="serverIdDisplay" class="form-label">\u670D\u52A1\u5668ID</label>
                            <div class="input-group">
                                <input type="text" class="form-control" id="serverIdDisplay" readonly>
                                <button class="btn btn-outline-secondary" type="button" id="copyServerIdBtn">
                                    <i class="bi bi-clipboard"></i>
                                </button>
                            </div>
                        </div>

                        <div id="apiKeyGroup" class="mb-3 d-none">
                            <label for="apiKey" class="form-label">API\u5BC6\u94A5</label>
                            <div class="input-group">
                                <input type="text" class="form-control" id="apiKey" readonly>
                                <button class="btn btn-outline-secondary" type="button" id="copyApiKeyBtn">
                                    <i class="bi bi-clipboard"></i>
                                </button>
                            </div>
                        </div>

                        <div id="workerUrlDisplayGroup" class="mb-3 d-none">
                            <label for="workerUrlDisplay" class="form-label">Worker \u5730\u5740</label>
                            <div class="input-group">
                                <input type="text" class="form-control" id="workerUrlDisplay" readonly>
                                <button class="btn btn-outline-secondary" type="button" id="copyWorkerUrlBtn">
                                    <i class="bi bi-clipboard"></i>
                                </button>
                            </div>
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">\u5173\u95ED</button>
                    <button type="button" class="btn btn-primary" id="saveServerBtn">\u4FDD\u5B58</button>
                </div>
            </div>
        </div>
    </div>

    <!-- \u7F51\u7AD9\u76D1\u63A7\u6A21\u6001\u6846 -->
    <div class="modal fade" id="siteModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="siteModalTitle">\u6DFB\u52A0\u76D1\u63A7\u7F51\u7AD9</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <form id="siteForm">
                        <input type="hidden" id="siteId">
                        <div class="mb-3">
                            <label for="siteName" class="form-label">\u7F51\u7AD9\u540D\u79F0\uFF08\u53EF\u9009\uFF09</label>
                            <input type="text" class="form-control" id="siteName">
                        </div>
                        <div class="mb-3">
                            <label for="siteUrl" class="form-label">\u7F51\u7AD9URL</label>
                            <input type="url" class="form-control" id="siteUrl" placeholder="https://example.com" required>
                        </div>
                        <!-- Removed siteEnableFrequentNotifications checkbox -->
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">\u5173\u95ED</button>
                    <button type="button" class="btn btn-primary" id="saveSiteBtn">\u4FDD\u5B58</button>
                </div>
            </div>
        </div>
    </div>

    <!-- \u670D\u52A1\u5668\u5220\u9664\u786E\u8BA4\u6A21\u6001\u6846 -->
    <div class="modal fade" id="deleteModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">\u786E\u8BA4\u5220\u9664</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <p>\u786E\u5B9A\u8981\u5220\u9664\u670D\u52A1\u5668 "<span id="deleteServerName"></span>" \u5417\uFF1F</p>
                    <p class="text-danger">\u6B64\u64CD\u4F5C\u4E0D\u53EF\u9006\uFF0C\u6240\u6709\u76F8\u5173\u7684\u76D1\u63A7\u6570\u636E\u4E5F\u5C06\u88AB\u5220\u9664\u3002</p>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">\u53D6\u6D88</button>
                    <button type="button" class="btn btn-danger" id="confirmDeleteBtn">\u5220\u9664</button>
                </div>
            </div>
        </div>
    </div>

     <!-- \u7F51\u7AD9\u5220\u9664\u786E\u8BA4\u6A21\u6001\u6846 -->
    <div class="modal fade" id="deleteSiteModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">\u786E\u8BA4\u5220\u9664\u7F51\u7AD9\u76D1\u63A7</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <p>\u786E\u5B9A\u8981\u505C\u6B62\u76D1\u63A7\u7F51\u7AD9 "<span id="deleteSiteName"></span>" (<span id="deleteSiteUrl"></span>) \u5417\uFF1F</p>
                    <p class="text-danger">\u6B64\u64CD\u4F5C\u4E0D\u53EF\u9006\u3002</p>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">\u53D6\u6D88</button>
                    <button type="button" class="btn btn-danger" id="confirmDeleteSiteBtn">\u5220\u9664</button>
                </div>
            </div>
        </div>
    </div>

    <!-- \u4FEE\u6539\u5BC6\u7801\u6A21\u6001\u6846 -->
    <div class="modal fade" id="passwordModal" tabindex="-1">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">\u4FEE\u6539\u5BC6\u7801</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">

                    <form id="passwordForm">
                        <div class="mb-3">
                            <label for="currentPassword" class="form-label">\u5F53\u524D\u5BC6\u7801</label>
                            <input type="password" class="form-control" id="currentPassword" required>
                        </div>
                        <div class="mb-3">
                            <label for="newPassword" class="form-label">\u65B0\u5BC6\u7801</label>
                            <input type="password" class="form-control" id="newPassword" required>
                        </div>
                        <div class="mb-3">
                            <label for="confirmPassword" class="form-label">\u786E\u8BA4\u65B0\u5BC6\u7801</label>
                            <input type="password" class="form-control" id="confirmPassword" required>
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">\u53D6\u6D88</button>
                    <button type="button" class="btn btn-primary" id="savePasswordBtn">\u4FDD\u5B58</button>
                </div>
            </div>
        </div>
    </div>

    <footer class="footer app-footer py-4">
        <div class="container text-center">
            <span class="text-muted small">VPS\u76D1\u63A7\u9762\u677F &copy; ${(/* @__PURE__ */ new Date()).getFullYear()}</span>
            <a href="https://github.com/kadidalax/cf-vps-monitor" target="_blank" rel="noopener noreferrer" class="ms-3 text-muted" title="GitHub Repository">
                <i class="bi bi-github"></i>
            </a>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-C6RzsynM9kWDrMNeT87bh95OGNyZPhcTNXj1NW7RuBCsyN/o0jlpcV8Qyq46cDfL" crossorigin="anonymous"><\/script>
    <script src="/js/admin.js"><\/script>
</body>
</html>`;
}
__name(getAdminHtml, "getAdminHtml");
function getFaviconSvg() {
  return `<svg width="32" height="32" viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <radialGradient id="bg" cx="0.3" cy="0.3">
      <stop offset="0%" stop-color="#fff" stop-opacity="0.9"/>
      <stop offset="100%" stop-color="#0277bd" stop-opacity="0.8"/>
    </radialGradient>
    <linearGradient id="ecg" x1="0%" x2="100%">
      <stop offset="0%" stop-color="#f08"/>
      <stop offset="50%" stop-color="#0f8"/>
      <stop offset="100%" stop-color="#80f"/>
    </linearGradient>
  </defs>
  <circle cx="16" cy="16" r="15" fill="url(#bg)" stroke="#0277bd" stroke-width="1.5"/>
  <circle cx="16" cy="16" r="13" fill="none" stroke="#fff" stroke-width="1" opacity="0.4"/>
  <line x1="4" y1="16" x2="28" y2="16" stroke="#b3e5fc" stroke-width="0.5" opacity="0.8"/>
  <path id="p" d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="url(#ecg)" stroke-width="2.8"/>
  <path d="M4 16L8 16L9 15L10 17L11 14L12 18L13 10L14 22L15 16L28 16" fill="none" stroke="#fff" stroke-width="1.2" opacity="0.7"/>
  <circle r="1.5" fill="#fff">
    <animateMotion dur="2s" repeatCount="indefinite">
      <mpath href="#p"/>
    </animateMotion>
  </circle>
  <circle cx="16" cy="16" r="8" fill="none" stroke="#f08" stroke-width="0.5" opacity="0.6">
    <animate attributeName="r" values="8;12;8" dur="3s" repeatCount="indefinite"/>
    <animate attributeName="opacity" values="0.6;0;0.6" dur="3s" repeatCount="indefinite"/>
  </circle>
</svg>`;
}
__name(getFaviconSvg, "getFaviconSvg");
function getStyleCss() {
  return `/* \u5168\u5C40\u6837\u5F0F */
body {
    min-height: 100vh;
    display: flex;
    flex-direction: column;
}

.footer {
    margin-top: auto;
}

/* \u56FE\u8868\u5BB9\u5668 */
.chart-container {
    position: relative;
    height: 200px;
    width: 100%;
}

/* \u5361\u7247\u6837\u5F0F */
.card {
    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
    margin-bottom: 1.5rem;
}

.card-header {
    background-color: rgba(0, 0, 0, 0.03);
    border-bottom: 1px solid rgba(0, 0, 0, 0.125);
}

/* \u8FDB\u5EA6\u6761\u6837\u5F0F */
.progress {
    height: 0.75rem;
}

/* \u8868\u683C\u6837\u5F0F */
.table th {
    font-weight: 600;
}

/* Modal centering and light theme transparency */
.modal-dialog {
    display: flex;
    align-items: center;
    min-height: calc(100% - 1rem); /* Adjust as needed */
}

.modal-content {
    background-color: rgba(255, 255, 255, 0.9); /* Semi-transparent white for light theme */
    /* backdrop-filter: blur(5px); /* Optional: adds a blur effect to content behind modal */
}


/* \u54CD\u5E94\u5F0F\u8C03\u6574 */
@media (max-width: 768px) {
    .chart-container {
        height: 150px;
    }

    /* \u79FB\u52A8\u7AEF\u9690\u85CF\u8868\u683C\uFF0C\u663E\u793A\u5361\u7247 */
    .table-responsive {
        display: none !important;
    }

    .mobile-card-container {
        display: block !important;
    }

    /* \u79FB\u52A8\u7AEF\u9690\u85CF\u684C\u9762\u7AEF\u6309\u94AE */
    .desktop-only {
        display: none !important;
    }

    /* \u79FB\u52A8\u7AEF\u5BFC\u822A\u680F\u4F18\u5316 */
    .navbar-brand {
        font-size: 1rem;
        margin-right: 0.5rem;
    }

    .container {
        padding-left: 10px;
        padding-right: 10px;
    }

    /* \u79FB\u52A8\u7AEF\u5BFC\u822A\u680F\u6309\u94AE\u7EC4\u4F18\u5316 */
    .navbar .d-flex {
        gap: 0.25rem;
        flex-wrap: wrap;
    }

    .navbar .btn-sm {
        padding: 0.25rem 0.5rem;
        font-size: 0.75rem;
        min-width: auto;
        border-width: 1px;
    }

    .navbar .nav-link {
        font-size: 0.8rem;
        padding: 0.25rem 0.5rem;
        margin: 0;
    }

    /* \u79FB\u52A8\u7AEF\u5BFC\u822A\u680F\u4E0B\u62C9\u83DC\u5355\u4F18\u5316 - \u7CBE\u7B80\u7248 */
    .dropdown-menu {
        font-size: 0.875rem;
        min-width: 150px;
        z-index: 10000 !important; /* \u7EDF\u4E00\u4F7F\u7528\u6700\u9AD8\u5C42\u7EA7 */
        position: absolute !important; /* \u4F7F\u7528absolute\u5B9A\u4F4D\u786E\u4FDD\u6B63\u786E\u663E\u793A */
        /* \u79FB\u9664position: fixed\uFF0C\u8BA9Bootstrap\u81EA\u52A8\u5904\u7406\u5B9A\u4F4D */
    }

    /* \u786E\u4FDD\u5BFC\u822A\u680F\u6709\u5408\u9002\u7684\u5C42\u7EA7\u4F46\u4E0D\u521B\u5EFA\u5C42\u53E0\u4E0A\u4E0B\u6587 */
    .navbar {
        position: relative;
        z-index: 1000; /* \u7ED9\u5BFC\u822A\u680F\u4E00\u4E2A\u4E2D\u7B49\u5C42\u7EA7 */
    }

    .navbar .dropdown-item {
        padding: 0.5rem 1rem;
        font-size: 0.875rem;
    }

    .navbar .dropdown-item i {
        width: 1.2rem;
    }

    /* \u79FB\u52A8\u7AEF\u7BA1\u7406\u533A\u57DF\u6807\u9898\u884C\u4F18\u5316 */
    .admin-header-row {
        display: flex;
        flex-direction: column;
        gap: 0.75rem; /* \u51CF\u5C11\u79FB\u52A8\u7AEF\u95F4\u9694 */
    }

    .admin-header-title h2 {
        font-size: 1.5rem;
        margin-bottom: 0;
    }

    .admin-header-content {
        display: flex;
        flex-direction: column;
        gap: 0.5rem; /* \u51CF\u5C11\u79FB\u52A8\u7AEF\u95F4\u9694 */
    }

    .admin-settings-form {
        order: 2; /* \u8BBE\u7F6E\u8868\u5355\u5728\u79FB\u52A8\u7AEF\u663E\u793A\u5728\u6309\u94AE\u7EC4\u4E0B\u65B9 */
    }

    .admin-actions-group {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        order: 1; /* \u6309\u94AE\u7EC4\u5728\u79FB\u52A8\u7AEF\u663E\u793A\u5728\u4E0A\u65B9 */
    }

    .settings-group {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }

    .settings-group .form-label {
        font-size: 0.875rem;
        margin-bottom: 0;
        font-weight: 500;
    }

    .settings-group .input-group {
        max-width: 250px;
    }

    /* \u8D85\u5C0F\u5C4F\u5E55\u4F18\u5316 (\u5C0F\u4E8E400px) */
    @media (max-width: 400px) {
        .navbar-brand {
            font-size: 0.9rem;
        }

        .navbar .btn-sm {
            padding: 0.2rem 0.4rem;
            font-size: 0.7rem;
        }

        .navbar .nav-link {
            font-size: 0.75rem;
            padding: 0.2rem 0.4rem;
        }

        .container {
            padding-left: 8px;
            padding-right: 8px;
        }
    }

    /* \u79FB\u52A8\u7AEF\u6309\u94AE\u4F18\u5316 */
    .btn-sm {
        padding: 0.375rem 0.75rem;
        font-size: 0.875rem;
    }
}

/* \u684C\u9762\u7AEF\u9690\u85CF\u5361\u7247\u5BB9\u5668\u548C\u79FB\u52A8\u7AEF\u83DC\u5355 */
@media (min-width: 769px) {
    .mobile-card-container {
        display: none !important;
    }

    .mobile-only {
        display: none !important;
    }
}

    /* \u684C\u9762\u7AEF\u7BA1\u7406\u533A\u57DF\u6807\u9898\u884C\u6837\u5F0F */
    .admin-header-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        flex-wrap: wrap;
        gap: 0.75rem; /* \u51CF\u5C11\u684C\u9762\u7AEF\u95F4\u9694 */
    }

    .admin-header-title {
        flex: 0 0 auto;
    }

    .admin-header-content {
        display: flex;
        align-items: center;
        gap: 1rem;
        flex: 1 1 auto;
        justify-content: flex-end;
    }

    .admin-settings-form {
        order: 1;
        margin-right: auto; /* \u63A8\u9001\u5230\u5DE6\u4FA7 */
    }

    .admin-actions-group {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        order: 2;
    }

    .settings-group {
        display: flex;
        flex-direction: row;
        align-items: center;
        gap: 0.5rem;
    }

    .settings-group .form-label {
        margin-bottom: 0;
        white-space: nowrap;
        font-size: 0.875rem;
    }
}

/* \u5355\u4E00\u5361\u7247\u5E03\u5C40\u6837\u5F0F */
.card.shadow-sm {
    border: none;
    box-shadow: 0 0.125rem 0.5rem rgba(0, 0, 0, 0.1) !important;
}

.card-title {
    color: var(--bs-primary);
    font-weight: 600;
}

.card-title i {
    color: var(--bs-primary);
}

/* \u5206\u9694\u7EBF\u6837\u5F0F */
hr.my-4 {
    border-color: var(--bs-border-color-translucent);
    opacity: 0.5;
}

/* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u5355\u4E00\u5361\u7247\u6837\u5F0F */
[data-bs-theme="dark"] .card.shadow-sm {
    background-color: var(--bs-dark);
    box-shadow: 0 0.125rem 0.5rem rgba(0, 0, 0, 0.3) !important;
}

[data-bs-theme="dark"] .card-title {
    color: #86b7fe;
}

[data-bs-theme="dark"] .card-title i {
    color: #86b7fe;
}

/* VPS\u76D1\u63A7\u9762\u677F\u6807\u9898 - \u84DD\u8272\u52A0\u7C97 */
.navbar-brand {
    color: var(--bs-primary) !important;
    font-weight: 600 !important;
}
[data-bs-theme="dark"] .navbar-brand {
    color: #86b7fe !important;
}

/* \u5BFC\u822A\u680F\u4E3B\u9898\u8DDF\u968F - \u7CBE\u7B80\u7248 */
[data-bs-theme="light"] .navbar { background-color: #f8f9fa !important; }
[data-bs-theme="dark"] .navbar { background-color: #343a40 !important; }

/* \u5BFC\u822A\u680F\u6587\u5B57\u4E3B\u9898\u8DDF\u968F */
[data-bs-theme="light"] .navbar .nav-link, [data-bs-theme="light"] .navbar a { color: #212529 !important; }
[data-bs-theme="dark"] .navbar .nav-link, [data-bs-theme="dark"] .navbar a { color: #ffffff !important; }

/* \u5BFC\u822A\u680F\u6309\u94AE\u4E3B\u9898\u8DDF\u968F */
[data-bs-theme="light"] .navbar .btn-outline-light { border-color: #212529 !important; color: #212529 !important; }
[data-bs-theme="dark"] .navbar .btn-outline-light { border-color: #ffffff !important; color: #ffffff !important; }

/* \u5BFC\u822A\u680F\u56FE\u6807\u4E3B\u9898\u8DDF\u968F */
[data-bs-theme="light"] .navbar i { color: #212529 !important; }
[data-bs-theme="dark"] .navbar i { color: #ffffff !important; }

/* \u5E95\u90E8\u7248\u6743\u4FE1\u606F - \u4E3B\u9898\u8DDF\u968F\u8C03\u5927 */
.footer .text-muted { font-size: 0.95rem !important; font-weight: 500; }
.footer a.text-muted { font-size: 1.1rem !important; }
.footer .text-muted { color: #6c757d !important; }
[data-bs-theme="dark"] .footer .text-muted { color: #adb5bd !important; }

[data-bs-theme="dark"] hr.my-4 {
    border-color: rgba(255, 255, 255, 0.2);
}

/* \u56FA\u5B9A\u5E95\u90E8\u9875\u811A\u6837\u5F0F */
body {
    padding-bottom: 60px; /* \u4E3A\u56FA\u5B9A\u9875\u811A\u7559\u51FA\u7A7A\u95F4 */
}

.footer.fixed-bottom {
    height: 35px;
    background-color: var(--bs-light) !important;
    border-top: 1px solid var(--bs-border-color);
    display: flex;
    align-items: center;
}

/* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u9875\u811A */
[data-bs-theme="dark"] .footer.fixed-bottom {
    background-color: var(--bs-dark) !important;
    border-top-color: var(--bs-border-color);
}

/* \u79FB\u52A8\u7AEF\u5361\u7247\u6837\u5F0F */
.mobile-card-container {
    display: none; /* \u9ED8\u8BA4\u9690\u85CF\uFF0C\u901A\u8FC7\u5A92\u4F53\u67E5\u8BE2\u63A7\u5236 */
    position: relative;
    z-index: 0; /* \u964D\u4F4E\u5BB9\u5668\u5C42\u7EA7\uFF0C\u786E\u4FDD\u4E0B\u62C9\u83DC\u5355\u5728\u4E0A\u65B9 */
}

.mobile-server-card, .mobile-site-card {
    background: var(--bs-card-bg, #fff);
    border: 1px solid var(--bs-border-color, rgba(0,0,0,.125));
    border-radius: 0.5rem;
    margin-bottom: 0.75rem;
    box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
    overflow: hidden;
    transition: box-shadow 0.15s ease-in-out, transform 0.15s ease-in-out;
    position: relative;
    z-index: 0; /* \u964D\u4F4E\u5361\u7247\u5C42\u7EA7\uFF0C\u786E\u4FDD\u4E0B\u62C9\u83DC\u5355\u5728\u4E0A\u65B9 */
}

@media (max-width: 768px) {
    .mobile-server-card:hover, .mobile-site-card:hover {
        box-shadow: 0 0.25rem 0.5rem rgba(0, 0, 0, 0.1);
    }
}

.mobile-card-header {
    padding: 0.75rem;
    background-color: var(--bs-card-cap-bg, rgba(0,0,0,.03));
    border-bottom: 1px solid var(--bs-border-color, rgba(0,0,0,.125));
    display: flex;
    justify-content: space-between;
    align-items: center;
    position: relative;
    z-index: 0; /* \u964D\u4F4E\u5361\u7247\u5934\u90E8\u5C42\u7EA7\uFF0C\u786E\u4FDD\u4E0B\u62C9\u83DC\u5355\u5728\u4E0A\u65B9 */
}

.mobile-card-header-left {
    flex: 0 0 auto;
}

.mobile-card-header-right {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    font-size: 0.875rem;
}

.mobile-card-footer {
    margin-top: 0.5rem;
    padding-top: 0.5rem;
    border-top: 1px solid var(--bs-border-color, rgba(0,0,0,.125));
    font-size: 0.875rem;
    color: var(--bs-secondary);
}

@media (max-width: 768px) {
    .mobile-card-header:hover {
        background-color: var(--bs-card-cap-bg, rgba(0,0,0,.05));
    }
}

.mobile-card-title {
    font-weight: 600;
    margin: 0;
    font-size: 1rem;
    line-height: 1.3;
}

.mobile-card-status {
    flex-shrink: 0;
}

.mobile-card-body {
    padding: 0.75rem;
}

.mobile-card-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem 0;
    border-bottom: 1px solid var(--bs-border-color-translucent, rgba(0,0,0,.08));
}

.mobile-card-row:last-child {
    border-bottom: none;
    padding-bottom: 0;
}

/* \u4E24\u5217\u5E03\u5C40\u6837\u5F0F */
.mobile-card-two-columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.75rem;
    padding: 0.5rem 0;
    border-bottom: 1px solid var(--bs-border-color-translucent, rgba(0,0,0,.08));
}

.mobile-card-two-columns:last-child {
    border-bottom: none;
    padding-bottom: 0.25rem;
}

.mobile-card-column-item {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    min-height: 2rem;
    justify-content: center;
}

.mobile-card-column-item .mobile-card-label {
    font-size: 0.7rem;
    margin-bottom: 0;
    color: var(--bs-secondary-color, #6c757d);
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.02em;
}

.mobile-card-column-item .mobile-card-value {
    font-size: 0.85rem;
    font-weight: 600;
    text-align: left;
    max-width: 100%;
    word-break: break-word;
    line-height: 1.2;
}

/* \u79FB\u52A8\u7AEF\u5355\u884C\u6837\u5F0F\u4F18\u5316 */
@media (max-width: 768px) {
    .mobile-card-row {
        padding: 0.5rem 0;
        min-height: 2rem;
        align-items: center;
    }

    .mobile-card-label {
        font-weight: 500;
        font-size: 0.875rem;
    }

    .mobile-card-value {
        font-weight: 600;
        font-size: 0.875rem;
        word-break: break-word;
    }
}

.mobile-card-label {
    font-weight: 500;
    color: var(--bs-secondary-color, #6c757d);
    font-size: 0.875rem;
}

.mobile-card-value {
    text-align: right;
    flex-shrink: 0;
    max-width: 60%;
}



/* \u79FB\u52A8\u7AEF\u8FDB\u5EA6\u6761\u4F18\u5316 */
@media (max-width: 768px) {
    .progress {
        height: 1rem;
        margin-top: 0.25rem;
        border-radius: 0.5rem;
    }

    .progress span {
        font-size: 0.75rem;
        line-height: 1rem;
    }
}

/* \u79FB\u52A8\u7AEF\u72B6\u6001\u5FBD\u7AE0\u4F18\u5316 */
@media (max-width: 768px) {
    .badge {
        font-size: 0.75rem;
        padding: 0.35em 0.65em;
        border-radius: 0.375rem;
    }
}

/* \u79FB\u52A8\u7AEF\u5386\u53F2\u8BB0\u5F55\u6761\u4F18\u5316 */
@media (max-width: 768px) {
    .mobile-history-container .history-bar-container {
        height: 1.5rem;
        border-radius: 0.25rem;
        overflow: hidden;
        display: flex;
        width: 100%;
        gap: 1px;
    }

    .mobile-history-container .history-bar {
        flex: 1;
        min-width: 0;
        border-radius: 1px;
        height: 100%;
    }
}

/* \u79FB\u52A8\u7AEF\u5386\u53F2\u8BB0\u5F55\u6761\u4F18\u5316 */
.mobile-history-container {
    margin-top: 0.5rem;
}

.mobile-history-label {
    font-size: 0.75rem;
    color: var(--bs-secondary-color, #6c757d);
    margin-bottom: 0.25rem;
}



/* \u79FB\u52A8\u7AEF\u6309\u94AE\u4F18\u5316 */
@media (max-width: 768px) {
    .mobile-card-body .btn-sm {
        padding: 0.5rem 0.75rem;
        font-size: 0.8rem;
        border-radius: 0.375rem;
        transition: all 0.15s ease-in-out;
    }

    .mobile-card-body .d-flex.gap-2 {
        gap: 0.5rem !important;
    }

    .mobile-card-body .btn i {
        font-size: 0.875rem;
    }

    /* \u79FB\u52A8\u7AEF\u89E6\u6478\u53CD\u9988 */
    .mobile-card-header:active {
        background-color: var(--bs-card-cap-bg, rgba(0,0,0,.08)) !important;
    }

    .mobile-card-body .btn:active {
        opacity: 0.8;
    }

    /* \u79FB\u52A8\u7AEF\u5BB9\u5668\u6807\u9898\u4F18\u5316 */
    .container h2 {
        font-size: 1.5rem;
        margin-bottom: 1rem;
    }

    /* \u79FB\u52A8\u7AEF\u5361\u7247\u6807\u9898\u5C42\u6B21\u4F18\u5316 */
    .mobile-card-title {
        font-size: 1rem;
        line-height: 1.3;
        font-weight: 600;
    }

    /* \u79FB\u52A8\u7AEF\u7BA1\u7406\u9875\u9762\u6309\u94AE\u4F18\u5316 */
    .admin-actions-group .btn {
        font-size: 0.875rem;
        padding: 0.5rem 0.75rem;
        border-radius: 0.375rem;
        transition: all 0.2s ease-in-out;
    }

    .admin-actions-group .btn:active {
        transform: scale(0.95);
    }

    .admin-actions-group .dropdown-toggle {
        min-width: auto;
    }



    /* \u79FB\u52A8\u7AEF\u5361\u7247\u95F4\u8DDD\u4F18\u5316 */
    .mobile-server-card, .mobile-site-card {
        margin-bottom: 1rem;
    }

    .mobile-card-body {
        padding: 0.75rem;
    }

    .mobile-card-row {
        padding: 0.375rem 0;
        border-bottom: 1px solid var(--bs-border-color-translucent, rgba(0,0,0,.08));
    }

    .mobile-card-row:last-child {
        border-bottom: none;
    }
}

/* \u81EA\u5B9A\u4E49\u6D45\u7EFF\u8272\u8FDB\u5EA6\u6761 */
.bg-light-green {
    background-color: #90ee90 !important; /* LightGreen */
}

/* Custom styles for non-disruptive alerts in admin page */
#serverAlert, #siteAlert, #telegramSettingsAlert {
    position: fixed !important; /* Use !important to override Bootstrap if necessary */
    top: 70px; /* Below navbar */
    left: 50%;
    transform: translateX(-50%);
    z-index: 1055; /* Higher than Bootstrap modals (1050) */
    padding: 0.75rem 1.25rem;
    /* margin-bottom: 1rem; /* Not needed for fixed */
    border: 1px solid transparent;
    border-radius: 0.25rem;
    min-width: 300px; /* Minimum width */
    max-width: 90%; /* Max width */
    text-align: center;
    box-shadow: 0 0.5rem 1rem rgba(0,0,0,0.15);
    /* Ensure d-none works to hide them, !important might be needed if Bootstrap's .alert.d-none is too specific */
}

#serverAlert.d-none, #siteAlert.d-none, #telegramSettingsAlert.d-none {
    display: none !important;
}

/* Semi-transparent backgrounds for different alert types */
/* Light Theme Overrides for fixed alerts */
#serverAlert.alert-success, #siteAlert.alert-success, #telegramSettingsAlert.alert-success {
    color: #0f5132; /* Bootstrap success text color */
    background-color: rgba(209, 231, 221, 0.95) !important; /* Semi-transparent success, !important for specificity */
    border-color: rgba(190, 221, 208, 0.95) !important;
}

#serverAlert.alert-danger, #siteAlert.alert-danger, #telegramSettingsAlert.alert-danger {
    color: #842029; /* Bootstrap danger text color */
    background-color: rgba(248, 215, 218, 0.95) !important; /* Semi-transparent danger */
    border-color: rgba(245, 198, 203, 0.95) !important;
}

#serverAlert.alert-warning, #siteAlert.alert-warning, #telegramSettingsAlert.alert-warning { /* For siteAlert if it uses warning */
    color: #664d03; /* Bootstrap warning text color */
    background-color: rgba(255, 243, 205, 0.95) !important; /* Semi-transparent warning */
    border-color: rgba(255, 238, 186, 0.95) !important;
}


    [data-bs-theme="dark"] {
        body {
            background-color: #212529; /* Bootstrap dark bg */
            color: #f8f9fa; /* Light text for dark mode */
        }

        .card {
            background-color: #343a40; /* Card dark background */
            border: 1px solid #495057;
            color: #f8f9fa; /* Card text color */
        }

        .card-header {
            background-color: #495057;
            border-bottom: 1px solid #5b6167;
            color: #ffffff;
        }

        .table {
            color: #ffffff; /* Table text color */
        }

        .table th, .table td {
            border-color: #495057; /* Table border color */
        }

        .table-striped > tbody > tr:nth-of-type(odd) > * {
             background-color: rgba(255, 255, 255, 0.05); /* Dark mode stripe */
             color: #ffffff;
        }

        .table-hover > tbody > tr:hover > * {
            background-color: rgba(255, 255, 255, 0.075); /* Dark mode hover */
            color: #ffffff;
        }

        .modal-content {
            background-color: #343a40; /* Dark grey for dark theme */
            color: #ffffff;
        }

        .modal-header {
            border-bottom-color: #495057;
        }

        .modal-footer {
            border-top-color: #495057;
        }

        .form-control {
            background-color: #495057;
            color: #ffffff;
            border-color: #5b6167;
        }

        .form-control:focus {
            background-color: #495057;
            color: #ffffff;
            border-color: #86b7fe; /* Bootstrap focus color */
            box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.25);
        }

        .btn-outline-secondary {
             color: #adb5bd;
             border-color: #6c757d;
        }
        .btn-outline-secondary:hover {
             color: #fff;
             background-color: #6c757d;
             border-color: #6c757d;
        }

        .navbar {
            background-color: #212529 !important; /* Ensure override Bootstrap default */
        }

        /* \u6697\u8272\u4E3B\u9898\u79FB\u52A8\u7AEF\u5361\u7247\u6837\u5F0F */
        .mobile-server-card, .mobile-site-card {
            background: var(--bs-dark, #212529);
            border-color: var(--bs-border-color, #495057);
        }

        .mobile-card-header {
            background-color: rgba(255, 255, 255, 0.05);
            border-bottom-color: var(--bs-border-color, #495057);
        }

        .mobile-card-title {
            color: #ffffff !important;
        }

        .mobile-card-label {
            color: #ced4da !important;
        }

        .mobile-card-value {
            color: #ffffff !important;
        }



        .mobile-card-row {
            border-bottom-color: rgba(255, 255, 255, 0.08);
        }

        .mobile-card-two-columns {
            border-bottom-color: rgba(255, 255, 255, 0.08);
        }

        .mobile-card-column-item .mobile-card-label {
            color: #ced4da !important;
        }

        .mobile-card-column-item .mobile-card-value {
            color: #ffffff !important;
        }

        .mobile-history-label {
            color: #ced4da !important;
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u7A7A\u72B6\u6001\u548C\u9519\u8BEF\u72B6\u6001\u6587\u5B57 */
        .mobile-card-container .text-muted {
            color: #ced4da !important;
        }

        .mobile-card-container .text-danger {
            color: #ff6b6b !important;
        }

        .mobile-card-container h6 {
            color: #ffffff !important;
        }

        .mobile-card-container small {
            color: #adb5bd !important;
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u79FB\u52A8\u7AEF\u6309\u94AE\u4F18\u5316 */
        .mobile-card-body .btn-outline-primary {
            color: #6ea8fe !important;
            border-color: #6ea8fe !important;
        }

        .mobile-card-body .btn-outline-primary:hover {
            color: #000 !important;
            background-color: #6ea8fe !important;
            border-color: #6ea8fe !important;
        }

        .mobile-card-body .btn-outline-info {
            color: #6edff6 !important;
            border-color: #6edff6 !important;
        }

        .mobile-card-body .btn-outline-info:hover {
            color: #000 !important;
            background-color: #6edff6 !important;
            border-color: #6edff6 !important;
        }

        .mobile-card-body .btn-outline-danger {
            color: #ea868f !important;
            border-color: #ea868f !important;
        }

        .mobile-card-body .btn-outline-danger:hover {
            color: #000 !important;
            background-color: #ea868f !important;
            border-color: #ea868f !important;
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684Badge\u5FBD\u7AE0\u4F18\u5316 */
        .mobile-card-header .badge.bg-success {
            background-color: #198754 !important;
            color: #ffffff !important;
        }

        .mobile-card-header .badge.bg-danger {
            background-color: #dc3545 !important;
            color: #ffffff !important;
        }

        .mobile-card-header .badge.bg-warning {
            background-color: #ffc107 !important;
            color: #000000 !important;
        }

        .mobile-card-header .badge.bg-secondary {
            background-color: #6c757d !important;
            color: #ffffff !important;
        }

        .mobile-card-header .badge.bg-primary {
            background-color: #0d6efd !important;
            color: #ffffff !important;
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u79FB\u52A8\u7AEF\u5BB9\u5668\u6807\u9898\u4F18\u5316 */
        .container h2 {
            color: #ffffff !important;
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u79FB\u52A8\u7AEF\u52A0\u8F7D\u72B6\u6001\u4F18\u5316 */
        .mobile-card-container .spinner-border {
            color: #6ea8fe !important;
        }

        .mobile-card-container .mt-2 {
            color: #ced4da !important;
        }

        /* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u5BFC\u822A\u680F\u6309\u94AE\u4F18\u5316 */
        .navbar .btn-outline-light {
            color: #f8f9fa !important;
            border-color: #f8f9fa !important;
        }

        .navbar .btn-outline-light:hover {
            color: #000 !important;
            background-color: #f8f9fa !important;
            border-color: #f8f9fa !important;
        }

        .navbar .nav-link {
            color: #f8f9fa !important;
        }

        .navbar .nav-link:hover {
            color: #e9ecef !important;
        }
        .navbar-light .navbar-nav .nav-link {
             color: #ccc;
        }
        .navbar-light .navbar-nav .nav-link:hover {
             color: #fff;
        }
        .navbar-light .navbar-brand {
             color: #fff;
        }
         .footer {
            background-color: #343a40 !important;
            color: #ced4da; /* Footer text color for dark mode */
        }
        a {
            color: #8ab4f8; /* \u793A\u4F8B\u94FE\u63A5\u989C\u8272 */
        }
        a:hover {
            color: #a9c9fc;
        }

        /* Dark Theme Overrides for fixed alerts */
        [data-bs-theme="dark"] #serverAlert.alert-success,
        [data-bs-theme="dark"] #siteAlert.alert-success,
        [data-bs-theme="dark"] #telegramSettingsAlert.alert-success {
            color: #75b798; /* Lighter green text for dark theme */
            background-color: rgba(40, 167, 69, 0.85) !important; /* Darker semi-transparent success */
            border-color: rgba(34, 139, 57, 0.85) !important;
        }

        [data-bs-theme="dark"] #serverAlert.alert-danger,
        [data-bs-theme="dark"] #siteAlert.alert-danger,
        [data-bs-theme="dark"] #telegramSettingsAlert.alert-danger {
            color: #ea868f; /* Lighter red text for dark theme */
            background-color: rgba(220, 53, 69, 0.85) !important; /* Darker semi-transparent danger */
            border-color: rgba(187, 45, 59, 0.85) !important;
        }

        [data-bs-theme="dark"] #serverAlert.alert-warning,
        [data-bs-theme="dark"] #siteAlert.alert-warning,
        [data-bs-theme="dark"] #telegramSettingsAlert.alert-warning {
            color: #ffd373; /* Lighter yellow text for dark theme */
            background-color: rgba(255, 193, 7, 0.85) !important; /* Darker semi-transparent warning */
            border-color: rgba(217, 164, 6, 0.85) !important;
        }
    }

/* \u62D6\u62FD\u6392\u5E8F\u6837\u5F0F */
.server-row-draggable, .site-row-draggable {
    transition: all 0.2s ease;
}
.server-row-draggable:hover, .site-row-draggable:hover {
    background-color: rgba(0, 123, 255, 0.1) !important;
}
.server-row-draggable.drag-over-top, .site-row-draggable.drag-over-top {
    border-top: 3px solid #007bff !important;
    background-color: rgba(0, 123, 255, 0.1) !important;
}
.server-row-draggable.drag-over-bottom, .site-row-draggable.drag-over-bottom {
    border-bottom: 3px solid #007bff !important;
    background-color: rgba(0, 123, 255, 0.1) !important;
}
.server-row-draggable[draggable="true"], .site-row-draggable[draggable="true"] {
    cursor: grab;
}
.server-row-draggable[draggable="true"]:active, .site-row-draggable[draggable="true"]:active {
    cursor: grabbing;
}

/* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u62D6\u62FD\u6837\u5F0F */
[data-bs-theme="dark"] .server-row-draggable:hover,
[data-bs-theme="dark"] .site-row-draggable:hover {
    background-color: rgba(13, 110, 253, 0.2) !important;
}
[data-bs-theme="dark"] .server-row-draggable.drag-over-top,
[data-bs-theme="dark"] .site-row-draggable.drag-over-top {
    border-top: 3px solid #0d6efd !important;
    background-color: rgba(13, 110, 253, 0.2) !important;
}
[data-bs-theme="dark"] .server-row-draggable.drag-over-bottom,
[data-bs-theme="dark"] .site-row-draggable.drag-over-bottom {
    border-bottom: 3px solid #0d6efd !important;
    background-color: rgba(13, 110, 253, 0.2) !important;
}

/* ==================== \u81EA\u5B9A\u4E49\u80CC\u666F\u548C\u900F\u660E\u5EA6\u63A7\u5236\u7CFB\u7EDF ==================== */

/* CSS\u53D8\u91CF\u5B9A\u4E49 */
:root {
    --custom-background-url: '';
    --page-opacity: 0.8;
    --text-contrast-light: rgba(0, 0, 0, 0.87);
    --text-contrast-dark: rgba(255, 255, 255, 0.87);
    --background-overlay-light: rgba(255, 255, 255, 0.9);
    --background-overlay-dark: rgba(18, 18, 18, 0.9);
}

/* \u80CC\u666F\u56FE\u7247\u663E\u793A */
body.custom-background-enabled::before {
    content: '';
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background-image: var(--custom-background-url);
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
    background-attachment: fixed;
    z-index: -1;
    opacity: 1;
}

/* \u542F\u7528\u81EA\u5B9A\u4E49\u80CC\u666F\u65F6\u7684\u9875\u9762\u5143\u7D20\u900F\u660E\u5EA6\u8C03\u6574 */
body.custom-background-enabled .navbar {
    background-color: rgba(248, 249, 250, var(--page-opacity)) !important;
    backdrop-filter: saturate(180%) blur(20px);
    -webkit-backdrop-filter: saturate(180%) blur(20px);
}

body.custom-background-enabled .card {
    background-color: rgba(255, 255, 255, var(--page-opacity)) !important;
    backdrop-filter: saturate(180%) blur(28px);
    -webkit-backdrop-filter: saturate(180%) blur(28px);
    border: 1px solid rgba(0, 0, 0, 0.125);
}

body.custom-background-enabled .card-header {
    background-color: rgba(0, 0, 0, calc(0.03 * var(--page-opacity))) !important;
    border-bottom: 1px solid rgba(0, 0, 0, calc(0.125 * var(--page-opacity)));
}

body.custom-background-enabled .modal-content {
    background-color: rgba(255, 255, 255, var(--page-opacity)) !important;
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
}

body.custom-background-enabled .footer {
    background-color: rgba(248, 249, 250, var(--page-opacity)) !important;
    backdrop-filter: blur(5px);
    -webkit-backdrop-filter: blur(5px);
}

/* \u8868\u683C\u900F\u660E\u5EA6\u8C03\u6574 - \u907F\u514D\u4E0E\u5361\u7247\u80CC\u666F\u53E0\u52A0 */
body.custom-background-enabled .table {
    background-color: transparent !important;
}

body.custom-background-enabled .table th {
    background-color: transparent !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
}

body.custom-background-enabled .table td {
    background-color: transparent !important;
}

/* \u8F93\u5165\u6846\u5B8C\u5168\u900F\u660E\u5316 - \u65B9\u6848A */
body.custom-background-enabled .form-control {
    background-color: transparent !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
    border: 1px solid rgba(0, 0, 0, 0.15) !important;
}

body.custom-background-enabled .form-control:focus {
    background-color: transparent !important;
    border: 1px solid rgba(13, 110, 253, 0.6) !important;
    box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.15) !important;
}

/* \u6309\u94AE\u900F\u660E\u5EA6\u8C03\u6574 */
body.custom-background-enabled .btn {
    backdrop-filter: blur(3px);
    -webkit-backdrop-filter: blur(3px);
}

/* \u6ED1\u5757\u5B8C\u5168\u900F\u660E\u5316 - \u5B8C\u6574\u91CD\u7F6E */
body.custom-background-enabled .form-range {
    -webkit-appearance: none !important;
    appearance: none !important;
    background: transparent !important;
    outline: none !important;
}

/* WebKit\u6D4F\u89C8\u5668 (Chrome, Safari) */
body.custom-background-enabled .form-range::-webkit-slider-track {
    -webkit-appearance: none !important;
    appearance: none !important;
    background: transparent !important;
    border: 1px solid rgba(0, 0, 0, 0.15) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
    outline: none !important;
    margin: 0 !important;
    padding: 0 !important;
    box-sizing: border-box !important;
}

body.custom-background-enabled .form-range::-webkit-slider-runnable-track {
    -webkit-appearance: none !important;
    background: transparent !important;
    border: 1px solid rgba(0, 0, 0, 0.15) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
}

/* Firefox */
body.custom-background-enabled .form-range::-moz-range-track {
    background: transparent !important;
    border: 1px solid rgba(0, 0, 0, 0.15) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
    outline: none !important;
}

body.custom-background-enabled .form-range::-moz-range-progress {
    background: transparent !important;
    height: 6px !important;
    border-radius: 3px !important;
}

/* \u6ED1\u5757\u6309\u94AE - \u5782\u76F4\u5C45\u4E2D\u5BF9\u9F50 */
body.custom-background-enabled .form-range::-webkit-slider-thumb {
    -webkit-appearance: none !important;
    appearance: none !important;
    background-color: rgba(13, 110, 253, 0.8) !important;
    border: 1px solid rgba(0, 0, 0, 0.1) !important;
    width: 20px !important;
    height: 20px !important;
    border-radius: 50% !important;
    cursor: pointer !important;
    margin-top: -7px !important;
    box-sizing: border-box !important;
}

body.custom-background-enabled .form-range::-moz-range-thumb {
    background-color: rgba(13, 110, 253, 0.8) !important;
    border: 1px solid rgba(0, 0, 0, 0.1) !important;
    width: 20px !important;
    height: 20px !important;
    border-radius: 50% !important;
    cursor: pointer !important;
    box-shadow: none !important;
    margin-top: -8px !important;
    box-sizing: border-box !important;
}

/* \u4E0B\u62C9\u83DC\u5355\u900F\u660E\u5EA6\u8C03\u6574 - \u786E\u4FDD\u6700\u9AD8\u5C42\u7EA7\u663E\u793A */
body.custom-background-enabled .dropdown-menu {
    background-color: rgba(255, 255, 255, var(--page-opacity)) !important;
    /* \u79FB\u9664backdrop-filter\u907F\u514D\u521B\u5EFA\u5C42\u53E0\u4E0A\u4E0B\u6587\uFF0C\u786E\u4FDDz-index\u6B63\u5E38\u5DE5\u4F5C */
    /* backdrop-filter: blur(5px); */
    /* -webkit-backdrop-filter: blur(5px); */
}

/* \u79FB\u52A8\u7AEF\u5361\u7247\u900F\u660E\u5EA6\u8C03\u6574 */
body.custom-background-enabled .mobile-server-card,
body.custom-background-enabled .mobile-site-card {
    background-color: rgba(255, 255, 255, var(--page-opacity)) !important;
    backdrop-filter: saturate(180%) blur(28px);
    -webkit-backdrop-filter: saturate(180%) blur(28px);
}

body.custom-background-enabled .mobile-card-header {
    background-color: rgba(0, 0, 0, calc(0.03 * var(--page-opacity))) !important;
}

/* \u8868\u683C\u6761\u7EB9\u548C\u60AC\u505C\u6548\u679C - \u8F7B\u5FAE\u80CC\u666F\u8272\uFF0C\u4E0D\u53E0\u52A0\u900F\u660E\u5EA6 */
body.custom-background-enabled .table-striped > tbody > tr:nth-of-type(odd) > * {
    background-color: rgba(0, 0, 0, 0.02) !important;
}

body.custom-background-enabled .table-hover > tbody > tr:hover > * {
    background-color: rgba(0, 0, 0, 0.04) !important;
}

/* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u81EA\u5B9A\u4E49\u80CC\u666F\u6837\u5F0F */
[data-bs-theme="dark"] body.custom-background-enabled .navbar {
    background-color: rgba(30, 35, 45, var(--page-opacity)) !important;
    backdrop-filter: saturate(180%) blur(20px);
    -webkit-backdrop-filter: saturate(180%) blur(20px);
}

[data-bs-theme="dark"] body.custom-background-enabled .card {
    background-color: rgba(30, 35, 45, var(--page-opacity)) !important;
    backdrop-filter: saturate(180%) blur(28px);
    -webkit-backdrop-filter: saturate(180%) blur(28px);
    border-color: rgba(255, 255, 255, 0.12);
}

[data-bs-theme="dark"] body.custom-background-enabled .card-header {
    background-color: rgba(35, 42, 55, calc(0.7 * var(--page-opacity))) !important;
    border-bottom-color: rgba(255, 255, 255, 0.12);
}

[data-bs-theme="dark"] body.custom-background-enabled .modal-content {
    background-color: rgba(30, 35, 45, var(--page-opacity)) !important;
    backdrop-filter: saturate(180%) blur(28px);
    -webkit-backdrop-filter: saturate(180%) blur(28px);
}

[data-bs-theme="dark"] body.custom-background-enabled .footer {
    background-color: rgba(30, 35, 45, var(--page-opacity)) !important;
    backdrop-filter: saturate(180%) blur(20px);
    -webkit-backdrop-filter: saturate(180%) blur(20px);
}

/* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u8868\u683C\u900F\u660E\u5EA6\u8C03\u6574 - \u907F\u514D\u4E0E\u5361\u7247\u80CC\u666F\u53E0\u52A0 */
[data-bs-theme="dark"] body.custom-background-enabled .table {
    background-color: transparent !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .table th {
    background-color: transparent !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
}

[data-bs-theme="dark"] body.custom-background-enabled .table td {
    background-color: transparent !important;
}

/* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u8F93\u5165\u6846\u5B8C\u5168\u900F\u660E\u5316 - \u65B9\u6848A */
[data-bs-theme="dark"] body.custom-background-enabled .form-control {
    background-color: transparent !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    color: rgba(255, 255, 255, 0.9) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-control:focus {
    background-color: transparent !important;
    border: 1px solid rgba(13, 110, 253, 0.6) !important;
    box-shadow: 0 0 0 0.25rem rgba(13, 110, 253, 0.15) !important;
}

/* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u4E0B\u62C9\u83DC\u5355\u900F\u660E\u5EA6\u8C03\u6574 */
[data-bs-theme="dark"] body.custom-background-enabled .dropdown-menu {
    background-color: rgba(30, 35, 45, var(--page-opacity)) !important;
    backdrop-filter: saturate(180%) blur(20px);
    -webkit-backdrop-filter: saturate(180%) blur(20px);
}

/* \u6697\u8272\u4E3B\u9898\u4E0B\u7684\u6ED1\u5757\u5B8C\u5168\u900F\u660E\u5316 - \u5B8C\u6574\u91CD\u7F6E */
[data-bs-theme="dark"] body.custom-background-enabled .form-range {
    -webkit-appearance: none !important;
    appearance: none !important;
    background: transparent !important;
    outline: none !important;
}

/* WebKit\u6D4F\u89C8\u5668 (Chrome, Safari) - \u6697\u8272\u4E3B\u9898 */
[data-bs-theme="dark"] body.custom-background-enabled .form-range::-webkit-slider-track {
    -webkit-appearance: none !important;
    appearance: none !important;
    background: transparent !important;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
    outline: none !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-range::-webkit-slider-runnable-track {
    -webkit-appearance: none !important;
    background: transparent !important;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
}

/* Firefox - \u6697\u8272\u4E3B\u9898 */
[data-bs-theme="dark"] body.custom-background-enabled .form-range::-moz-range-track {
    background: transparent !important;
    border: 1px solid rgba(255, 255, 255, 0.2) !important;
    height: 6px !important;
    border-radius: 3px !important;
    box-shadow: none !important;
    outline: none !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-range::-moz-range-progress {
    background: transparent !important;
    height: 6px !important;
    border-radius: 3px !important;
}

/* \u6ED1\u5757\u6309\u94AE - \u6697\u8272\u4E3B\u9898 - \u5782\u76F4\u5C45\u4E2D\u5BF9\u9F50 */
[data-bs-theme="dark"] body.custom-background-enabled .form-range::-webkit-slider-thumb {
    -webkit-appearance: none !important;
    appearance: none !important;
    background-color: rgba(13, 110, 253, 0.9) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    width: 20px !important;
    height: 20px !important;
    border-radius: 50% !important;
    cursor: pointer !important;
    margin-top: -7px !important;
    box-sizing: border-box !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-range::-moz-range-thumb {
    background-color: rgba(13, 110, 253, 0.9) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    width: 20px !important;
    height: 20px !important;
    border-radius: 50% !important;
    cursor: pointer !important;
    box-shadow: none !important;
    margin-top: -8px !important;
    box-sizing: border-box !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .mobile-server-card,
[data-bs-theme="dark"] body.custom-background-enabled .mobile-site-card {
    background-color: rgba(33, 37, 41, var(--page-opacity)) !important;
    border-color: rgba(73, 80, 87, var(--page-opacity));
}

[data-bs-theme="dark"] body.custom-background-enabled .mobile-card-header {
    background-color: rgba(255, 255, 255, calc(0.05 * var(--page-opacity))) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .table-striped > tbody > tr:nth-of-type(odd) > * {
    background-color: rgba(255, 255, 255, 0.03) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .table-hover > tbody > tr:hover > * {
    background-color: rgba(255, 255, 255, 0.05) !important;
}





/* \u8B66\u544A\u6846\u900F\u660E\u5EA6\u8C03\u6574 */
body.custom-background-enabled #serverAlert,
body.custom-background-enabled #siteAlert,
body.custom-background-enabled #telegramSettingsAlert,
body.custom-background-enabled #backgroundSettingsAlert {
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.3);
}

/* ==================== \u6587\u5B57\u63CF\u8FB9\u6E32\u67D3\u7CFB\u7EDF ==================== */

/* \u6587\u5B57\u52A0\u7C97\u7CFB\u7EDF - \u7CBE\u7B80\u7248 */
p, div, span:not(.badge), td, th, .btn, button, a:not(.navbar-brand),
.form-control, .form-select, .form-check-label, input, textarea,
.card-header, .card-title, .card-body, .modal-content, .modal-title, .dropdown-menu,
.progress span, .alert, .breadcrumb, .list-group-item {
    font-weight: 500;
}

/* \u7EDF\u4E00Toast\u5F39\u7A97\u7CFB\u7EDF */
.toast-container {
    position: fixed;
    top: 15%;
    left: 50%;
    transform: translateX(-50%);
    z-index: 10000; /* \u786E\u4FDD\u5728\u6240\u6709\u5143\u7D20\u4E4B\u4E0A\uFF0C\u5305\u62EC\u6A21\u6001\u6846 */
    pointer-events: none;
    display: flex;
    flex-direction: column;
    align-items: center;
}

.unified-toast {
    pointer-events: auto;
    min-width: 120px;
    max-width: 90vw;
    padding: 16px 50px 16px 24px;
    margin-bottom: 12px;
    border-radius: 12px;
    backdrop-filter: blur(16px);
    border: 1px solid rgba(255, 255, 255, 0.2);
    font-weight: 500;
    font-size: 15px;
    position: relative;
    display: inline-flex;
    align-items: center;
    animation: toastIn 0.3s ease;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
}

.unified-toast.hiding {
    animation: toastOut 0.3s ease;
    opacity: 0;
}

.unified-toast.success {
    background: linear-gradient(135deg,
        rgba(34, 197, 94, calc(0.7 * var(--page-opacity, 0.8))),
        rgba(22, 163, 74, calc(0.7 * var(--page-opacity, 0.8))));
    color: white;
    border-color: rgba(34, 197, 94, calc(0.4 * var(--page-opacity, 0.8)));
}

.unified-toast.danger {
    background: linear-gradient(135deg,
        rgba(239, 68, 68, calc(0.7 * var(--page-opacity, 0.8))),
        rgba(220, 38, 38, calc(0.7 * var(--page-opacity, 0.8))));
    color: white;
    border-color: rgba(239, 68, 68, calc(0.4 * var(--page-opacity, 0.8)));
}

.unified-toast.warning {
    background: linear-gradient(135deg,
        rgba(245, 158, 11, calc(0.7 * var(--page-opacity, 0.8))),
        rgba(217, 119, 6, calc(0.7 * var(--page-opacity, 0.8))));
    color: white;
    border-color: rgba(245, 158, 11, calc(0.4 * var(--page-opacity, 0.8)));
}

.unified-toast.info {
    background: linear-gradient(135deg,
        rgba(59, 130, 246, calc(0.7 * var(--page-opacity, 0.8))),
        rgba(37, 99, 235, calc(0.7 * var(--page-opacity, 0.8))));
    color: white;
    border-color: rgba(59, 130, 246, calc(0.4 * var(--page-opacity, 0.8)));
}

.toast-icon {
    margin-right: 8px;
    font-size: 16px;
    flex-shrink: 0;
}

.toast-content {
    flex: 1;
    line-height: 1.4;
}

.toast-close {
    position: absolute;
    top: 50%;
    right: 12px;
    transform: translateY(-50%);
    background: none;
    border: none;
    color: rgba(255, 255, 255, 0.8);
    font-size: 16px;
    cursor: pointer;
    padding: 6px;
    border-radius: 50%;
    width: 28px;
    height: 28px;
}

.toast-close:hover {
    background: rgba(255, 255, 255, 0.2);
}

.toast-progress {
    position: absolute;
    bottom: 0;
    left: 0;
    height: 3px;
    background: rgba(255, 255, 255, 0.3);
    border-radius: 0 0 12px 12px;
    animation: progressBar 5s linear;
}

@keyframes toastIn {
    from { opacity: 0; transform: translateY(-20px); }
    to { opacity: 1; transform: translateY(0); }
}

@keyframes toastOut {
    from { opacity: 1; }
    to { opacity: 0; }
}

@keyframes progressBar {
    from { width: 100%; }
    to { width: 0%; }
}

[data-bs-theme="dark"] .unified-toast {
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    border-color: rgba(255, 255, 255, 0.1);
}

/* \u81EA\u5B9A\u4E49\u5BFC\u822A\u680F\u9AD8\u5EA6 */
.navbar {
    --bs-navbar-padding-y: 0.375rem;
    min-height: 50px;
    height: 50px;
}

.navbar-brand {
    padding-top: 0.3125rem;
    padding-bottom: 0.3125rem;
    line-height: 1.25;
}

/* ==================== 2026\u89C6\u89C9\u4F18\u5316\u5C42 ==================== */
:root {
    --monitor-bg: #f4f7fb;
    --monitor-surface: rgba(255, 255, 255, 0.92);
    --monitor-surface-strong: #ffffff;
    --monitor-border: rgba(15, 23, 42, 0.1);
    --monitor-text: #182033;
    --monitor-muted: #64748b;
    --monitor-primary: #2563eb;
    --monitor-accent: #14b8a6;
    --monitor-warning: #f59e0b;
    --monitor-danger: #ef4444;
    --monitor-shadow: 0 18px 45px rgba(15, 23, 42, 0.08);
}

[data-bs-theme="dark"] {
    --monitor-bg: #111827;
    --monitor-surface: rgba(17, 24, 39, 0.9);
    --monitor-surface-strong: #172033;
    --monitor-border: rgba(148, 163, 184, 0.18);
    --monitor-text: #f8fafc;
    --monitor-muted: #cbd5e1;
    --monitor-primary: #60a5fa;
    --monitor-accent: #2dd4bf;
    --monitor-shadow: 0 18px 45px rgba(0, 0, 0, 0.28);
}

html, body {
    background: var(--monitor-bg) !important;
    color: var(--monitor-text);
}

body {
    font-family: Inter, "Segoe UI", "Microsoft YaHei", system-ui, -apple-system, sans-serif;
}

.navbar {
    min-height: 58px;
    height: auto;
    border-bottom: 1px solid var(--monitor-border);
    background: var(--monitor-surface) !important;
    box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
}

.navbar .container {
    min-height: 58px;
}

.navbar-brand {
    color: var(--monitor-text) !important;
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 1.05rem;
}

.navbar-brand svg {
    width: 30px;
    height: 30px;
    filter: drop-shadow(0 6px 12px rgba(37, 99, 235, 0.22));
}

.navbar .btn-outline-light,
.navbar .nav-link {
    border-color: var(--monitor-border) !important;
    color: var(--monitor-text) !important;
    background: rgba(255, 255, 255, 0.45);
    border-radius: 8px;
}

[data-bs-theme="dark"] .navbar .btn-outline-light,
[data-bs-theme="dark"] .navbar .nav-link {
    background: rgba(15, 23, 42, 0.55);
}

.navbar .nav-link {
    padding: 0.35rem 0.75rem;
    border: 1px solid var(--monitor-border);
}

.navbar .btn-outline-light:hover,
.navbar .nav-link:hover {
    color: #ffffff !important;
    background: var(--monitor-primary) !important;
    border-color: var(--monitor-primary) !important;
}

.card.shadow-sm {
    border: 1px solid var(--monitor-border);
    border-radius: 8px;
    background: var(--monitor-surface);
    box-shadow: var(--monitor-shadow) !important;
}

.card-body {
    padding: 1.25rem;
}

.dashboard-section,
.admin-header-row {
    position: relative;
}

.card-title {
    color: var(--monitor-text) !important;
    display: flex;
    align-items: center;
    gap: 0.35rem;
    font-weight: 700;
}

.card-title i {
    color: var(--monitor-primary) !important;
}

.table {
    --bs-table-bg: transparent;
    --bs-table-striped-bg: rgba(37, 99, 235, 0.035);
    --bs-table-hover-bg: rgba(20, 184, 166, 0.075);
    color: var(--monitor-text);
    margin-bottom: 0;
}

.table-responsive {
    border: 1px solid var(--monitor-border);
    border-radius: 8px;
    overflow: hidden;
    background: var(--monitor-surface-strong);
}

.table thead th {
    background: rgba(37, 99, 235, 0.08) !important;
    color: var(--monitor-muted);
    border-bottom: 1px solid var(--monitor-border);
    font-size: 0.8rem;
    text-transform: uppercase;
    white-space: nowrap;
}

.table td {
    border-color: var(--monitor-border);
    vertical-align: middle;
}

.badge {
    border-radius: 6px;
    font-weight: 700;
    letter-spacing: 0;
}

.progress {
    height: 1.05rem !important;
    min-width: 74px;
    border-radius: 999px;
    overflow: hidden;
    background: rgba(148, 163, 184, 0.22) !important;
}

.progress-bar {
    border-radius: inherit;
    background: linear-gradient(135deg, var(--monitor-primary), var(--monitor-accent)) !important;
}

.progress span {
    color: var(--monitor-text) !important;
    font-size: 0.76rem !important;
    line-height: 1.05rem !important;
    font-weight: 800 !important;
    text-shadow: none !important;
}

.bg-light-green {
    background-color: var(--monitor-accent) !important;
}

.history-bar-container {
    padding: 3px;
    border-radius: 7px;
    background: rgba(148, 163, 184, 0.12);
}

.history-bar {
    border-radius: 3px;
}

.history-bar-up { background-color: #22c55e !important; }
.history-bar-warning { background-color: #f59e0b !important; }
.history-bar-down { background-color: #ef4444 !important; }
.history-bar-pending { background-color: #94a3b8 !important; }

.btn {
    border-radius: 8px;
    font-weight: 700;
}

.btn-primary {
    background: var(--monitor-primary);
    border-color: var(--monitor-primary);
}

.btn-success {
    background: var(--monitor-accent);
    border-color: var(--monitor-accent);
}

.form-control,
.form-select {
    border-radius: 8px;
    border-color: var(--monitor-border);
}

.form-check-input {
    cursor: pointer;
}

.form-check-input:checked {
    background-color: var(--monitor-accent);
    border-color: var(--monitor-accent);
}

.section-display-switch {
    color: var(--monitor-muted);
    font-size: 0.9rem;
}

.section-display-switch .form-check {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.35rem 0.65rem 0.35rem 2.6rem;
    border: 1px solid var(--monitor-border);
    border-radius: 8px;
    background: rgba(37, 99, 235, 0.05);
}

.mobile-server-card,
.mobile-site-card {
    border-radius: 8px;
    border-color: var(--monitor-border);
    background: var(--monitor-surface-strong);
    box-shadow: 0 12px 30px rgba(15, 23, 42, 0.08);
}

.mobile-card-header {
    background: rgba(37, 99, 235, 0.07);
}

.mobile-card-title {
    color: var(--monitor-text) !important;
}

.mobile-card-label {
    color: var(--monitor-muted) !important;
}

.footer.fixed-bottom {
    height: 38px;
    background: var(--monitor-surface) !important;
    border-top: 1px solid var(--monitor-border);
}

.footer .text-muted {
    color: var(--monitor-muted) !important;
}

@media (max-width: 768px) {
    .navbar {
        min-height: auto;
    }

    .navbar .container {
        min-height: 56px;
        gap: 0.5rem;
    }

    .card-body {
        padding: 1rem;
    }

    .section-display-switch .form-check {
        width: 100%;
        justify-content: flex-start;
    }
}

/* ==================== AppStorePrice\u98CE\u683C\u91CD\u6784 ==================== */
:root {
    --monitor-bg: #fbfcff;
    --monitor-surface: #ffffff;
    --monitor-surface-strong: #ffffff;
    --monitor-border: #e7ecf6;
    --monitor-text: #101828;
    --monitor-muted: #667085;
    --monitor-primary: #2962ff;
    --monitor-accent: #7c3aed;
    --monitor-soft-blue: #eef4ff;
    --monitor-soft-purple: #f3f0ff;
    --monitor-shadow: 0 18px 50px rgba(16, 24, 40, 0.08);
}

[data-bs-theme="dark"] {
    --monitor-bg: #0f172a;
    --monitor-surface: #111c31;
    --monitor-surface-strong: #14213a;
    --monitor-border: rgba(226, 232, 240, 0.14);
    --monitor-text: #f8fafc;
    --monitor-muted: #cbd5e1;
    --monitor-primary: #7aa2ff;
    --monitor-accent: #b794f4;
    --monitor-soft-blue: rgba(122, 162, 255, 0.14);
    --monitor-soft-purple: rgba(183, 148, 244, 0.13);
    --monitor-shadow: 0 22px 56px rgba(0, 0, 0, 0.34);
}

body {
    background:
        radial-gradient(circle at 18% 8%, rgba(41, 98, 255, 0.08), transparent 28%),
        radial-gradient(circle at 82% 12%, rgba(124, 58, 237, 0.08), transparent 30%),
        var(--monitor-bg) !important;
    color: var(--monitor-text);
    letter-spacing: 0;
}

.monitor-shell,
.appstore-shell {
    max-width: 1120px;
    padding-top: 2rem;
}

.admin-shell {
    padding-top: 2rem;
}

.appstore-hero {
    text-align: center;
    padding: 1.2rem 1rem 2.4rem;
}

.admin-hero {
    padding-bottom: 1.6rem;
}

.hero-kicker {
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
    padding: 0.45rem 0.85rem;
    border: 1px solid var(--monitor-border);
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.72);
    color: var(--monitor-primary);
    font-size: 0.86rem;
    font-weight: 700;
    box-shadow: 0 8px 24px rgba(16, 24, 40, 0.05);
}

[data-bs-theme="dark"] .hero-kicker {
    background: rgba(20, 33, 58, 0.7);
}

.appstore-hero h1 {
    margin: 1rem auto 0.65rem;
    max-width: 760px;
    color: var(--monitor-text);
    font-size: clamp(2.35rem, 5vw, 4.6rem);
    line-height: 1.03;
    font-weight: 800;
}

.admin-hero h1 {
    font-size: clamp(2rem, 4vw, 3.2rem);
}

.appstore-hero p {
    max-width: 650px;
    margin: 0 auto;
    color: var(--monitor-muted);
    font-size: 1.08rem;
    line-height: 1.7;
}

.hero-actions {
    display: flex;
    justify-content: center;
    flex-wrap: wrap;
    gap: 0.8rem;
    margin-top: 1.35rem;
}

.hero-btn {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    min-height: 44px;
    padding: 0.7rem 1.05rem;
    border-radius: 999px;
    text-decoration: none;
    font-weight: 800;
    border: 1px solid transparent;
    transition: transform 0.16s ease, box-shadow 0.16s ease, background 0.16s ease;
}

.hero-btn:hover {
    transform: translateY(-1px);
}

.hero-btn-primary {
    color: #ffffff;
    background: linear-gradient(135deg, var(--monitor-primary), var(--monitor-accent));
    box-shadow: 0 12px 28px rgba(41, 98, 255, 0.22);
}

.hero-btn-secondary {
    color: var(--monitor-text);
    background: #ffffff;
    border-color: var(--monitor-border);
    box-shadow: 0 10px 24px rgba(16, 24, 40, 0.06);
}

[data-bs-theme="dark"] .hero-btn-secondary {
    background: var(--monitor-surface-strong);
}

.navbar {
    background: rgba(255, 255, 255, 0.86) !important;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    box-shadow: none;
}

[data-bs-theme="dark"] .navbar {
    background: rgba(15, 23, 42, 0.82) !important;
}

.navbar .btn-outline-light,
.navbar .nav-link {
    border-radius: 999px;
    background: #ffffff;
    box-shadow: 0 8px 18px rgba(16, 24, 40, 0.06);
}

[data-bs-theme="dark"] .navbar .btn-outline-light,
[data-bs-theme="dark"] .navbar .nav-link {
    background: var(--monitor-surface-strong);
}

.card.shadow-sm {
    border-radius: 18px;
    border: 1px solid var(--monitor-border);
    background: rgba(255, 255, 255, 0.88);
    box-shadow: var(--monitor-shadow) !important;
}

[data-bs-theme="dark"] .card.shadow-sm {
    background: rgba(17, 28, 49, 0.9);
}

.card-body {
    padding: 1.15rem;
}

.dashboard-section,
.card-body > div {
    border-radius: 14px;
}

.card-title {
    font-size: 1.25rem;
    font-weight: 800;
}

.card-title i {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    border-radius: 10px;
    background: linear-gradient(135deg, var(--monitor-soft-blue), var(--monitor-soft-purple));
    color: var(--monitor-primary) !important;
}

.table-responsive {
    border-radius: 14px;
    border: 1px solid var(--monitor-border);
    background: var(--monitor-surface);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7);
}

.table thead th {
    background: linear-gradient(180deg, #f8faff, #f2f6ff) !important;
    color: #667085;
    font-size: 0.78rem;
    text-transform: none;
}

[data-bs-theme="dark"] .table thead th {
    background: rgba(122, 162, 255, 0.1) !important;
    color: var(--monitor-muted);
}

.table > :not(caption) > * > * {
    padding: 0.85rem 0.75rem;
}

#serverTableBody td:nth-child(3),
#serverTableBody td:nth-child(4),
#serverTableBody td:nth-child(5) {
    min-width: 86px;
}

#serverTableBody td:nth-child(6),
#serverTableBody td:nth-child(7),
#serverTableBody td:nth-child(8),
#serverTableBody td:nth-child(9),
#serverTableBody td:nth-child(10),
#serverTableBody td:nth-child(11),
#siteStatusTableBody td {
    font-weight: 700;
}

.table-striped > tbody > tr:nth-of-type(odd) > * {
    --bs-table-accent-bg: rgba(41, 98, 255, 0.025);
}

.btn-primary,
.btn-success,
.btn-info {
    border: none;
    background: linear-gradient(135deg, var(--monitor-primary), var(--monitor-accent)) !important;
    color: #fff !important;
    box-shadow: 0 10px 24px rgba(41, 98, 255, 0.18);
}

.btn-outline-secondary,
.btn-outline-primary,
.btn-outline-info,
.btn-outline-danger {
    background: #ffffff;
    border-color: var(--monitor-border);
}

[data-bs-theme="dark"] .btn-outline-secondary,
[data-bs-theme="dark"] .btn-outline-primary,
[data-bs-theme="dark"] .btn-outline-info,
[data-bs-theme="dark"] .btn-outline-danger {
    background: var(--monitor-surface-strong);
}

.form-control {
    min-height: 42px;
    border-radius: 12px;
    background: var(--monitor-surface);
}

.section-display-switch .form-check {
    border-radius: 999px;
    background: #f8faff;
}

[data-bs-theme="dark"] .section-display-switch .form-check {
    background: rgba(122, 162, 255, 0.08);
}

.mobile-server-card,
.mobile-site-card {
    border-radius: 16px;
}

.footer.fixed-bottom {
    background: rgba(255, 255, 255, 0.84) !important;
    backdrop-filter: blur(14px);
    -webkit-backdrop-filter: blur(14px);
}

[data-bs-theme="dark"] .footer.fixed-bottom {
    background: rgba(15, 23, 42, 0.84) !important;
}

.app-footer,
.footer.fixed-bottom {
    position: static !important;
    height: auto !important;
    margin-top: auto !important;
    padding: 10px 0 !important;
    background: transparent !important;
    border-top: 1px solid var(--monitor-border) !important;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
}

.app-footer .container,
.footer.fixed-bottom .container {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.75rem;
}

body {
    padding-bottom: 0 !important;
}

.dropdown-menu:not(.show) {
    display: none;
}

.dropdown-menu.show {
    display: block;
}

.modal:not(.show) {
    display: none !important;
}

.modal.show {
    display: block;
}

@media (max-width: 768px) {
    .monitor-shell,
    .appstore-shell {
        padding-top: 2rem;
    }

    .appstore-hero {
        padding: 0.7rem 0.4rem 1.5rem;
    }

    .appstore-hero h1 {
        font-size: 2.35rem;
    }

    .appstore-hero p {
        font-size: 0.98rem;
    }

    .hero-actions {
        gap: 0.55rem;
    }

    .hero-btn {
        width: 100%;
        justify-content: center;
    }

    .card.shadow-sm {
        border-radius: 16px;
    }
}

/* ==================== Apple glassmorphism polish ==================== */
:root {
    --apple-bg: #f6f9ff;
    --apple-glass: rgba(255, 255, 255, 0.66);
    --apple-glass-strong: rgba(255, 255, 255, 0.82);
    --apple-glass-border: rgba(255, 255, 255, 0.72);
    --apple-line: rgba(129, 151, 183, 0.22);
    --apple-text: #0f172a;
    --apple-muted: #667085;
    --apple-blue: #007aff;
    --apple-blue-2: #5ac8fa;
    --apple-purple: #5856d6;
    --apple-green: #34c759;
    --apple-yellow: #ffcc00;
    --apple-red: #ff3b30;
    --apple-shadow: 0 24px 70px rgba(31, 41, 55, 0.12);
    --apple-shadow-soft: 0 12px 36px rgba(31, 41, 55, 0.08);
}

[data-bs-theme="dark"] {
    --apple-glass: rgba(30, 35, 45, 0.75);
    --apple-glass-strong: rgba(35, 42, 55, 0.88);
    --apple-glass-border: rgba(255, 255, 255, 0.12);
    --apple-line: rgba(255, 255, 255, 0.15);
    --apple-text: #f1f5f9;
    --apple-muted: #94a3b8;
    --apple-shadow: 0 24px 70px rgba(0, 0, 0, 0.4);
    --apple-shadow-soft: 0 12px 36px rgba(0, 0, 0, 0.28);
}

html,
body {
    background: linear-gradient(135deg, rgba(245, 249, 255, 0.98) 0%, rgba(238, 246, 255, 0.96) 44%, rgba(250, 248, 255, 0.98) 100%) !important;
    color: var(--apple-text) !important;
}

[data-bs-theme="dark"] body {
    background: linear-gradient(135deg, #1a1f2e 0%, #161b29 44%, #1e2433 100%) !important;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Segoe UI", "Microsoft YaHei", system-ui, sans-serif;
    letter-spacing: 0;
    padding-bottom: 0 !important;
}

.navbar {
    height: auto !important;
    min-height: 54px !important;
    background: rgba(255, 255, 255, 0.56) !important;
    border-bottom: 1px solid rgba(255, 255, 255, 0.75) !important;
    box-shadow: 0 1px 0 rgba(129, 151, 183, 0.12);
    backdrop-filter: saturate(180%) blur(28px);
    -webkit-backdrop-filter: saturate(180%) blur(28px);
}

[data-bs-theme="light"] .navbar {
    background: rgba(255, 255, 255, 0.56) !important;
}

[data-bs-theme="dark"] .navbar {
    background: rgba(30, 35, 45, 0.82) !important;
    border-bottom-color: rgba(255, 255, 255, 0.1) !important;
}

.navbar .container {
    min-height: 54px !important;
}

.navbar .d-flex.align-items-center {
    gap: 0.45rem !important;
    flex-wrap: nowrap !important;
}

.navbar-brand {
    color: var(--apple-text) !important;
    font-weight: 800 !important;
    gap: 0.55rem;
    font-size: 1.02rem;
}

.brand-mark {
    width: 30px;
    height: 30px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 10px;
    color: #fff;
    background: linear-gradient(135deg, var(--apple-blue), var(--apple-blue-2));
    box-shadow: 0 8px 18px rgba(0, 122, 255, 0.22);
}

.brand-mark i {
    display: inline-block;
    font-size: 1.05rem;
    line-height: 1;
}

.brand-mark::before {
    content: none;
}

.brand-text {
    letter-spacing: -0.02em;
}

.navbar-brand:not(:has(.brand-text)) {
    font-size: 0 !important;
}

.navbar-brand:not(:has(.brand-text)) svg {
    display: none !important;
}

.navbar-brand:not(:has(.brand-text))::before {
    content: "\u2301";
    width: 30px;
    height: 30px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 10px;
    color: #fff;
    font-size: 1.12rem;
    font-weight: 900;
    line-height: 1;
    background: linear-gradient(135deg, var(--apple-blue), var(--apple-blue-2));
    box-shadow: 0 8px 18px rgba(0, 122, 255, 0.22);
}

.navbar-brand:not(:has(.brand-text))::after {
    content: "Monitor";
    color: var(--apple-text);
    font-size: 1.02rem;
    font-weight: 800;
    line-height: 1;
}

.navbar .btn-outline-light,
.navbar .nav-link {
    min-height: 34px;
    border: 1px solid var(--apple-glass-border) !important;
    border-radius: 999px !important;
    color: var(--apple-text) !important;
    background: rgba(255, 255, 255, 0.64) !important;
    box-shadow: 0 8px 24px rgba(31, 41, 55, 0.07);
    backdrop-filter: blur(18px);
    -webkit-backdrop-filter: blur(18px);
}

.navbar .btn-outline-light {
    width: 36px;
    height: 36px;
    padding: 0 !important;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex: 0 0 36px;
}

.navbar .nav-link {
    min-height: 36px;
    padding: 0.35rem 0.7rem !important;
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
}

.navbar .btn-outline-light.me-2,
.navbar .nav-link.me-2 {
    margin-right: 0 !important;
}

.navbar i {
    color: currentColor !important;
}

[data-bs-theme="dark"] .navbar .btn-outline-light,
[data-bs-theme="dark"] .navbar .nav-link {
    background: rgba(14, 14, 16, 0.9) !important;
    border-color: rgba(255, 255, 255, 0.12) !important;
}

.navbar .btn-outline-light:hover,
.navbar .nav-link:hover {
    color: #ffffff !important;
    background: linear-gradient(135deg, var(--apple-blue), var(--apple-purple)) !important;
    border-color: transparent !important;
}

.monitor-shell,
.admin-shell {
    max-width: 1180px;
    padding-top: 3.5rem;
}

.dashboard-stack {
    display: flex;
    flex-direction: column;
    gap: 1.6rem;
}

.dashboard-stack .dashboard-section {
    padding: 0;
}

.dashboard-stack #statusSectionDivider {
    margin: 0 !important;
}

.mobile-only-view {
    display: none !important;
}

@media (max-width: 768px) {
    .desktop-table-view {
        display: none !important;
    }

    .mobile-only-view {
        display: block !important;
    }
}

.card.shadow-sm {
    border: 1px solid var(--apple-glass-border) !important;
    border-radius: 28px !important;
    background: var(--apple-glass) !important;
    box-shadow: var(--apple-shadow) !important;
    backdrop-filter: saturate(180%) blur(32px);
    -webkit-backdrop-filter: saturate(180%) blur(32px);
}

[data-bs-theme="dark"] .card.shadow-sm {
    background: rgba(6, 6, 7, 0.88) !important;
    border-color: rgba(255, 255, 255, 0.08) !important;
}

.card-body {
    padding: 1.35rem !important;
}

.card-title {
    color: var(--apple-text) !important;
    font-size: 1.2rem !important;
    font-weight: 800 !important;
}

.card-title i {
    width: 36px;
    height: 36px;
    border-radius: 14px;
    background: rgba(255, 255, 255, 0.72) !important;
    color: var(--apple-blue) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.9), 0 10px 26px rgba(0, 122, 255, 0.12);
}

[data-bs-theme="dark"] .card-title i {
    background: rgba(28, 28, 31, 0.92) !important;
    color: #8ab4ff !important;
    box-shadow: none;
}

.alert-info {
    border: 1px solid rgba(129, 151, 183, 0.16) !important;
    border-radius: 14px !important;
    background: rgba(255, 255, 255, 0.5) !important;
    color: var(--apple-muted) !important;
    font-weight: 800;
}

[data-bs-theme="dark"] .alert-info {
    border-color: rgba(255, 255, 255, 0.08) !important;
    background: rgba(10, 10, 11, 0.96) !important;
    color: #d4d4d8 !important;
}

.table-responsive {
    border: 1px solid var(--apple-line) !important;
    border-radius: 22px !important;
    background: rgba(255, 255, 255, 0.5) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.9), var(--apple-shadow-soft);
    backdrop-filter: saturate(160%) blur(22px);
    -webkit-backdrop-filter: saturate(160%) blur(22px);
}

.table {
    color: var(--apple-text) !important;
    --bs-table-bg: transparent;
    --bs-table-striped-bg: rgba(255, 255, 255, 0.28);
    --bs-table-hover-bg: rgba(0, 122, 255, 0.08);
}

.table thead th {
    background: rgba(248, 251, 255, 0.74) !important;
    color: var(--apple-muted) !important;
    border-bottom: 1px solid var(--apple-line) !important;
    font-size: 0.78rem !important;
    font-weight: 700 !important;
}

.table td {
    border-color: rgba(129, 151, 183, 0.14) !important;
}

[data-bs-theme="dark"] .table-responsive {
    background: rgba(10, 10, 11, 0.94) !important;
    border-color: rgba(255, 255, 255, 0.08) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05), var(--apple-shadow-soft);
}

[data-bs-theme="dark"] .table {
    --bs-table-striped-bg: rgba(255, 255, 255, 0.025);
    --bs-table-hover-bg: rgba(255, 255, 255, 0.045);
}

[data-bs-theme="dark"] .table thead th {
    background: rgba(15, 15, 17, 0.95) !important;
    color: #a1a1aa !important;
}

[data-bs-theme="dark"] .table td {
    border-color: rgba(255, 255, 255, 0.07) !important;
    color: #d4d4d8 !important;
}

.btn {
    border-radius: 999px !important;
    font-weight: 800 !important;
}

.btn-primary,
.btn-success,
.btn-info {
    background: linear-gradient(135deg, var(--apple-blue), var(--apple-purple)) !important;
    color: #ffffff !important;
    border: none !important;
    box-shadow: 0 12px 30px rgba(0, 122, 255, 0.22);
}

.btn-outline-secondary,
.btn-outline-primary,
.btn-outline-info,
.btn-outline-danger {
    background: rgba(255, 255, 255, 0.56) !important;
    border: 1px solid var(--apple-line) !important;
    color: var(--apple-text) !important;
    box-shadow: 0 8px 22px rgba(31, 41, 55, 0.06);
}

.form-control,
.form-select,
.input-group .form-control {
    border-radius: 16px !important;
    border: 1px solid var(--apple-line) !important;
    background: rgba(255, 255, 255, 0.62) !important;
    color: var(--apple-text) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.72);
}

.login-shell {
    flex: 1 0 auto;
    min-height: calc(100vh - 154px);
    display: flex;
    align-items: center;
    justify-content: center;
    padding-top: 2rem;
    padding-bottom: 2rem;
}

.login-panel-wrap {
    width: min(100%, 430px);
}

.login-card {
    overflow: hidden;
    border: 1px solid var(--apple-glass-border) !important;
    border-radius: 30px !important;
    background: rgba(255, 255, 255, 0.62) !important;
    box-shadow: 0 26px 80px rgba(31, 41, 55, 0.14), inset 0 1px 0 rgba(255, 255, 255, 0.88) !important;
    backdrop-filter: saturate(180%) blur(34px);
    -webkit-backdrop-filter: saturate(180%) blur(34px);
}

.login-card-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1.35rem 1.45rem 0.85rem !important;
    border: 0 !important;
    background: transparent !important;
}

.login-icon {
    width: 46px;
    height: 46px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex: 0 0 46px;
    border-radius: 16px;
    color: #fff;
    background: linear-gradient(135deg, var(--apple-blue), var(--apple-blue-2));
    box-shadow: 0 14px 30px rgba(0, 122, 255, 0.24);
}

.login-kicker {
    color: var(--apple-muted);
    font-size: 0.78rem;
    font-weight: 800;
    letter-spacing: 0;
}

.login-card-body {
    padding: 0.75rem 1.45rem 1.25rem !important;
}

.login-card .form-label {
    margin-bottom: 0.45rem;
    color: var(--apple-muted);
    font-size: 0.86rem;
    font-weight: 800;
}

.login-card .form-control {
    min-height: 52px;
    border-radius: 18px !important;
    background: rgba(255, 255, 255, 0.72) !important;
}

.login-card .form-control:focus {
    border-color: rgba(0, 122, 255, 0.42) !important;
    box-shadow: 0 0 0 4px rgba(0, 122, 255, 0.12), inset 0 1px 0 rgba(255, 255, 255, 0.8) !important;
}

.login-submit {
    min-height: 52px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    font-size: 1rem;
}

.login-card-footer {
    display: flex;
    align-items: flex-start;
    gap: 0.55rem;
    padding: 0.95rem 1.45rem 1.25rem !important;
    border-top: 1px solid rgba(129, 151, 183, 0.14) !important;
    background: rgba(255, 255, 255, 0.28) !important;
    color: var(--apple-muted) !important;
}

.login-card-footer i {
    margin-top: 0.5rem;
    color: var(--apple-blue);
}

[data-bs-theme="dark"] .login-card {
    background: rgba(30, 35, 45, 0.75) !important;
    border-color: rgba(255, 255, 255, 0.12) !important;
    box-shadow: 0 26px 80px rgba(0, 0, 0, 0.3), inset 0 1px 0 rgba(255, 255, 255, 0.08) !important;
}

[data-bs-theme="dark"] .login-card .form-control,
[data-bs-theme="dark"] .login-card-footer {
    background: rgba(35, 42, 55, 0.65) !important;
    color: #f1f5f9 !important;
    border-color: rgba(255, 255, 255, 0.15) !important;
}

[data-bs-theme="dark"] .login-card .form-label {
    color: #94a3b8 !important;
}

[data-bs-theme="dark"] .login-kicker {
    color: #94a3b8 !important;
}

[data-bs-theme="dark"] .login-icon {
    box-shadow: 0 14px 30px rgba(0, 122, 255, 0.35);
}

[data-bs-theme="dark"] .login-submit {
    background: linear-gradient(135deg, #007aff, #5ac8fa) !important;
    border-color: transparent !important;
    color: #ffffff !important;
}

[data-bs-theme="dark"] .login-submit:hover {
    background: linear-gradient(135deg, #0066d6, #4ab8e8) !important;
    transform: translateY(-1px);
    box-shadow: 0 8px 20px rgba(0, 122, 255, 0.35) !important;
}

/* \u767B\u5F55\u9875\u9762\u81EA\u5B9A\u4E49\u80CC\u666F\u73BB\u7483\u6548\u679C */
body.custom-background-enabled .login-card {
    backdrop-filter: saturate(180%) blur(34px) !important;
    -webkit-backdrop-filter: saturate(180%) blur(34px) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .login-card {
    background: rgba(30, 35, 45, 0.72) !important;
}

.section-display-switch .form-check {
    border: 1px solid var(--apple-line) !important;
    border-radius: 999px !important;
    background: rgba(255, 255, 255, 0.48) !important;
    color: var(--apple-muted);
}

.form-check-input:checked {
    background-color: var(--apple-green) !important;
    border-color: var(--apple-green) !important;
}

.progress {
    min-width: 86px;
    background: rgba(129, 151, 183, 0.18) !important;
}

.progress-bar.bg-light-green,
.progress-bar.bg-success {
    background: linear-gradient(90deg, #22c55e, var(--apple-green)) !important;
}

.progress-bar.bg-warning {
    background: linear-gradient(90deg, #f59e0b, var(--apple-yellow)) !important;
}

.progress-bar.bg-danger {
    background: linear-gradient(90deg, #ef4444, var(--apple-red)) !important;
}

.badge.bg-success { background-color: var(--apple-green) !important; }
.badge.bg-warning { background-color: var(--apple-yellow) !important; }
.badge.bg-danger { background-color: var(--apple-red) !important; }

.history-bar-container {
    height: 22px !important;
    gap: 4px !important;
    background: transparent !important;
    border: 0 !important;
    border-radius: 0 !important;
    padding: 0 !important;
    overflow: visible !important;
}

.history-bar-container:hover {
    background: transparent !important;
    box-shadow: none !important;
}

.history-bar {
    width: 8px !important;
    height: 18px !important;
    border-radius: 999px !important;
    opacity: 0.95;
    cursor: default;
    transform-origin: center;
    transition: transform 0.18s cubic-bezier(.2,.8,.2,1), opacity 0.18s ease, box-shadow 0.18s ease;
}

.history-bar:hover {
    transform: scale(1.38);
    opacity: 1;
    box-shadow: 0 8px 18px rgba(0, 0, 0, 0.18);
    z-index: 2;
}

.history-bar-up { background-color: var(--apple-green) !important; }
.history-bar-warning { background-color: var(--apple-yellow) !important; }
.history-bar-down { background-color: var(--apple-red) !important; }
.history-bar-pending { background-color: rgba(129, 151, 183, 0.48) !important; }

.history-tooltip {
    position: fixed;
    z-index: 9999;
    max-width: 260px;
    padding: 0.55rem 0.7rem;
    border: 1px solid rgba(255, 255, 255, 0.68);
    border-radius: 14px;
    color: var(--apple-text);
    background: rgba(255, 255, 255, 0.78);
    box-shadow: 0 18px 42px rgba(31, 41, 55, 0.16);
    backdrop-filter: saturate(180%) blur(22px);
    -webkit-backdrop-filter: saturate(180%) blur(22px);
    font-size: 0.78rem;
    font-weight: 700;
    line-height: 1.45;
    pointer-events: none;
    opacity: 0;
    transform: translate(-50%, -8px) scale(0.96);
    transition: opacity 0.14s ease, transform 0.14s cubic-bezier(.2,.8,.2,1);
}

.history-tooltip.show {
    opacity: 1;
    transform: translate(-50%, -14px) scale(1);
}

[data-bs-theme="dark"] .history-tooltip {
    border-color: rgba(255, 255, 255, 0.1);
    color: #f4f4f5;
    background: rgba(24, 24, 27, 0.94);
    box-shadow: 0 18px 42px rgba(0, 0, 0, 0.42);
}

.empty-table-cell {
    padding: 1.2rem !important;
    color: var(--apple-muted) !important;
    font-weight: 700 !important;
}

.table tbody td.empty-table-cell,
.table-striped > tbody > tr > td.empty-table-cell,
.table-hover > tbody > tr:hover > td.empty-table-cell {
    color: var(--apple-muted) !important;
    background: transparent !important;
}

[data-bs-theme="dark"] .empty-table-cell {
    color: #d4d4d8 !important;
    background: rgba(24, 24, 27, 0.82) !important;
}

[data-bs-theme="dark"] .table tbody td.empty-table-cell,
[data-bs-theme="dark"] .table-striped > tbody > tr > td.empty-table-cell,
[data-bs-theme="dark"] .table-hover > tbody > tr:hover > td.empty-table-cell {
    color: #d4d4d8 !important;
    background: rgba(24, 24, 27, 0.82) !important;
}

.mobile-server-card,
.mobile-site-card {
    border: 1px solid var(--apple-glass-border) !important;
    border-radius: 24px !important;
    background: var(--apple-glass-strong) !important;
    box-shadow: var(--apple-shadow-soft) !important;
    backdrop-filter: blur(24px);
    -webkit-backdrop-filter: blur(24px);
}

.app-footer,
.footer.fixed-bottom {
    color: var(--apple-muted) !important;
    background: transparent !important;
    border-top: 1px solid rgba(129, 151, 183, 0.16) !important;
}

[data-bs-theme="dark"] .app-footer,
[data-bs-theme="dark"] .footer.fixed-bottom {
    background: rgba(14, 14, 15, 0.92) !important;
    border-top-color: rgba(255, 255, 255, 0.08) !important;
}

body.custom-background-enabled {
    --surface-alpha: calc(0.32 + (var(--page-opacity, 0.8) * 0.34));
    --background-dim: 0.14;
    background: transparent !important;
    position: relative;
}

body.custom-background-enabled::before {
    z-index: -2 !important;
    opacity: 1 !important;
    filter: none !important;
}

body.custom-background-enabled::after {
    content: "";
    position: fixed;
    inset: 0;
    z-index: -1;
    pointer-events: none;
    background: rgba(246, 249, 255, var(--background-dim));
}

[data-bs-theme="dark"] body.custom-background-enabled {
    --background-dim: 0.2;
}

[data-bs-theme="dark"] body.custom-background-enabled::after {
    background: rgba(0, 0, 0, var(--background-dim));
}

body.custom-background-enabled .navbar {
    background: rgba(255, 255, 255, var(--surface-alpha)) !important;
    backdrop-filter: saturate(180%) blur(24px);
    -webkit-backdrop-filter: saturate(180%) blur(24px);
}

[data-bs-theme="dark"] body.custom-background-enabled .navbar {
    background: rgba(4, 4, 5, var(--surface-alpha)) !important;
}

body.custom-background-enabled .card,
body.custom-background-enabled .modal-content,
body.custom-background-enabled .table-responsive,
body.custom-background-enabled .mobile-server-card,
body.custom-background-enabled .mobile-site-card {
    background: rgba(255, 255, 255, var(--surface-alpha)) !important;
    border-color: rgba(255, 255, 255, 0.42) !important;
    backdrop-filter: saturate(170%) blur(22px);
    -webkit-backdrop-filter: saturate(170%) blur(22px);
}

[data-bs-theme="dark"] body.custom-background-enabled .card,
[data-bs-theme="dark"] body.custom-background-enabled .modal-content,
[data-bs-theme="dark"] body.custom-background-enabled .table-responsive,
[data-bs-theme="dark"] body.custom-background-enabled .mobile-server-card,
[data-bs-theme="dark"] body.custom-background-enabled .mobile-site-card {
    background: rgba(6, 6, 7, var(--surface-alpha)) !important;
    border-color: rgba(255, 255, 255, 0.1) !important;
}

body.custom-background-enabled .form-control,
body.custom-background-enabled .form-select {
    background: rgba(255, 255, 255, calc(var(--surface-alpha) + 0.12)) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .form-control,
[data-bs-theme="dark"] body.custom-background-enabled .form-select {
    background: rgba(20, 20, 22, calc(var(--surface-alpha) + 0.12)) !important;
}

.app-footer .text-muted,
.footer.fixed-bottom .text-muted {
    color: var(--apple-muted) !important;
}

@media (max-width: 768px) {
    .monitor-shell,
    .admin-shell {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }

    .card.shadow-sm {
        border-radius: 24px !important;
    }

    .card-body {
        padding: 1rem !important;
    }

    .navbar .container {
        padding-left: 1rem;
        padding-right: 1rem;
    }

    .navbar-brand {
        font-size: 1rem;
    }

    .brand-mark {
        width: 30px;
        height: 30px;
    }

    .navbar .btn-outline-light {
        width: 34px;
        height: 34px;
        flex-basis: 34px;
    }

    .navbar .nav-link {
        min-height: 34px;
        padding: 0.32rem 0.62rem !important;
        font-size: 0.92rem;
    }

    .navbar .d-flex.align-items-center {
        gap: 0.45rem !important;
    }
}

.table tbody td.empty-table-cell,
.table-striped > tbody > tr > td.empty-table-cell,
.table-hover > tbody > tr:hover > td.empty-table-cell {
    color: #667085 !important;
    background: transparent !important;
}

[data-bs-theme="dark"] body {
    background: #030303 !important;
}

[data-bs-theme="dark"] .navbar,
[data-bs-theme="dark"] .app-footer,
[data-bs-theme="dark"] .footer.fixed-bottom {
    background: #030303 !important;
    border-color: rgba(255, 255, 255, 0.06) !important;
}

[data-bs-theme="dark"] .navbar .container,
[data-bs-theme="dark"] .app-footer .container,
[data-bs-theme="dark"] .footer.fixed-bottom .container {
    background: transparent !important;
}

[data-bs-theme="dark"] .table tbody td.empty-table-cell,
[data-bs-theme="dark"] .table-striped > tbody > tr > td.empty-table-cell,
[data-bs-theme="dark"] .table-hover > tbody > tr:hover > td.empty-table-cell {
    color: #a1a1aa !important;
    background: rgba(18, 18, 20, 0.72) !important;
}

[data-bs-theme="dark"] body.custom-background-enabled .navbar,
[data-bs-theme="dark"] body.custom-background-enabled .app-footer,
[data-bs-theme="dark"] body.custom-background-enabled .footer.fixed-bottom {
    background: rgba(3, 3, 3, var(--surface-alpha)) !important;
}


`;
}
__name(getStyleCss, "getStyleCss");
function getMainJs() {
  return `// main.js - \u9996\u9875\u9762\u7684JavaScript\u903B\u8F91

// Global variables
let vpsUpdateInterval = null;
let siteUpdateInterval = null;
let serverDataCache = {}; // Cache server data to avoid re-fetching for details
let vpsStatusCache = {}; // \u7528\u4E8E\u8DDF\u8E2AVPS\u72B6\u6001\u53D8\u5316
let lastServerRenderSignature = '';
let lastSiteRenderSignature = '';
let publicDisplaySettings = {
    showServerSection: true,
    showSiteSection: true
};
const DEFAULT_VPS_REFRESH_INTERVAL_MS = 60000; // Default to 60 seconds for VPS data if backend setting fails
const DEFAULT_SITE_REFRESH_INTERVAL_MS = 60000; // Default to 60 seconds for Site data

// ==================== \u7EDF\u4E00API\u8BF7\u6C42\u5DE5\u5177 ====================

// \u83B7\u53D6\u8BA4\u8BC1\u5934
function getAuthHeaders() {
    const token = localStorage.getItem('auth_token');
    const headers = { 'Content-Type': 'application/json' };
    if (token) {
        headers['Authorization'] = 'Bearer ' + token;
    }
    return headers;
}

// ==================== VPS\u72B6\u6001\u53D8\u5316\u68C0\u6D4B ====================

// \u68C0\u6D4BVPS\u72B6\u6001\u53D8\u5316\u5E76\u53D1\u9001\u901A\u77E5
async function checkVpsStatusChanges(allStatuses) {
    for (const data of allStatuses) {
        const serverId = data.server.id;
        const serverName = data.server.name;
        const currentStatus = determineVpsStatus(data);
        const previousStatus = vpsStatusCache[serverId];

        // \u9996\u6B21\u52A0\u8F7D\u6216\u72B6\u6001\u53D8\u5316\u65F6\u68C0\u6D4B
        if (previousStatus === undefined || previousStatus !== currentStatus) {
                        if (currentStatus === 'offline') {
                await notifyVpsOffline(serverId, serverName);
            } else if (currentStatus === 'online' && previousStatus === 'offline') {
                await notifyVpsRecovery(serverId, serverName);
            }
        }

        vpsStatusCache[serverId] = currentStatus;
    }
}

// \u5224\u65ADVPS\u72B6\u6001
function determineVpsStatus(data) {
    if (data.error) return 'error';
    if (!data.metrics) return 'unknown';

    const now = new Date();
    const lastReportTime = new Date(data.metrics.timestamp * 1000);
    const diffMinutes = (now - lastReportTime) / (1000 * 60);

    return diffMinutes <= 5 ? 'online' : 'offline';
}

// \u53D1\u9001VPS\u79BB\u7EBF\u901A\u77E5
async function notifyVpsOffline(serverId, serverName) {
    try {
        // \u4F7F\u7528\u5B8C\u6574URL
        const baseUrl = window.location.origin;
        await fetch(baseUrl + '/api/notify/offline', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ serverId, serverName })
        });
            } catch (error) {
            }
}

// \u53D1\u9001VPS\u6062\u590D\u901A\u77E5
async function notifyVpsRecovery(serverId, serverName) {
    try {
        // \u4F7F\u7528\u5B8C\u6574URL
        const baseUrl = window.location.origin;
        await fetch(baseUrl + '/api/notify/recovery', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ serverId, serverName })
        });
            } catch (error) {
            }
}

// \u7EDF\u4E00API\u8BF7\u6C42\u51FD\u6570\uFF08\u7528\u4E8E\u9700\u8981\u8BA4\u8BC1\u7684\u8BF7\u6C42\uFF09
async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: getAuthHeaders(),
        ...options
    };

    try {
        const response = await fetch(url, defaultOptions);

        // \u5904\u7406\u8BA4\u8BC1\u5931\u8D25
        if (response.status === 401) {
            localStorage.removeItem('auth_token');
            if (window.location.pathname !== '/login.html') {
                window.location.href = 'login.html';
            }
            throw new Error('\u8BA4\u8BC1\u5931\u8D25\uFF0C\u8BF7\u91CD\u65B0\u767B\u5F55');
        }

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`\u8BF7\u6C42\u5931\u8D25 (\${response.status})\`);
        }

        return await response.json();
    } catch (error) {
                throw error;
    }
}

// \u516C\u5F00API\u8BF7\u6C42\u51FD\u6570\uFF08\u7528\u4E8E\u4E0D\u9700\u8981\u8BA4\u8BC1\u7684\u8BF7\u6C42\uFF09
async function publicApiRequest(url, options = {}) {
    const defaultOptions = {
        headers: getAuthHeaders(), // \u4ECD\u7136\u53D1\u9001token\uFF08\u5982\u679C\u6709\uFF09\uFF0C\u4F46\u4E0D\u5F3A\u5236\u8981\u6C42
        ...options
    };

    try {
        const response = await fetch(url, defaultOptions);

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`\u8BF7\u6C42\u5931\u8D25 (\${response.status})\`);
        }

        return await response.json();
    } catch (error) {
                throw error;
    }
}

async function loadPublicDisplaySettings() {
    try {
        publicDisplaySettings = await publicApiRequest('/api/display-settings');
    } catch (error) {
        publicDisplaySettings = { showServerSection: true, showSiteSection: true };
    }

    const serverSection = document.getElementById('serverStatusSection');
    const siteSection = document.getElementById('siteStatusSection');
    const divider = document.getElementById('statusSectionDivider');

    if (serverSection) {
        serverSection.classList.toggle('d-none', !publicDisplaySettings.showServerSection);
    }
    if (siteSection) {
        siteSection.classList.toggle('d-none', !publicDisplaySettings.showSiteSection);
    }
    if (divider) {
        divider.classList.toggle('d-none', !publicDisplaySettings.showServerSection || !publicDisplaySettings.showSiteSection);
    }
    updatePublicDashboardCardVisibility();
}

function hidePublicSectionWhenEmpty(sectionId) {
    const section = document.getElementById(sectionId);
    if (section) {
        section.classList.add('d-none');
    }
    const serverHidden = document.getElementById('serverStatusSection')?.classList.contains('d-none');
    const siteHidden = document.getElementById('siteStatusSection')?.classList.contains('d-none');
    const divider = document.getElementById('statusSectionDivider');
    if (divider) {
        divider.classList.toggle('d-none', serverHidden || siteHidden);
    }
    updatePublicDashboardCardVisibility();
}

function updatePublicDashboardCardVisibility() {
    const dashboardCard = document.getElementById('statusDashboardCard');
    if (!dashboardCard) return;

    const serverSection = document.getElementById('serverStatusSection');
    const siteSection = document.getElementById('siteStatusSection');
    const serverHidden = !serverSection || serverSection.classList.contains('d-none');
    const siteHidden = !siteSection || siteSection.classList.contains('d-none');

    dashboardCard.classList.toggle('d-none', serverHidden && siteHidden);
}

// \u663E\u793A\u9519\u8BEF\u6D88\u606F
function showError(message, containerId = null) {
    console.error('\u9519\u8BEF:', message);
    if (containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = \`<div class="alert alert-danger">\${message}</div>\`;
        }
    }
}

// \u663E\u793A\u6210\u529F\u6D88\u606F
function showSuccess(message, containerId = null) {
        if (containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = \`<div class="alert alert-success">\${message}</div>\`;
        }
    }
}

// Function to fetch VPS refresh interval and start periodic VPS data updates
async function initializeVpsDataUpdates() {
        let vpsRefreshIntervalMs = DEFAULT_VPS_REFRESH_INTERVAL_MS;

    try {
                const data = await publicApiRequest('/api/admin/settings/vps-report-interval');
                if (data && typeof data.interval === 'number' && data.interval > 0) {
            vpsRefreshIntervalMs = data.interval * 1000; // Convert seconds to milliseconds
                    } else {
            // \u4F7F\u7528\u9ED8\u8BA4\u503C
        }
    } catch (error) {
            }

    // Clear existing interval if any
    if (vpsUpdateInterval) {
                clearInterval(vpsUpdateInterval);
    }

    // VPS\u6570\u636E\u8DDF\u968F\u540E\u53F0\u8BBE\u7F6E\u9891\u7387\u5237\u65B0
        vpsUpdateInterval = setInterval(() => {
                loadAllServerStatuses();
    }, vpsRefreshIntervalMs);

    }

// \u4F18\u5316\uFF1A\u7F51\u7AD9\u72B6\u6001\u6BCF\u5C0F\u65F6\u5237\u65B0\u4E00\u6B21
function initializeSiteDataUpdates() {
    const hourlyRefreshInterval = 60 * 60 * 1000; // 1\u5C0F\u65F6
        // \u6E05\u9664\u4EFB\u4F55\u73B0\u6709\u7684\u81EA\u52A8\u5237\u65B0\u95F4\u9694
    if (siteUpdateInterval) {
        clearInterval(siteUpdateInterval);
    }

    // \u8BBE\u7F6E\u6BCF\u5C0F\u65F6\u5237\u65B0\u4E00\u6B21
    siteUpdateInterval = setInterval(() => {
                loadAllSiteStatuses();
    }, hourlyRefreshInterval);

    }

// \u79FB\u9664\u624B\u52A8\u5237\u65B0\u6309\u94AE\u76F8\u5173\u4EE3\u7801\uFF0C\u6539\u4E3A\u81EA\u52A8\u5237\u65B0

// Execute after the page loads (only for main page)
document.addEventListener('DOMContentLoaded', async function() {
        // Check if we're on the main page by looking for the server table
    const serverTableBody = document.getElementById('serverTableBody');
    if (!serverTableBody) {
        // Not on the main page, only initialize theme
                initializeTheme();
        return;
    }

        // Initialize theme
    initializeTheme();

    // Load display preferences before drawing the public dashboard
    await loadPublicDisplaySettings();

    // Load initial data
    if (publicDisplaySettings.showServerSection) {
        loadAllServerStatuses();
    }
    if (publicDisplaySettings.showSiteSection) {
        loadAllSiteStatuses();
    }

    // Initialize periodic updates separately
        if (publicDisplaySettings.showServerSection) initializeVpsDataUpdates();
        if (publicDisplaySettings.showSiteSection) initializeSiteDataUpdates();

    // Add click event listener to the table body for row expansion
    serverTableBody.addEventListener('click', handleRowClick);

    // Check login status and update admin link
    updateAdminLink();
});

// --- Theme Management ---
const THEME_KEY = 'vps-monitor-theme';
const LIGHT_THEME = 'light';
const DARK_THEME = 'dark';

function initializeTheme() {
    const themeToggler = document.getElementById('themeToggler');
    if (!themeToggler) return;

    const storedTheme = localStorage.getItem(THEME_KEY) || LIGHT_THEME;
    applyTheme(storedTheme);

    themeToggler.addEventListener('click', () => {
        const currentTheme = document.documentElement.getAttribute('data-bs-theme');
        const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;
        applyTheme(newTheme);
        localStorage.setItem(THEME_KEY, newTheme);
    });
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-bs-theme', theme);
    const themeTogglerIcon = document.querySelector('#themeToggler i');
    if (themeTogglerIcon) {
        if (theme === DARK_THEME) {
            themeTogglerIcon.classList.remove('bi-moon-stars-fill');
            themeTogglerIcon.classList.add('bi-sun-fill');
        } else {
            themeTogglerIcon.classList.remove('bi-sun-fill');
            themeTogglerIcon.classList.add('bi-moon-stars-fill');
        }
    }
}
// --- End Theme Management ---

// Check login status and update the admin link in the navbar
async function updateAdminLink() {
    const adminLink = document.getElementById('adminAuthLink');
    if (!adminLink) return; // Exit if link not found

    try {
        const token = localStorage.getItem('auth_token');
        if (!token) {
            // Not logged in (no token)
            adminLink.innerHTML = '<i class="bi bi-person-lock me-1"></i><span class="nav-link-label">\u540E\u53F0</span>';
            adminLink.title = '\u540E\u53F0';
            adminLink.href = '/login.html';
            return;
        }

        const data = await publicApiRequest('/api/auth/status');
        if (data.authenticated) {
            // Logged in
            adminLink.innerHTML = '<i class="bi bi-speedometer2 me-1"></i><span class="nav-link-label">\u540E\u53F0</span>';
            adminLink.title = '\u540E\u53F0';
            adminLink.href = '/admin.html';
        } else {
            // Invalid token or not authenticated
            adminLink.innerHTML = '<i class="bi bi-person-lock me-1"></i><span class="nav-link-label">\u540E\u53F0</span>';
            adminLink.title = '\u540E\u53F0';
            adminLink.href = '/login.html';
            localStorage.removeItem('auth_token'); // Clean up invalid token
        }
    } catch (error) {
                // Network error, assume not logged in
        adminLink.innerHTML = '<i class="bi bi-person-lock me-1"></i><span class="nav-link-label">\u540E\u53F0</span>';
        adminLink.title = '\u540E\u53F0';
        adminLink.href = '/login.html';
    }
}


// Handle click on a server row
function handleRowClick(event) {
    const clickedRow = event.target.closest('tr.server-row');
    if (!clickedRow) return; // Not a server row

    const serverId = clickedRow.getAttribute('data-server-id');
    const detailsRow = clickedRow.nextElementSibling; // The details row is the next sibling

    if (detailsRow && detailsRow.classList.contains('server-details-row')) {
        // Toggle visibility
        detailsRow.classList.toggle('d-none');

        // If showing, populate with detailed data
        if (!detailsRow.classList.contains('d-none')) {
            populateDetailsRow(serverId, detailsRow);
        }
    }
}

// Populate the detailed row with data
function populateDetailsRow(serverId, detailsRow) {
    const serverData = serverDataCache[serverId];
    const detailsContentDiv = detailsRow.querySelector('.server-details-content');

    if (!serverData || !serverData.metrics || !detailsContentDiv) {
        detailsContentDiv.innerHTML = '<p class="text-muted">\u65E0\u8BE6\u7EC6\u6570\u636E</p>';
        return;
    }

    const metrics = serverData.metrics;

    let detailsHtml = '';

    // CPU Details
    if (metrics.cpu && metrics.cpu.load_avg) {
        detailsHtml += \`
            <div class="detail-item">
                <strong>CPU\u8D1F\u8F7D (1m, 5m, 15m):</strong> \${metrics.cpu.load_avg.join(', ')}
            </div>
        \`;
    }

    // Memory Details
    if (metrics.memory) {
        detailsHtml += \`
            <div class="detail-item">
                <strong>\u5185\u5B58:</strong>
                \u603B\u8BA1: \${formatDataSize(metrics.memory.total * 1024)}<br>
                \u5DF2\u7528: \${formatDataSize(metrics.memory.used * 1024)}<br>
                \u7A7A\u95F2: \${formatDataSize(metrics.memory.free * 1024)}
            </div>
        \`;
    }

    // Disk Details
    if (metrics.disk) {
         detailsHtml += \`
            <div class="detail-item">
                <strong>\u786C\u76D8 (/):</strong>
                \u603B\u8BA1: \${typeof metrics.disk.total === 'number' ? metrics.disk.total.toFixed(2) : '-'} GB<br>
                \u5DF2\u7528: \${typeof metrics.disk.used === 'number' ? metrics.disk.used.toFixed(2) : '-'} GB<br>
                \u7A7A\u95F2: \${typeof metrics.disk.free === 'number' ? metrics.disk.free.toFixed(2) : '-'} GB
            </div>
        \`;
    }

    // Network Totals
    if (metrics.network) {
        detailsHtml += \`
            <div class="detail-item">
                <strong>\u603B\u6D41\u91CF:</strong>
                \u4E0A\u4F20: \${formatDataSize(metrics.network.total_upload)}<br>
                \u4E0B\u8F7D: \${formatDataSize(metrics.network.total_download)}
            </div>
        \`;
    }

    detailsContentDiv.innerHTML = detailsHtml || '<p class="text-muted">\u65E0\u8BE6\u7EC6\u6570\u636E</p>';
}


// Load all server statuses
async function loadAllServerStatuses() {
    if (!publicDisplaySettings.showServerSection) return;
        try {
        // \u4F7F\u7528\u6279\u91CFAPI\u4E00\u6B21\u6027\u83B7\u53D6\u6240\u6709VPS\u72B6\u6001
        let batchData;
        try {
            batchData = await publicApiRequest('/api/status/batch');
        } catch (error) {
            // \u5982\u679C\u6279\u91CFAPI\u5931\u8D25\uFF0C\u53EF\u80FD\u662F\u6570\u636E\u5E93\u672A\u521D\u59CB\u5316\uFF0C\u5C1D\u8BD5\u521D\u59CB\u5316
                        await publicApiRequest('/api/init-db');
            batchData = await publicApiRequest('/api/status/batch');
        }

        const allStatuses = batchData.servers || [];
                const noServersAlert = document.getElementById('noServers');
        const serverTableBody = document.getElementById('serverTableBody');

        if (allStatuses.length === 0) {
            document.getElementById('serverStatusSection')?.classList.remove('d-none');
            document.getElementById('statusSectionDivider')?.classList.toggle('d-none', document.getElementById('siteStatusSection')?.classList.contains('d-none'));
            updatePublicDashboardCardVisibility();
            noServersAlert.classList.remove('d-none');
            serverTableBody.innerHTML = '<tr><td colspan="11" class="text-center empty-table-cell">\u6682\u65E0\u670D\u52A1\u5668\u6570\u636E</td></tr>';
            // Remove any existing detail rows if the server list becomes empty
            removeAllDetailRows();
            // \u540C\u65F6\u66F4\u65B0\u79FB\u52A8\u7AEF\u5361\u7247\u5BB9\u5668
            renderMobileServerCards([]);
            return;
        } else {
            document.getElementById('serverStatusSection')?.classList.remove('d-none');
            document.getElementById('statusSectionDivider')?.classList.toggle('d-none', document.getElementById('siteStatusSection')?.classList.contains('d-none'));
            updatePublicDashboardCardVisibility();
            noServersAlert.classList.add('d-none');
        }

        // Update the serverDataCache with the latest data
        allStatuses.forEach(data => {
             serverDataCache[data.server.id] = data;
        });

        // \u68C0\u6D4BVPS\u72B6\u6001\u53D8\u5316\u5E76\u53D1\u9001\u901A\u77E5
        await checkVpsStatusChanges(allStatuses);

        // 3. Render the table using DOM manipulation
        const nextServerSignature = JSON.stringify(allStatuses);
        if (nextServerSignature === lastServerRenderSignature) return;
        lastServerRenderSignature = nextServerSignature;
        renderServerTable(allStatuses);

    } catch (error) {
                const serverTableBody = document.getElementById('serverTableBody');
        serverTableBody.innerHTML = '<tr><td colspan="11" class="text-center">Failed to load server data. Please refresh the page.</td></tr>';
        removeAllDetailRows();
        // \u540C\u65F6\u66F4\u65B0\u79FB\u52A8\u7AEF\u5361\u7247\u5BB9\u5668\u663E\u793A\u9519\u8BEF\u72B6\u6001
        showToast('danger', '\u52A0\u8F7D\u670D\u52A1\u5668\u6570\u636E\u5931\u8D25\uFF0C\u8BF7\u5237\u65B0\u9875\u9762\u91CD\u8BD5');
    }
}

// Remove all existing server detail rows
function removeAllDetailRows() {
    document.querySelectorAll('.server-details-row').forEach(row => row.remove());
}


// Generate progress bar HTML
function getProgressBarHtml(percentage) {
    if (typeof percentage !== 'number' || isNaN(percentage)) return '-';
    const percent = Math.max(0, Math.min(100, percentage)); // Ensure percentage is between 0 and 100
    let bgColorClass = 'bg-light-green'; // Use custom light green for < 50%

    if (percent >= 80) {
        bgColorClass = 'bg-danger'; // Red for >= 80%
    } else if (percent >= 50) {
        bgColorClass = 'bg-warning'; // Yellow for 50% - 79%
    }

    // Use relative positioning on the container and absolute for the text, centered over the whole bar
    return \`
        <div class="progress" style="height: 25px; font-size: 0.8em; position: relative; background-color: #e9ecef;">
            <div class="progress-bar \${bgColorClass}" role="progressbar" style="width: \${percent}%;" aria-valuenow="\${percent}" aria-valuemin="0" aria-valuemax="100"></div>
            <span style="position: absolute; width: 100%; text-align: center; line-height: 25px; font-weight: bold;">
                \${percent.toFixed(1)}%
            </span>
        </div>
    \`;
}


// \u79FB\u52A8\u7AEF\u8F85\u52A9\u51FD\u6570
function getServerStatusBadge(status) {
    if (status === 'online') {
        return { class: 'bg-success', text: '\u5728\u7EBF' };
    } else if (status === 'offline') {
        return { class: 'bg-danger', text: '\u79BB\u7EBF' };
    } else if (status === 'error') {
        return { class: 'bg-warning text-dark', text: '\u9519\u8BEF' };
    } else {
        return { class: 'bg-secondary', text: '\u672A\u77E5' };
    }
}


// \u79FB\u52A8\u7AEF\u670D\u52A1\u5668\u5361\u7247\u6E32\u67D3\u51FD\u6570
function renderMobileServerCards(allStatuses) {
    const mobileContainer = document.getElementById('mobileServerContainer');
    if (!mobileContainer) return;
    if (!window.matchMedia('(max-width: 768px)').matches) {
        mobileContainer.innerHTML = '';
        return;
    }

    mobileContainer.innerHTML = '';

    if (!allStatuses || allStatuses.length === 0) {
        mobileContainer.innerHTML = \`
            <div class="text-center p-4">
                <i class="bi bi-server text-muted" style="font-size: 3rem;"></i>
                <div class="mt-3 text-muted">
                    <h6>\u6682\u65E0\u670D\u52A1\u5668\u6570\u636E</h6>
                    <small>\u8BF7\u767B\u5F55\u7BA1\u7406\u540E\u53F0\u6DFB\u52A0\u670D\u52A1\u5668</small>
                </div>
            </div>
        \`;
        return;
    }

    allStatuses.forEach(data => {
        const serverId = data.server.id;
        const serverName = data.server.name;
        const metrics = data.metrics;
        const hasError = data.error;

        const card = document.createElement('div');
        card.className = 'mobile-server-card';
        card.setAttribute('data-server-id', serverId);

        // \u786E\u5B9A\u670D\u52A1\u5668\u72B6\u6001
        let status = 'unknown';
        let lastUpdate = '\u4ECE\u672A';

        if (hasError) {
            status = 'error';
        } else if (metrics) {
            const now = new Date();
            const lastReportTime = new Date(metrics.timestamp * 1000);
            const diffMinutes = (now - lastReportTime) / (1000 * 60);

            if (diffMinutes <= 5) {
                status = 'online';
            } else {
                status = 'offline';
            }
            lastUpdate = lastReportTime.toLocaleString();
        }

        const statusInfo = getServerStatusBadge(status);

        // \u5361\u7247\u5934\u90E8
        const cardHeader = document.createElement('div');
        cardHeader.className = 'mobile-card-header';
        cardHeader.innerHTML = \`
            <div style="flex: 1;"></div>
            <h6 class="mobile-card-title text-center" style="flex: 1;">\${serverName || '\u672A\u547D\u540D\u670D\u52A1\u5668'}</h6>
            <div style="flex: 1; display: flex; justify-content: flex-end;">
                <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
            </div>
        \`;

        // \u5361\u7247\u4E3B\u4F53 - \u663E\u793A\u6240\u6709\u4FE1\u606F
        const cardBody = document.createElement('div');
        cardBody.className = 'mobile-card-body';

        // \u83B7\u53D6\u6240\u6709\u6570\u636E
        const cpuValue = metrics && metrics.cpu && typeof metrics.cpu.usage_percent === 'number' ? \`\${metrics.cpu.usage_percent.toFixed(1)}%\` : '-';
        const memoryValue = metrics && metrics.memory && typeof metrics.memory.usage_percent === 'number' ? \`\${metrics.memory.usage_percent.toFixed(1)}%\` : '-';
        const diskValue = metrics && metrics.disk && typeof metrics.disk.usage_percent === 'number' ? \`\${metrics.disk.usage_percent.toFixed(1)}%\` : '-';
        const uptimeValue = metrics && metrics.uptime ? formatUptime(metrics.uptime) : '-';
        const uploadSpeed = metrics && metrics.network ? formatNetworkSpeed(metrics.network.upload_speed) : '-';
        const downloadSpeed = metrics && metrics.network ? formatNetworkSpeed(metrics.network.download_speed) : '-';
        const totalUpload = metrics && metrics.network ? formatDataSize(metrics.network.total_upload) : '-';
        const totalDownload = metrics && metrics.network ? formatDataSize(metrics.network.total_download) : '-';

        // \u4E0A\u4F20\u901F\u5EA6 | \u4E0B\u8F7D\u901F\u5EA6
        const speedRow = document.createElement('div');
        speedRow.className = 'mobile-card-two-columns';
        speedRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u4E0A\u4F20\u901F\u5EA6</span>
                <span class="mobile-card-value">\${uploadSpeed}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u4E0B\u8F7D\u901F\u5EA6</span>
                <span class="mobile-card-value">\${downloadSpeed}</span>
            </div>
        \`;
        cardBody.appendChild(speedRow);

        // CPU | \u5185\u5B58
        const cpuMemoryRow = document.createElement('div');
        cpuMemoryRow.className = 'mobile-card-two-columns';
        cpuMemoryRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">CPU</span>
                <span class="mobile-card-value">\${cpuValue}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u5185\u5B58</span>
                <span class="mobile-card-value">\${memoryValue}</span>
            </div>
        \`;
        cardBody.appendChild(cpuMemoryRow);

        // \u786C\u76D8 | \u8FD0\u884C\u65F6\u957F
        const diskUptimeRow = document.createElement('div');
        diskUptimeRow.className = 'mobile-card-two-columns';
        diskUptimeRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u786C\u76D8</span>
                <span class="mobile-card-value">\${diskValue}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u8FD0\u884C\u65F6\u957F</span>
                <span class="mobile-card-value">\${uptimeValue}</span>
            </div>
        \`;
        cardBody.appendChild(diskUptimeRow);

        // \u603B\u4E0A\u4F20 | \u603B\u4E0B\u8F7D
        const totalRow = document.createElement('div');
        totalRow.className = 'mobile-card-two-columns';
        totalRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u603B\u4E0A\u4F20</span>
                <span class="mobile-card-value">\${totalUpload}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u603B\u4E0B\u8F7D</span>
                <span class="mobile-card-value">\${totalDownload}</span>
            </div>
        \`;
        cardBody.appendChild(totalRow);

        // \u6700\u540E\u66F4\u65B0 - \u5355\u884C
        const lastUpdateRow = document.createElement('div');
        lastUpdateRow.className = 'mobile-card-row';
        lastUpdateRow.innerHTML = \`
            <span class="mobile-card-label">\u6700\u540E\u66F4\u65B0: \${lastUpdate}</span>
        \`;
        cardBody.appendChild(lastUpdateRow);

        // \u7EC4\u88C5\u5361\u7247
        card.appendChild(cardHeader);
        card.appendChild(cardBody);

        mobileContainer.appendChild(card);
    });
}

// \u79FB\u52A8\u7AEF\u7F51\u7AD9\u5361\u7247\u6E32\u67D3\u51FD\u6570
function renderMobileSiteCards(sites) {
    const mobileContainer = document.getElementById('mobileSiteContainer');
    if (!mobileContainer) return;
    if (!window.matchMedia('(max-width: 768px)').matches) {
        mobileContainer.innerHTML = '';
        return;
    }

    mobileContainer.innerHTML = '';

    if (!sites || sites.length === 0) {
        mobileContainer.innerHTML = \`
            <div class="text-center p-4">
                <i class="bi bi-globe text-muted" style="font-size: 3rem;"></i>
                <div class="mt-3 text-muted">
                    <h6>\u6682\u65E0\u76D1\u63A7\u7F51\u7AD9\u6570\u636E</h6>
                    <small>\u8BF7\u767B\u5F55\u7BA1\u7406\u540E\u53F0\u6DFB\u52A0\u76D1\u63A7\u7F51\u7AD9</small>
                </div>
            </div>
        \`;
        return;
    }

    sites.forEach(site => {
        const card = document.createElement('div');
        card.className = 'mobile-site-card';

        const statusInfo = getSiteStatusBadge(site.last_status);
        const lastCheckTime = site.last_checked ? new Date(site.last_checked * 1000).toLocaleString() : '\u4ECE\u672A';
        const responseTime = site.last_response_time_ms !== null ? \`\${site.last_response_time_ms} ms\` : '-';

        // \u5361\u7247\u5934\u90E8
        const cardHeader = document.createElement('div');
        cardHeader.className = 'mobile-card-header';
        cardHeader.innerHTML = \`
            <div style="flex: 1;"></div>
            <h6 class="mobile-card-title text-center" style="flex: 1;">\${site.name || '\u672A\u547D\u540D\u7F51\u7AD9'}</h6>
            <div style="flex: 1; display: flex; justify-content: flex-end;">
                <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
            </div>
        \`;

        // \u5361\u7247\u4E3B\u4F53
        const cardBody = document.createElement('div');
        cardBody.className = 'mobile-card-body';

        // \u7F51\u7AD9\u4FE1\u606F - \u4E24\u5217\u5E03\u5C40
        const statusCode = site.last_status_code || '-';

        // \u72B6\u6001\u7801 | \u54CD\u5E94\u65F6\u95F4
        const statusResponseRow = document.createElement('div');
        statusResponseRow.className = 'mobile-card-two-columns';
        statusResponseRow.innerHTML = \`
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u72B6\u6001\u7801</span>
                <span class="mobile-card-value">\${statusCode}</span>
            </div>
            <div class="mobile-card-column-item">
                <span class="mobile-card-label">\u54CD\u5E94\u65F6\u95F4</span>
                <span class="mobile-card-value">\${responseTime}</span>
            </div>
        \`;
        cardBody.appendChild(statusResponseRow);

        // \u6700\u540E\u68C0\u67E5 - \u5355\u884C
        const lastCheckRow = document.createElement('div');
        lastCheckRow.className = 'mobile-card-row';
        lastCheckRow.innerHTML = \`
            <span class="mobile-card-label">\u6700\u540E\u68C0\u67E5: \${lastCheckTime}</span>
        \`;
        cardBody.appendChild(lastCheckRow);

        // 24\u5C0F\u65F6\u5386\u53F2\u8BB0\u5F55 - \u59CB\u7EC8\u663E\u793A\uFF0C\u5373\u4F7F\u6CA1\u6709\u6570\u636E
        const historyContainer = document.createElement('div');
        historyContainer.className = 'mobile-history-container';
        historyContainer.innerHTML = \`
            <div class="mobile-history-label">24\u5C0F\u65F6\u8BB0\u5F55</div>
            <div class="history-bar-container"></div>
        \`;
        cardBody.appendChild(historyContainer);

        // \u4F7F\u7528\u7EDF\u4E00\u7684\u5386\u53F2\u8BB0\u5F55\u6E32\u67D3\u51FD\u6570
        const historyBarContainer = historyContainer.querySelector('.history-bar-container');
        renderSiteHistoryBar(historyBarContainer, site.history || []);

        // \u7EC4\u88C5\u5361\u7247
        card.appendChild(cardHeader);
        card.appendChild(cardBody);

        mobileContainer.appendChild(card);
    });
}





// Render the server table using DOM manipulation
function renderServerTable(allStatuses) {
    const tableBody = document.getElementById('serverTableBody');
    const detailsTemplate = document.getElementById('serverDetailsTemplate');

    // 1. Store IDs of currently expanded servers
    const expandedServerIds = new Set();
    // Iterate over main server rows to find their expanded detail rows
    tableBody.querySelectorAll('tr.server-row').forEach(mainRow => {
        const detailRow = mainRow.nextElementSibling;
        if (detailRow && detailRow.classList.contains('server-details-row') && !detailRow.classList.contains('d-none')) {
            const serverId = mainRow.getAttribute('data-server-id');
            if (serverId) {
                expandedServerIds.add(serverId);
            }
        }
    });

    tableBody.innerHTML = ''; // Clear existing rows

    allStatuses.forEach(data => {
        const serverId = data.server.id;
        const serverName = data.server.name;
        const metrics = data.metrics;
        const hasError = data.error;

        let statusBadge = '<span class="badge bg-secondary">\u672A\u77E5</span>';
        let cpuHtml = '-';
        let memoryHtml = '-';
        let diskHtml = '-';
        let uploadSpeed = '-';
        let downloadSpeed = '-';
        let totalUpload = '-';
        let totalDownload = '-';
        let uptime = '-';
        let lastUpdate = '-';

        if (hasError) {
            statusBadge = '<span class="badge bg-warning text-dark">\u9519\u8BEF</span>';
        } else if (metrics) {
            const now = new Date();
            const lastReportTime = new Date(metrics.timestamp * 1000);
            const diffMinutes = (now - lastReportTime) / (1000 * 60);

            if (diffMinutes <= 5) { // Considered online within 5 minutes
                statusBadge = '<span class="badge bg-success">\u5728\u7EBF</span>';
            } else {
                statusBadge = '<span class="badge bg-danger">\u79BB\u7EBF</span>';
            }

            cpuHtml = getProgressBarHtml(metrics.cpu.usage_percent);
            memoryHtml = getProgressBarHtml(metrics.memory.usage_percent);
            diskHtml = getProgressBarHtml(metrics.disk.usage_percent);
            uploadSpeed = formatNetworkSpeed(metrics.network.upload_speed);
            downloadSpeed = formatNetworkSpeed(metrics.network.download_speed);
            totalUpload = formatDataSize(metrics.network.total_upload);
            totalDownload = formatDataSize(metrics.network.total_download);
            uptime = metrics.uptime ? formatUptime(metrics.uptime) : '-';
            lastUpdate = lastReportTime.toLocaleString();
        }

        // Create the main row
        const mainRow = document.createElement('tr');
        mainRow.classList.add('server-row');
        mainRow.setAttribute('data-server-id', serverId);
        mainRow.innerHTML = \`
            <td>\${serverName}</td>
            <td>\${statusBadge}</td>
            <td>\${cpuHtml}</td>
            <td>\${memoryHtml}</td>
            <td>\${diskHtml}</td>
            <td><span style="color: #000;">\${uploadSpeed}</span></td>
            <td><span style="color: #000;">\${downloadSpeed}</span></td>
            <td><span style="color: #000;">\${totalUpload}</span></td>
            <td><span style="color: #000;">\${totalDownload}</span></td>
            <td><span style="color: #000;">\${uptime}</span></td>
            <td><span style="color: #000;">\${lastUpdate}</span></td>
        \`;

        // Clone the details row template
        const detailsRowElement = detailsTemplate.content.cloneNode(true).querySelector('tr');
        // The template has d-none by default. We will remove it if needed.
        // Set a unique attribute for easier selection if needed, though direct reference is used here.
        // detailsRowElement.setAttribute('data-detail-for', serverId);

        tableBody.appendChild(mainRow);
        tableBody.appendChild(detailsRowElement);

        // 2. If this server was previously expanded, re-expand it and populate its details
        if (expandedServerIds.has(serverId)) {
            detailsRowElement.classList.remove('d-none');
            populateDetailsRow(serverId, detailsRowElement); // Populate content
        }
    });

    // 3. \u540C\u65F6\u6E32\u67D3\u79FB\u52A8\u7AEF\u5361\u7247
    renderMobileServerCards(allStatuses);
}


// Format network speed
function formatNetworkSpeed(bytesPerSecond) {
    if (typeof bytesPerSecond !== 'number' || isNaN(bytesPerSecond)) return '-';
    if (bytesPerSecond < 1024) {
        return \`\${bytesPerSecond.toFixed(1)} B/s\`;
    } else if (bytesPerSecond < 1024 * 1024) {
        return \`\${(bytesPerSecond / 1024).toFixed(1)} KB/s\`;
    } else if (bytesPerSecond < 1024 * 1024 * 1024) {
        return \`\${(bytesPerSecond / (1024 * 1024)).toFixed(1)} MB/s\`;
    } else {
        return \`\${(bytesPerSecond / (1024 * 1024 * 1024)).toFixed(1)} GB/s\`;
    }
}

// Format data size
function formatDataSize(bytes) {
    if (typeof bytes !== 'number' || isNaN(bytes)) return '-';
    if (bytes < 1024) {
        return \`\${bytes.toFixed(1)} B\`;
    } else if (bytes < 1024 * 1024) {
        return \`\${(bytes / 1024).toFixed(1)} KB\`;
    } else if (bytes < 1024 * 1024 * 1024) {
        return \`\${(bytes / (1024 * 1024)).toFixed(1)} MB\`;
    } else if (bytes < 1024 * 1024 * 1024 * 1024) {
        return \`\${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB\`;
    } else {
        return \`\${(bytes / (1024 * 1024 * 1024 * 1024)).toFixed(1)} TB\`;
    }
}

// Format uptime from seconds to a human-readable string
function formatUptime(totalSeconds) {
    if (typeof totalSeconds !== 'number' || isNaN(totalSeconds) || totalSeconds < 0) {
        return '-';
    }

    const days = Math.floor(totalSeconds / (3600 * 24));
    totalSeconds %= (3600 * 24);
    const hours = Math.floor(totalSeconds / 3600);
    totalSeconds %= 3600;
    const minutes = Math.floor(totalSeconds / 60);

    let uptimeString = '';
    if (days > 0) {
        uptimeString += \`\${days}\u5929 \`;
    }
    if (hours > 0) {
        uptimeString += \`\${hours}\u5C0F\u65F6 \`;
    }
    if (minutes > 0 || (days === 0 && hours === 0)) { // Show minutes if it's the only unit or if other units are zero
        uptimeString += \`\${minutes}\u5206\u949F\`;
    }

    return uptimeString.trim() || '0\u5206\u949F'; // Default to 0 minutes if string is empty
}


// --- Website Status Functions ---

// Load all website statuses
async function loadAllSiteStatuses() {
    if (!publicDisplaySettings.showSiteSection) return;
    try {
        let data;
        try {
            data = await publicApiRequest('/api/sites/status');
        } catch (error) {
            // \u5982\u679C\u83B7\u53D6\u7F51\u7AD9\u72B6\u6001\u5931\u8D25\uFF0C\u53EF\u80FD\u662F\u6570\u636E\u5E93\u672A\u521D\u59CB\u5316\uFF0C\u5C1D\u8BD5\u521D\u59CB\u5316
                        await publicApiRequest('/api/init-db');
            data = await publicApiRequest('/api/sites/status');
        }
        const sites = data.sites || [];

        const noSitesAlert = document.getElementById('noSites');
        const siteStatusTableBody = document.getElementById('siteStatusTableBody');

        if (sites.length === 0) {
            document.getElementById('siteStatusSection')?.classList.remove('d-none');
            document.getElementById('statusSectionDivider')?.classList.toggle('d-none', document.getElementById('serverStatusSection')?.classList.contains('d-none'));
            updatePublicDashboardCardVisibility();
            noSitesAlert.classList.remove('d-none');
            siteStatusTableBody.innerHTML = '<tr><td colspan="6" class="text-center empty-table-cell">\u6682\u65E0\u76D1\u63A7\u7F51\u7AD9\u6570\u636E</td></tr>'; // Colspan updated
            // \u540C\u65F6\u66F4\u65B0\u79FB\u52A8\u7AEF\u5361\u7247\u5BB9\u5668
            renderMobileSiteCards([]);
            return;
        } else {
            document.getElementById('siteStatusSection')?.classList.remove('d-none');
            document.getElementById('statusSectionDivider')?.classList.toggle('d-none', document.getElementById('serverStatusSection')?.classList.contains('d-none'));
            updatePublicDashboardCardVisibility();
            noSitesAlert.classList.add('d-none');
        }

        const nextSiteSignature = JSON.stringify(sites);
        if (nextSiteSignature === lastSiteRenderSignature) return;
        lastSiteRenderSignature = nextSiteSignature;
        renderSiteStatusTable(sites);

    } catch (error) {
                const siteStatusTableBody = document.getElementById('siteStatusTableBody');
        siteStatusTableBody.innerHTML = '<tr><td colspan="6" class="text-center ">Failed to load website status data. Please refresh the page.</td></tr>'; // Colspan updated
        // \u663E\u793A\u9519\u8BEF\u901A\u77E5
        showToast('danger', '\u52A0\u8F7D\u7F51\u7AD9\u6570\u636E\u5931\u8D25\uFF0C\u8BF7\u5237\u65B0\u9875\u9762\u91CD\u8BD5');
    }
}

// Render the website status table
async function renderSiteStatusTable(sites) {
    const tableBody = document.getElementById('siteStatusTableBody');
    tableBody.innerHTML = ''; // Clear existing rows

    for (const site of sites) {
        const row = document.createElement('tr');
        const statusInfo = getSiteStatusBadge(site.last_status);
        const lastCheckTime = site.last_checked ? new Date(site.last_checked * 1000).toLocaleString() : '\u4ECE\u672A';
        const responseTime = site.last_response_time_ms !== null ? \`\${site.last_response_time_ms} ms\` : '-';

        const historyCell = document.createElement('td');
        const historyContainer = document.createElement('div');
        historyContainer.className = 'history-bar-container';
        historyCell.appendChild(historyContainer);

        row.innerHTML = \`
            <td>\${site.name || '-'}</td>
            <td><span class="badge \${statusInfo.class}">\${statusInfo.text}</span></td>
            <td>\${site.last_status_code || '-'}</td>
            <td>\${responseTime}</td>
            <td>\${lastCheckTime}</td>
        \`;
        row.appendChild(historyCell);
        tableBody.appendChild(row);

        // \u76F4\u63A5\u4F7F\u7528\u7AD9\u70B9\u7684\u5386\u53F2\u6570\u636E\u6E32\u67D3\u5386\u53F2\u6761
        renderSiteHistoryBar(historyContainer, site.history || []);
    }

    // \u540C\u65F6\u6E32\u67D3\u79FB\u52A8\u7AEF\u5361\u7247
    renderMobileSiteCards(sites);
}

// Render 24h history bar for a site (unified function for PC and mobile)
function renderSiteHistoryBar(containerElement, history) {
    let historyHtml = '';
    const now = new Date();

    for (let i = 0; i < 24; i++) {
        const slotTime = new Date(now);
        slotTime.setHours(now.getHours() - i);
        const slotStart = new Date(slotTime);
        slotStart.setMinutes(0, 0, 0);
        const slotEnd = new Date(slotTime);
        slotEnd.setMinutes(59, 59, 999);

        const slotStartTimestamp = Math.floor(slotStart.getTime() / 1000);
        const slotEndTimestamp = Math.floor(slotEnd.getTime() / 1000);

        const recordForHour = history?.find(
            r => r.timestamp >= slotStartTimestamp && r.timestamp <= slotEndTimestamp
        );

        let barClass = 'history-bar-pending';
        let titleText = \`\${String(slotStart.getHours()).padStart(2, '0')}:00 - \${String((slotStart.getHours() + 1) % 24).padStart(2, '0')}:00: \u65E0\u8BB0\u5F55\`;

        if (recordForHour) {
            if (recordForHour.status === 'UP') {
                barClass = 'history-bar-up';
            } else if (['TIMEOUT', 'ERROR'].includes(recordForHour.status)) {
                barClass = 'history-bar-warning';
            } else if (recordForHour.status === 'DOWN') {
                barClass = 'history-bar-down';
            }
            const recordDate = new Date(recordForHour.timestamp * 1000);
            titleText = \`\${recordDate.toLocaleString()}: \${recordForHour.status} (\${recordForHour.status_code || 'N/A'}), \${recordForHour.response_time_ms || '-'}ms\`;
        }

        historyHtml += \`<div class="history-bar \${barClass}" data-history-title="\${escapeHtml(titleText)}" aria-label="\${escapeHtml(titleText)}"></div>\`;
    }

    containerElement.innerHTML = historyHtml;
    attachHistoryTooltipHandlers(containerElement);
}

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function getHistoryTooltip() {
    let tooltip = document.getElementById('historyTooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.id = 'historyTooltip';
        tooltip.className = 'history-tooltip';
        document.body.appendChild(tooltip);
    }
    return tooltip;
}

function positionHistoryTooltip(target, tooltip) {
    const rect = target.getBoundingClientRect();
    tooltip.style.left = (rect.left + rect.width / 2) + 'px';
    tooltip.style.top = rect.top + 'px';
}

function attachHistoryTooltipHandlers(containerElement) {
    containerElement.querySelectorAll('.history-bar').forEach(bar => {
        bar.addEventListener('mouseenter', () => {
            const tooltip = getHistoryTooltip();
            tooltip.textContent = bar.dataset.historyTitle || '';
            positionHistoryTooltip(bar, tooltip);
            tooltip.classList.add('show');
        });
        bar.addEventListener('mousemove', () => {
            const tooltip = getHistoryTooltip();
            positionHistoryTooltip(bar, tooltip);
        });
        bar.addEventListener('mouseleave', () => {
            const tooltip = getHistoryTooltip();
            tooltip.classList.remove('show');
        });
    });
}


// Get website status badge class and text (copied from admin.js for reuse)
function getSiteStatusBadge(status) {
    switch (status) {
        case 'UP': return { class: 'bg-success', text: '\u6B63\u5E38' };
        case 'DOWN': return { class: 'bg-danger', text: '\u6545\u969C' };
        case 'TIMEOUT': return { class: 'bg-warning text-dark', text: '\u8D85\u65F6' };
        case 'ERROR': return { class: 'bg-danger', text: '\u9519\u8BEF' };
        case 'PENDING': return { class: 'bg-secondary', text: '\u5F85\u68C0\u6D4B' };
        default: return { class: 'bg-secondary', text: '\u672A\u77E5' };
    }
}

// ==================== \u5168\u5C40\u80CC\u666F\u8BBE\u7F6E\u529F\u80FD ====================

// \u5168\u5C40\u80CC\u666F\u8BBE\u7F6E\u52A0\u8F7D\u51FD\u6570
async function loadGlobalBackgroundSettings() {
    try {
        // \u68C0\u67E5localStorage\u7F13\u5B58\uFF08\u65E0\u75D5\u6A21\u5F0F\u517C\u5BB9\uFF09
        const cacheKey = 'background-settings-cache';
        let cached = null;
        let settings = null;

        try {
            cached = localStorage.getItem(cacheKey);
        } catch (storageError) {
                    }

        if (cached) {
            try {
                const cachedData = JSON.parse(cached);
                const now = Date.now();
                const cacheAge = now - cachedData.timestamp;
                const CACHE_DURATION = 5 * 60 * 1000; // 5\u5206\u949F\u7F13\u5B58

                if (cacheAge < CACHE_DURATION) {
                    settings = cachedData;
                                    }
            } catch (parseError) {
                            }
        }

        // \u7F13\u5B58\u8FC7\u671F\u6216\u4E0D\u5B58\u5728\uFF0C\u4ECEAPI\u83B7\u53D6
        if (!settings) {
            try {
                const response = await fetch('/api/background-settings');
                if (response.ok) {
                    const apiSettings = await response.json();
                    settings = {
                        enabled: apiSettings.enabled,
                        url: apiSettings.url,
                        opacity: apiSettings.opacity,
                        timestamp: Date.now()
                    };

                    // \u5C1D\u8BD5\u66F4\u65B0\u7F13\u5B58\uFF08\u65E0\u75D5\u6A21\u5F0F\u53EF\u80FD\u5931\u8D25\uFF0C\u4F46\u4E0D\u5F71\u54CD\u529F\u80FD\uFF09
                    try {
                        localStorage.setItem(cacheKey, JSON.stringify(settings));
                                            } catch (storageError) {
                                            }
                } else {
                                        settings = { enabled: false, url: '', opacity: 80 };
                }
            } catch (error) {
                                settings = { enabled: false, url: '', opacity: 80 };
            }
        }

        // \u5E94\u7528\u80CC\u666F\u8BBE\u7F6E
        applyGlobalBackgroundSettings(settings.enabled, settings.url, settings.opacity);

    } catch (error) {
            }
}

// \u5E94\u7528\u5168\u5C40\u80CC\u666F\u8BBE\u7F6E
function applyGlobalBackgroundSettings(enabled, url, opacity) {
    const body = document.body;

    if (enabled && url) {
        // \u9A8C\u8BC1URL\u683C\u5F0F
        if (!url.startsWith('https://')) {
                        return;
        }

        // \u9884\u52A0\u8F7D\u56FE\u7247\uFF0C\u786E\u4FDD\u52A0\u8F7D\u6210\u529F
        const img = new Image();
        img.onload = function() {
            // \u56FE\u7247\u52A0\u8F7D\u6210\u529F\uFF0C\u5E94\u7528\u80CC\u666F
            body.style.setProperty('--custom-background-url', \`url(\${url})\`);
            body.style.setProperty('--page-opacity', opacity / 100);
            body.classList.add('custom-background-enabled');



                    };
        img.onerror = function() {
            // \u56FE\u7247\u52A0\u8F7D\u5931\u8D25\uFF0C\u4E0D\u5E94\u7528\u80CC\u666F
            body.classList.remove('custom-background-enabled');
            body.classList.remove('low-contrast', 'medium-contrast', 'high-contrast');
        };
        img.src = url;
    } else {
        // \u79FB\u9664\u80CC\u666F\u8BBE\u7F6E
        body.style.removeProperty('--custom-background-url');
        body.style.removeProperty('--page-opacity');
        body.classList.remove('custom-background-enabled');
            }
}



// \u9875\u9762\u52A0\u8F7D\u65F6\u521D\u59CB\u5316\u80CC\u666F\u8BBE\u7F6E
document.addEventListener('DOMContentLoaded', function() {
    loadGlobalBackgroundSettings();
});

// \u76D1\u542Cstorage\u4E8B\u4EF6\uFF0C\u5B9E\u73B0\u8DE8\u9875\u9762\u8BBE\u7F6E\u540C\u6B65
window.addEventListener('storage', function(e) {
    if (e.key === 'background-settings-cache' && e.newValue) {
        try {
            const newSettings = JSON.parse(e.newValue);
            applyGlobalBackgroundSettings(newSettings.enabled, newSettings.url, newSettings.opacity);
                    } catch (error) {
                    }
    }
});
`;
}
__name(getMainJs, "getMainJs");
function getLoginJs() {
  return `// login.js - \u767B\u5F55\u9875\u9762\u7684JavaScript\u903B\u8F91

// ==================== \u7EDF\u4E00API\u8BF7\u6C42\u5DE5\u5177 ====================
// \u6CE8\u610F\uFF1A\u6B64\u5904\u7684apiRequest\u51FD\u6570\u5DF2\u79FB\u81F3\u4E3B\u8981\u4F4D\u7F6E\uFF0C\u907F\u514D\u91CD\u590D\u5B9A\u4E49

// --- Theme Management (copied from main.js) ---
const THEME_KEY = 'vps-monitor-theme';
const LIGHT_THEME = 'light';
const DARK_THEME = 'dark';

function initializeTheme() {
    const themeToggler = document.getElementById('themeToggler');
    if (!themeToggler) return;

    const storedTheme = localStorage.getItem(THEME_KEY) || LIGHT_THEME;
    applyTheme(storedTheme);

    themeToggler.addEventListener('click', () => {
        const currentTheme = document.documentElement.getAttribute('data-bs-theme');
        const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;
        applyTheme(newTheme);
        localStorage.setItem(THEME_KEY, newTheme);
    });
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-bs-theme', theme);
    const themeTogglerIcon = document.querySelector('#themeToggler i');
    if (themeTogglerIcon) {
        if (theme === DARK_THEME) {
            themeTogglerIcon.classList.remove('bi-moon-stars-fill');
            themeTogglerIcon.classList.add('bi-sun-fill');
        } else {
            themeTogglerIcon.classList.remove('bi-sun-fill');
            themeTogglerIcon.classList.add('bi-moon-stars-fill');
        }
    }
}
// --- End Theme Management ---


// \u9875\u9762\u52A0\u8F7D\u5B8C\u6210\u540E\u6267\u884C
document.addEventListener('DOMContentLoaded', function() {
    // Initialize theme
    initializeTheme();

    // \u83B7\u53D6\u767B\u5F55\u8868\u5355\u5143\u7D20
    const loginForm = document.getElementById('loginForm');
    const loginAlert = document.getElementById('loginAlert');

    // \u6DFB\u52A0\u8868\u5355\u63D0\u4EA4\u4E8B\u4EF6\u76D1\u542C
    loginForm.addEventListener('submit', function(e) {
        e.preventDefault();

        // \u83B7\u53D6\u7528\u6237\u8F93\u5165
        const username = document.getElementById('username').value.trim();
        const password = document.getElementById('password').value.trim();

        // \u9A8C\u8BC1\u8F93\u5165
        if (!username || !password) {
            showToast('warning', '\u8BF7\u8F93\u5165\u7528\u6237\u540D\u548C\u5BC6\u7801');
            return;
        }

        // \u6267\u884C\u767B\u5F55
        login(username, password);
    });

    // \u52A0\u8F7D\u9ED8\u8BA4\u51ED\u636E\u4FE1\u606F
    loadDefaultCredentials();

    // \u68C0\u67E5\u662F\u5426\u5DF2\u767B\u5F55
    checkLoginStatus();
});

// ==================== \u7EDF\u4E00API\u8BF7\u6C42\u5DE5\u5177 ====================

// \u83B7\u53D6\u8BA4\u8BC1\u5934
function getAuthHeaders() {
    const token = localStorage.getItem('auth_token');
    const headers = { 'Content-Type': 'application/json' };
    if (token) {
        headers['Authorization'] = 'Bearer ' + token;
    }
    return headers;
}

// \u7EDF\u4E00API\u8BF7\u6C42\u51FD\u6570
async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: getAuthHeaders(),
        ...options
    };

    try {
        const response = await fetch(url, defaultOptions);

        // \u5904\u7406\u8BA4\u8BC1\u5931\u8D25
        if (response.status === 401) {
            localStorage.removeItem('auth_token');
            if (window.location.pathname !== '/login.html') {
                window.location.href = 'login.html';
            }
            throw new Error('\u8BA4\u8BC1\u5931\u8D25\uFF0C\u8BF7\u91CD\u65B0\u767B\u5F55');
        }

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`\u8BF7\u6C42\u5931\u8D25 (\${response.status})\`);
        }

        return await response.json();
    } catch (error) {
                throw error;
    }
}

// \u52A0\u8F7D\u9ED8\u8BA4\u51ED\u636E\u4FE1\u606F\uFF08\u672C\u5730\u663E\u793A\uFF0C\u65E0\u9700API\u8C03\u7528\uFF09
function loadDefaultCredentials() {
    const credentialsInfo = document.getElementById('defaultCredentialsInfo');
    if (credentialsInfo) {
        credentialsInfo.innerHTML = '\u9ED8\u8BA4\u8D26\u53F7\u5BC6\u7801: <strong>admin</strong> / <strong>monitor2025!</strong><br><small class="text-danger fw-bold">\u5EFA\u8BAE\u9996\u6B21\u767B\u5F55\u540E\u4FEE\u6539\u5BC6\u7801</small>';
    }
}

// \u68C0\u67E5\u767B\u5F55\u72B6\u6001
async function checkLoginStatus() {
    try {
        // \u4ECElocalStorage\u83B7\u53D6token
        const token = localStorage.getItem('auth_token');
        if (!token) {
            return;
        }

        const data = await apiRequest('/api/auth/status');

        if (data.authenticated) {
            // \u5DF2\u767B\u5F55\uFF0C\u91CD\u5B9A\u5411\u5230\u7BA1\u7406\u540E\u53F0
            window.location.href = 'admin.html';
        }
    } catch (error) {
            }
}

// \u767B\u5F55\u51FD\u6570
async function login(username, password) {
    try {
        // \u663E\u793A\u52A0\u8F7D\u72B6\u6001
        const loginForm = document.getElementById('loginForm');
        const submitBtn = loginForm.querySelector('button[type="submit"]');
        const originalBtnText = submitBtn.innerHTML;
        submitBtn.disabled = true;
        submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> \u767B\u5F55\u4E2D...';

        // \u53D1\u9001\u767B\u5F55\u8BF7\u6C42\uFF08\u4E0D\u9700\u8981\u8BA4\u8BC1\u5934\uFF09
        const response = await fetch('/api/auth/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`\u767B\u5F55\u5931\u8D25 (\${response.status})\`);
        }

        const data = await response.json();

        // \u6062\u590D\u6309\u94AE\u72B6\u6001
        submitBtn.disabled = false;
        submitBtn.innerHTML = originalBtnText;

        // \u4FDD\u5B58token\u5230localStorage
        localStorage.setItem('auth_token', data.token);

        // \u76F4\u63A5\u8DF3\u8F6C\u5230\u7BA1\u7406\u540E\u53F0
        window.location.href = 'admin.html';

    } catch (error) {
                // \u6062\u590D\u6309\u94AE\u72B6\u6001
        const loginForm = document.getElementById('loginForm');
        const submitBtn = loginForm.querySelector('button[type="submit"]');
        submitBtn.disabled = false;
        submitBtn.innerHTML = '\u767B\u5F55';

        showToast('danger', error.message || '\u767B\u5F55\u8BF7\u6C42\u5931\u8D25\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5');
    }
}



// ==================== \u5168\u5C40\u80CC\u666F\u8BBE\u7F6E\u529F\u80FD ====================

// \u5168\u5C40\u80CC\u666F\u8BBE\u7F6E\u52A0\u8F7D\u51FD\u6570\uFF08\u767B\u5F55\u9875\u9762\u7248\u672C\uFF09
async function loadGlobalBackgroundSettings() {
    try {
        // \u68C0\u67E5localStorage\u7F13\u5B58\uFF08\u65E0\u75D5\u6A21\u5F0F\u517C\u5BB9\uFF09
        const cacheKey = 'background-settings-cache';
        let cached = null;
        let settings = null;

        try {
            cached = localStorage.getItem(cacheKey);
        } catch (storageError) {
                    }

        if (cached) {
            try {
                const cachedData = JSON.parse(cached);
                const now = Date.now();
                const cacheAge = now - cachedData.timestamp;
                const CACHE_DURATION = 5 * 60 * 1000; // 5\u5206\u949F\u7F13\u5B58

                if (cacheAge < CACHE_DURATION) {
                    settings = cachedData;
                                    }
            } catch (parseError) {
                            }
        }

        // \u7F13\u5B58\u8FC7\u671F\u6216\u4E0D\u5B58\u5728\uFF0C\u4ECEAPI\u83B7\u53D6
        if (!settings) {
            try {
                const response = await fetch('/api/background-settings');
                if (response.ok) {
                    const apiSettings = await response.json();
                    settings = {
                        enabled: apiSettings.enabled,
                        url: apiSettings.url,
                        opacity: apiSettings.opacity,
                        timestamp: Date.now()
                    };

                    // \u5C1D\u8BD5\u66F4\u65B0\u7F13\u5B58\uFF08\u65E0\u75D5\u6A21\u5F0F\u53EF\u80FD\u5931\u8D25\uFF0C\u4F46\u4E0D\u5F71\u54CD\u529F\u80FD\uFF09
                    try {
                        localStorage.setItem(cacheKey, JSON.stringify(settings));
                                            } catch (storageError) {
                                            }
                } else {
                                        settings = { enabled: false, url: '', opacity: 80 };
                }
            } catch (error) {
                                settings = { enabled: false, url: '', opacity: 80 };
            }
        }

        // \u5E94\u7528\u80CC\u666F\u8BBE\u7F6E
        applyGlobalBackgroundSettings(settings.enabled, settings.url, settings.opacity);

    } catch (error) {
            }
}

// \u5E94\u7528\u5168\u5C40\u80CC\u666F\u8BBE\u7F6E
function applyGlobalBackgroundSettings(enabled, url, opacity) {
    const body = document.body;

    if (enabled && url) {
        // \u9A8C\u8BC1URL\u683C\u5F0F
        if (!url.startsWith('https://')) {
                        return;
        }

        // \u9884\u52A0\u8F7D\u56FE\u7247\uFF0C\u786E\u4FDD\u52A0\u8F7D\u6210\u529F
        const img = new Image();
        img.onload = function() {
            // \u56FE\u7247\u52A0\u8F7D\u6210\u529F\uFF0C\u5E94\u7528\u80CC\u666F
            body.style.setProperty('--custom-background-url', \`url(\${url})\`);
            body.style.setProperty('--page-opacity', opacity / 100);
            body.classList.add('custom-background-enabled');



                    };
        img.onerror = function() {
            // \u56FE\u7247\u52A0\u8F7D\u5931\u8D25\uFF0C\u4E0D\u5E94\u7528\u80CC\u666F
            body.classList.remove('custom-background-enabled');
        };
        img.src = url;
    } else {
        // \u79FB\u9664\u80CC\u666F\u8BBE\u7F6E
        body.style.removeProperty('--custom-background-url');
        body.style.removeProperty('--page-opacity');
        body.classList.remove('custom-background-enabled');
            }
}



// \u9875\u9762\u52A0\u8F7D\u65F6\u521D\u59CB\u5316\u80CC\u666F\u8BBE\u7F6E
document.addEventListener('DOMContentLoaded', function() {
    loadGlobalBackgroundSettings();
});

// \u76D1\u542Cstorage\u4E8B\u4EF6\uFF0C\u5B9E\u73B0\u8DE8\u9875\u9762\u8BBE\u7F6E\u540C\u6B65
window.addEventListener('storage', function(e) {
    if (e.key === 'background-settings-cache' && e.newValue) {
        try {
            const newSettings = JSON.parse(e.newValue);
            applyGlobalBackgroundSettings(newSettings.enabled, newSettings.url, newSettings.opacity);
                    } catch (error) {
                    }
    }
});
`;
}
__name(getLoginJs, "getLoginJs");
function getAdminJs() {
  return `// admin.js - \u7BA1\u7406\u540E\u53F0\u7684JavaScript\u903B\u8F91

// ==================== \u7EDF\u4E00API\u8BF7\u6C42\u5DE5\u5177 ====================

// \u83B7\u53D6\u8BA4\u8BC1\u5934
function getAuthHeaders() {
    const token = localStorage.getItem('auth_token');
    const headers = { 'Content-Type': 'application/json' };
    if (token) {
        headers['Authorization'] = 'Bearer ' + token;
    }
    return headers;
}

// \u7EDF\u4E00API\u8BF7\u6C42\u51FD\u6570
async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: getAuthHeaders(),
        ...options
    };

    try {
        const response = await fetch(url, defaultOptions);

        // \u5904\u7406\u8BA4\u8BC1\u5931\u8D25
        if (response.status === 401) {
            localStorage.removeItem('auth_token');
            window.location.href = 'login.html';
            throw new Error('\u8BA4\u8BC1\u5931\u8D25\uFF0C\u8BF7\u91CD\u65B0\u767B\u5F55');
        }

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.message || \`\u8BF7\u6C42\u5931\u8D25 (\${response.status})\`);
        }

        return await response.json();
    } catch (error) {
                throw error;
    }
}

// Global variables for VPS data updates
let vpsUpdateInterval = null;
const DEFAULT_VPS_REFRESH_INTERVAL_MS = 60000; // Default to 60 seconds for VPS data if backend setting fails

// Function to fetch VPS refresh interval and start periodic VPS data updates
async function initializeVpsDataUpdates() {
        let vpsRefreshIntervalMs = DEFAULT_VPS_REFRESH_INTERVAL_MS;

    try {
                const data = await apiRequest('/api/admin/settings/vps-report-interval');
                if (data && typeof data.interval === 'number' && data.interval > 0) {
            vpsRefreshIntervalMs = data.interval * 1000; // Convert seconds to milliseconds
                    } else {
            // \u4F7F\u7528\u9ED8\u8BA4\u503C
        }
    } catch (error) {
            }

    // Clear existing interval if any
    if (vpsUpdateInterval) {
                clearInterval(vpsUpdateInterval);
    }

    // Set up new periodic updates for VPS data ONLY
        vpsUpdateInterval = setInterval(() => {
                // Reload server list to get updated data
        if (typeof loadServerList === 'function') {
            loadServerList();
        }
    }, vpsRefreshIntervalMs);

    }

// --- Theme Management (copied from main.js) ---
const THEME_KEY = 'vps-monitor-theme';
const LIGHT_THEME = 'light';
const DARK_THEME = 'dark';

function initializeTheme() {
    const themeToggler = document.getElementById('themeToggler');
    if (!themeToggler) return;

    const storedTheme = localStorage.getItem(THEME_KEY) || LIGHT_THEME;
    applyTheme(storedTheme);

    themeToggler.addEventListener('click', () => {
        const currentTheme = document.documentElement.getAttribute('data-bs-theme');
        const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;
        applyTheme(newTheme);
        localStorage.setItem(THEME_KEY, newTheme);
    });
}

function applyTheme(theme) {
    document.documentElement.setAttribute('data-bs-theme', theme);
    const themeTogglerIcon = document.querySelector('#themeToggler i');
    if (themeTogglerIcon) {
        if (theme === DARK_THEME) {
            themeTogglerIcon.classList.remove('bi-moon-stars-fill');
            themeTogglerIcon.classList.add('bi-sun-fill');
        } else {
            themeTogglerIcon.classList.remove('bi-sun-fill');
            themeTogglerIcon.classList.add('bi-moon-stars-fill');
        }
    }
}
// --- End Theme Management ---

// \u5DE5\u5177\u63D0\u793A\u73B0\u5728\u4F7F\u7528\u6D4F\u89C8\u5668\u539F\u751Ftitle\u5C5E\u6027\uFF0C\u65E0\u9700JavaScript\u521D\u59CB\u5316

// \u4F18\u5316\u7684\u6E05\u7406\u51FD\u6570 - \u6E05\u7406\u53EF\u80FD\u5361\u4F4F\u7684\u5F00\u5173
function cleanupStuckToggles() {
    const stuckToggles = document.querySelectorAll('[data-updating="true"]');
    if (stuckToggles.length > 0) {
                stuckToggles.forEach(toggle => {
            toggle.disabled = false;
            delete toggle.dataset.updating;
            toggle.style.opacity = '1';
        });
    }
}

// \u79FB\u9664\u4E86\u590D\u6742\u7684waitForToggleReady\u51FD\u6570\uFF0C\u73B0\u5728\u76F4\u63A5\u5728API\u54CD\u5E94\u540E\u66F4\u65B0UI\u72B6\u6001

// \u5168\u5C40\u53D8\u91CF
let currentServerId = null;
let currentSiteId = null; // For site deletion
let serverList = [];
let siteList = []; // For monitored sites
let hasAddedNewServer = false; // \u6807\u8BB0\u662F\u5426\u6DFB\u52A0\u4E86\u65B0\u670D\u52A1\u5668
let displaySettings = {
    showServerSection: true,
    showSiteSection: true
};

// \u9875\u9762\u52A0\u8F7D\u5B8C\u6210\u540E\u6267\u884C
document.addEventListener('DOMContentLoaded', async function() {
    // Initialize theme
    initializeTheme();

    // \u68C0\u67E5\u767B\u5F55\u72B6\u6001 - \u5FC5\u987B\u5148\u5B8C\u6210\u8BA4\u8BC1\u68C0\u67E5
    await checkLoginStatus();

    // \u521D\u59CB\u5316\u4E8B\u4EF6\u76D1\u542C
    initEventListeners();

    // \u521D\u59CB\u5316Bootstrap tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // \u52A0\u8F7D\u670D\u52A1\u5668\u5217\u8868
    loadServerList();
    // \u52A0\u8F7D\u76D1\u63A7\u7F51\u7AD9\u5217\u8868
    loadSiteList();
    // \u52A0\u8F7DTelegram\u8BBE\u7F6E
    loadTelegramSettings();
    // \u52A0\u8F7D\u80CC\u666F\u8BBE\u7F6E
    loadBackgroundSettings();
    // \u52A0\u8F7D\u5168\u5C40\u8BBE\u7F6E (VPS Report Interval) - will use serverAlert for notifications
    loadGlobalSettings();
    // \u52A0\u8F7D\u9996\u9875\u5C55\u793A\u8BBE\u7F6E
    loadDisplaySettings();

    // \u521D\u59CB\u5316\u7BA1\u7406\u540E\u53F0\u7684\u5B9A\u65F6\u5237\u65B0\u673A\u5236
    initializeVpsDataUpdates();

    // \u68C0\u67E5\u662F\u5426\u4F7F\u7528\u9ED8\u8BA4\u5BC6\u7801
    checkDefaultPasswordUsage();

    // \u4F18\u5316\uFF1A\u505C\u6B62\u81EA\u52A8\u6E05\u7406\u4EE5\u8282\u7701\u914D\u989D
    // setInterval(cleanupStuckToggles, 30000);
    });

// \u68C0\u67E5\u767B\u5F55\u72B6\u6001
async function checkLoginStatus() {
    try {
        // \u4ECElocalStorage\u83B7\u53D6token
        const token = localStorage.getItem('auth_token');
        if (!token) {
            // \u672A\u767B\u5F55\uFF0C\u91CD\u5B9A\u5411\u5230\u767B\u5F55\u9875\u9762
            window.location.href = 'login.html';
            return;
        }

        const data = await apiRequest('/api/auth/status');
        if (!data.authenticated) {
            // \u672A\u767B\u5F55\uFF0C\u91CD\u5B9A\u5411\u5230\u767B\u5F55\u9875\u9762
            window.location.href = 'login.html';
        }
    } catch (error) {
                window.location.href = 'login.html';
    }
}

// \u68C0\u67E5\u662F\u5426\u4F7F\u7528\u9ED8\u8BA4\u5BC6\u7801
async function checkDefaultPasswordUsage() {
    try {
        // \u4ECElocalStorage\u83B7\u53D6\u662F\u5426\u663E\u793A\u8FC7\u9ED8\u8BA4\u5BC6\u7801\u63D0\u9192
        const hasShownDefaultPasswordWarning = localStorage.getItem('hasShownDefaultPasswordWarning');

        if (hasShownDefaultPasswordWarning === 'true') {
            return; // \u5DF2\u7ECF\u663E\u793A\u8FC7\u63D0\u9192\uFF0C\u4E0D\u518D\u663E\u793A
        }

        // \u68C0\u67E5\u5F53\u524D\u7528\u6237\u767B\u5F55\u72B6\u6001\u548C\u9ED8\u8BA4\u5BC6\u7801\u4F7F\u7528\u60C5\u51B5
        const token = localStorage.getItem('auth_token');
                if (token) {
            try {
                const statusData = await apiRequest('/api/auth/status');
                if (statusData.authenticated && statusData.user && statusData.user.usingDefaultPassword) {
                    // \u663E\u793A\u9ED8\u8BA4\u5BC6\u7801\u63D0\u9192
                    showToast('warning',
                        '\u5B89\u5168\u63D0\u9192\uFF1A\u60A8\u6B63\u5728\u4F7F\u7528\u9ED8\u8BA4\u5BC6\u7801\u767B\u5F55\u3002\u4E3A\u4E86\u60A8\u7684\u8D26\u6237\u5B89\u5168\uFF0C\u5EFA\u8BAE\u5C3D\u5FEB\u4FEE\u6539\u5BC6\u7801\u3002\u70B9\u51FB\u53F3\u4E0A\u89D2\u7684"\u4FEE\u6539\u5BC6\u7801"\u6309\u94AE\u6765\u66F4\u6539\u5BC6\u7801\u3002',
                        { duration: 10000 }); // 10\u79D2\u663E\u793A

                    // \u6807\u8BB0\u5DF2\u663E\u793A\u8FC7\u63D0\u9192
                    localStorage.setItem('hasShownDefaultPasswordWarning', 'true');
                }
            } catch (error) {
                            }
        }
    } catch (error) {
            }
}

// \u521D\u59CB\u5316\u4E8B\u4EF6\u76D1\u542C
function initEventListeners() {
    // \u6DFB\u52A0\u670D\u52A1\u5668\u6309\u94AE
    document.getElementById('addServerBtn').addEventListener('click', function() {
        showServerModal();
    });

    // \u4FDD\u5B58\u670D\u52A1\u5668\u6309\u94AE
    document.getElementById('saveServerBtn').addEventListener('click', function() {
        saveServer();
    });

    // Helper function for copying text to clipboard and providing button feedback
    function copyToClipboard(textToCopy, buttonElement) {
        navigator.clipboard.writeText(textToCopy).then(() => {
            const originalHtml = buttonElement.innerHTML;
            buttonElement.innerHTML = '<i class="bi bi-check-lg"></i>'; // Using a larger check icon
            buttonElement.classList.add('btn-success');
            buttonElement.classList.remove('btn-outline-secondary');

            setTimeout(() => {
                buttonElement.innerHTML = originalHtml;
                buttonElement.classList.remove('btn-success');
                buttonElement.classList.add('btn-outline-secondary');
            }, 2000);
        }).catch(err => {
            // \u9759\u9ED8\u5904\u7406\u590D\u5236\u5931\u8D25
            const originalHtml = buttonElement.innerHTML;
            buttonElement.innerHTML = '<i class="bi bi-x-lg"></i>'; // Error icon
            buttonElement.classList.add('btn-danger');
            buttonElement.classList.remove('btn-outline-secondary');
            setTimeout(() => {
                buttonElement.innerHTML = originalHtml;
                buttonElement.classList.remove('btn-danger');
                buttonElement.classList.add('btn-outline-secondary');
            }, 2000);
        });
    }

    // \u590D\u5236API\u5BC6\u94A5\u6309\u94AE
    document.getElementById('copyApiKeyBtn').addEventListener('click', function() {
        const apiKeyInput = document.getElementById('apiKey');
        copyToClipboard(apiKeyInput.value, this);
    });

    // \u590D\u5236\u670D\u52A1\u5668ID\u6309\u94AE
    document.getElementById('copyServerIdBtn').addEventListener('click', function() {
        const serverIdInput = document.getElementById('serverIdDisplay');
        copyToClipboard(serverIdInput.value, this);
    });

    // \u590D\u5236Worker\u5730\u5740\u6309\u94AE
    document.getElementById('copyWorkerUrlBtn').addEventListener('click', function() {
        const workerUrlInput = document.getElementById('workerUrlDisplay');
        copyToClipboard(workerUrlInput.value, this);
    });

    // \u786E\u8BA4\u5220\u9664\u6309\u94AE
    document.getElementById('confirmDeleteBtn').addEventListener('click', function() {
        if (currentServerId) {
            deleteServer(currentServerId);
        }
    });

    // \u4FEE\u6539\u5BC6\u7801\u6309\u94AE\uFF08\u79FB\u52A8\u7AEF\uFF09
    document.getElementById('changePasswordBtn').addEventListener('click', function() {
        showPasswordModal();
    });

    // \u4FEE\u6539\u5BC6\u7801\u6309\u94AE\uFF08PC\u7AEF\uFF09
    document.getElementById('changePasswordBtnDesktop').addEventListener('click', function() {
        showPasswordModal();
    });

    // \u4FDD\u5B58\u5BC6\u7801\u6309\u94AE
    document.getElementById('savePasswordBtn').addEventListener('click', function() {
        changePassword();
    });

    // \u9000\u51FA\u767B\u5F55\u6309\u94AE
    document.getElementById('logoutBtn').addEventListener('click', function() {
        logout();
    });

    // --- Site Monitoring Event Listeners ---
    document.getElementById('addSiteBtn').addEventListener('click', function() {
        showSiteModal();
    });

    document.getElementById('saveSiteBtn').addEventListener('click', function() {
        saveSite();
    });

     document.getElementById('confirmDeleteSiteBtn').addEventListener('click', function() {
        if (currentSiteId) {
            deleteSite(currentSiteId);
        }
    });

    // \u4FDD\u5B58Telegram\u8BBE\u7F6E\u6309\u94AE
    document.getElementById('saveTelegramSettingsBtn').addEventListener('click', function() {
        saveTelegramSettings();
    });

    // Background Settings Event Listeners
    document.getElementById('saveBackgroundSettingsBtn').addEventListener('click', function() {
        saveBackgroundSettings();
    });

    // \u900F\u660E\u5EA6\u6ED1\u5757\u5B9E\u65F6\u9884\u89C8
    document.getElementById('pageOpacity').addEventListener('input', function() {
        updateOpacityPreview();
    });

    // \u80CC\u666F\u5F00\u5173\u53D8\u5316\u65F6\u7684\u9884\u89C8
    document.getElementById('enableCustomBackground').addEventListener('change', function() {
        const enabled = this.checked;
        const url = document.getElementById('backgroundImageUrl').value.trim();
        const opacity = parseInt(document.getElementById('pageOpacity').value, 10);
        applyBackgroundSettings(enabled, url, opacity, false);
    });

    // URL\u8F93\u5165\u6846\u53D8\u5316\u65F6\u7684\u9884\u89C8
    document.getElementById('backgroundImageUrl').addEventListener('input', function() {
        const enabled = document.getElementById('enableCustomBackground').checked;
        const url = this.value.trim();
        const opacity = parseInt(document.getElementById('pageOpacity').value, 10);
        if (enabled) {
            applyBackgroundSettings(enabled, url, opacity, false);
        }
    });

    // Global Settings Event Listener
    document.getElementById('saveVpsReportIntervalBtn').addEventListener('click', function() {
        saveVpsReportInterval();
    });

    document.getElementById('showServerSectionToggle').addEventListener('change', function() {
        saveDisplaySettings({ showServerSection: this.checked });
    });

    document.getElementById('showSiteSectionToggle').addEventListener('change', function() {
        saveDisplaySettings({ showSiteSection: this.checked });
    });

    // \u670D\u52A1\u5668\u6A21\u6001\u6846\u5173\u95ED\u4E8B\u4EF6\u76D1\u542C\u5668
    const serverModal = document.getElementById('serverModal');
    if (serverModal) {
        serverModal.addEventListener('hidden.bs.modal', function() {
            // \u68C0\u67E5\u662F\u5426\u6709\u65B0\u6DFB\u52A0\u7684\u670D\u52A1\u5668\u9700\u8981\u5237\u65B0\u5217\u8868
            if (hasAddedNewServer) {
                hasAddedNewServer = false; // \u91CD\u7F6E\u6807\u8BB0
                loadServerList(); // \u5237\u65B0\u670D\u52A1\u5668\u5217\u8868
            }
        });
    }

    // \u521D\u59CB\u5316\u6392\u5E8F\u4E0B\u62C9\u83DC\u5355\u9ED8\u8BA4\u9009\u62E9
    setTimeout(() => {
        // \u786E\u4FDDDOM\u5DF2\u5B8C\u5168\u52A0\u8F7D
        updateServerSortDropdownSelection('custom');
        updateSiteSortDropdownSelection('custom');
    }, 100);
}

// --- Server Management Functions ---

// \u52A0\u8F7D\u670D\u52A1\u5668\u5217\u8868
async function loadServerList() {
    try {
        const data = await apiRequest('/api/admin/servers');
        serverList = data.servers || [];

        // \u7B80\u5316\u903B\u8F91\uFF1A\u76F4\u63A5\u6E32\u67D3\uFF0C\u667A\u80FD\u72B6\u6001\u663E\u793A\u4F1A\u5904\u7406\u66F4\u65B0\u4E2D\u7684\u6309\u94AE
        renderServerTable(serverList);
    } catch (error) {
                showToast('danger', '\u52A0\u8F7D\u670D\u52A1\u5668\u5217\u8868\u5931\u8D25\uFF0C\u8BF7\u5237\u65B0\u9875\u9762\u91CD\u8BD5');
    }
}

// \u6E32\u67D3\u670D\u52A1\u5668\u8868\u683C
function renderServerTable(servers) {
    const tableBody = document.getElementById('serverTableBody');

    // \u7B80\u5316\u72B6\u6001\u7BA1\u7406\uFF1A\u4E0D\u518D\u9700\u8981\u590D\u6742\u7684\u72B6\u6001\u4FDD\u5B58\u673A\u5236

    tableBody.innerHTML = '';

    if (servers.length === 0) {
        const row = document.createElement('tr');
        row.innerHTML = '<td colspan="10" class="text-center">\u6682\u65E0\u670D\u52A1\u5668\u6570\u636E</td>'; // Updated colspan
        tableBody.appendChild(row);
        // \u540C\u65F6\u66F4\u65B0\u79FB\u52A8\u7AEF\u5361\u7247
        renderMobileAdminServerCards([]);
        return;
    }

    servers.forEach((server, index) => {
        const row = document.createElement('tr');
        row.setAttribute('data-server-id', server.id);
        row.classList.add('server-row-draggable');
        row.draggable = true;

        // \u683C\u5F0F\u5316\u6700\u540E\u66F4\u65B0\u65F6\u95F4
        let lastUpdateText = '\u4ECE\u672A';
        let statusBadge = '<span class="badge bg-secondary">\u672A\u77E5</span>';

        if (server.last_report) {
            const lastUpdate = new Date(server.last_report * 1000);
            lastUpdateText = lastUpdate.toLocaleString();

            // \u68C0\u67E5\u662F\u5426\u5728\u7EBF\uFF08\u6700\u540E\u62A5\u544A\u65F6\u95F4\u57285\u5206\u949F\u5185\uFF09
            const now = new Date();
            const diffMinutes = (now - lastUpdate) / (1000 * 60);

            if (diffMinutes <= 5) {
                statusBadge = '<span class="badge bg-success">\u5728\u7EBF</span>';
            } else {
                statusBadge = '<span class="badge bg-danger">\u79BB\u7EBF</span>';
            }
        }

        // \u667A\u80FD\u72B6\u6001\u663E\u793A\uFF1A\u5B8C\u6574\u4FDD\u5B58\u66F4\u65B0\u4E2D\u6309\u94AE\u7684\u6240\u6709\u72B6\u6001
        const existingToggle = document.querySelector('.server-visibility-toggle[data-server-id="' + server.id + '"]');
        const isCurrentlyUpdating = existingToggle && existingToggle.dataset.updating === 'true';
        const displayState = isCurrentlyUpdating ? existingToggle.checked : server.is_public;
        const needsUpdatingState = isCurrentlyUpdating;

        row.innerHTML =
            '<td>' +
                '<div class="btn-group">' +
                    '<i class="bi bi-grip-vertical text-muted me-2" style="cursor: grab;" title="\u62D6\u62FD\u6392\u5E8F"></i>' +
                     '<button class="btn btn-sm btn-outline-secondary move-server-btn" data-id="' + server.id + '" data-direction="up" ' + (index === 0 ? 'disabled' : '') + '>' +
                        '<i class="bi bi-arrow-up"></i>' +
                    '</button>' +
                     '<button class="btn btn-sm btn-outline-secondary move-server-btn" data-id="' + server.id + '" data-direction="down" ' + (index === servers.length - 1 ? 'disabled' : '') + '>' +
                        '<i class="bi bi-arrow-down"></i>' +
                    '</button>' +
                '</div>' +
            '</td>' +
            '<td>' + server.id + '</td>' +
            '<td>' + server.name + '</td>' +
            '<td>' + (server.description || '-') + '</td>' +
            '<td>' + statusBadge + '</td>' +
            '<td>' + lastUpdateText + '</td>' +
            '<td>' +
                '<button class="btn btn-sm btn-outline-secondary view-key-btn" data-id="' + server.id + '">' +
                    '<i class="bi bi-key"></i> \u67E5\u770B\u5BC6\u94A5' +
                '</button>' +
            '</td>' +
            '<td>' +
                '<button class="btn btn-sm btn-outline-info copy-vps-script-btn" data-id="' + server.id + '" data-name="' + server.name + '" title="\u590D\u5236VPS\u5B89\u88C5\u811A\u672C">' +
                    '<i class="bi bi-clipboard-plus"></i> \u590D\u5236\u811A\u672C' +
                '</button>' +
            '</td>' +
            '<td>' +
                '<div class="form-check form-switch">' +
                    '<input class="form-check-input server-visibility-toggle" type="checkbox" data-server-id="' + server.id + '" ' + (displayState ? 'checked' : '') + (needsUpdatingState ? ' data-updating="true"' : '') + '>' +
                '</div>' +
            '</td>' +
            '<td>' +
                '<div class="btn-group">' +
                    '<button class="btn btn-sm btn-outline-primary edit-server-btn" data-id="' + server.id + '">' +
                        '<i class="bi bi-pencil"></i>' +
                    '</button>' +
                    '<button class="btn btn-sm btn-outline-danger delete-server-btn" data-id="' + server.id + '" data-name="' + server.name + '">' +
                        '<i class="bi bi-trash"></i>' +
                    '</button>' +
                '</div>' +
            '</td>';

        tableBody.appendChild(row);
    });

    // \u521D\u59CB\u5316\u62D6\u62FD\u6392\u5E8F
    initializeServerDragSort();

    // \u6DFB\u52A0\u4E8B\u4EF6\u76D1\u542C
    document.querySelectorAll('.view-key-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            viewApiKey(serverId);
        });
    });

    document.querySelectorAll('.edit-server-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            editServer(serverId);
        });
    });

    document.querySelectorAll('.delete-server-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            const serverName = this.getAttribute('data-name');
            showDeleteConfirmation(serverId, serverName);
        });
    });

    document.querySelectorAll('.move-server-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            const direction = this.getAttribute('data-direction');
            moveServer(serverId, direction);
        });
    });

    document.querySelectorAll('.copy-vps-script-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const serverId = this.getAttribute('data-id');
            const serverName = this.getAttribute('data-name');
            copyVpsInstallScript(serverId, serverName, this);
        });
    });

    // \u4F18\u5316\u7684\u663E\u793A\u5F00\u5173\u4E8B\u4EF6\u76D1\u542C - \u76F4\u63A5\u5904\u7406\u72B6\u6001\u5207\u6362
    document.querySelectorAll('.server-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('click', function(event) {
            // \u5982\u679C\u5F00\u5173\u6B63\u5728\u66F4\u65B0\u4E2D\uFF0C\u5FFD\u7565\u70B9\u51FB
            if (this.disabled || this.dataset.updating === 'true') {
                event.preventDefault();
                return;
            }

            const serverId = this.getAttribute('data-server-id');
            const targetState = this.checked; // \u70B9\u51FB\u540E\u7684\u72B6\u6001\u5C31\u662F\u76EE\u6807\u72B6\u6001
            const originalState = !this.checked; // \u539F\u59CB\u72B6\u6001\u662F\u76EE\u6807\u72B6\u6001\u7684\u76F8\u53CD

                        // \u7ACB\u5373\u8BBE\u7F6E\u4E3A\u52A0\u8F7D\u72B6\u6001
            this.disabled = true;
            this.style.opacity = '0.6';
            this.dataset.updating = 'true';

            updateServerVisibility(serverId, targetState, originalState, this);
        });
    });

    // \u91CD\u65B0\u5E94\u7528\u6B63\u5728\u66F4\u65B0\u6309\u94AE\u7684\u89C6\u89C9\u72B6\u6001\uFF08\u56E0\u4E3A\u91CD\u65B0\u6E32\u67D3\u4F1A\u521B\u5EFA\u65B0\u5143\u7D20\uFF09
    document.querySelectorAll('.server-visibility-toggle[data-updating="true"]').forEach(toggle => {
        toggle.disabled = true;
        toggle.style.opacity = '0.6';
    });

    // \u540C\u65F6\u6E32\u67D3\u79FB\u52A8\u7AEF\u5361\u7247
    renderMobileAdminServerCards(servers);
}

// \u521D\u59CB\u5316\u670D\u52A1\u5668\u62D6\u62FD\u6392\u5E8F
function initializeServerDragSort() {
    const tableBody = document.getElementById('serverTableBody');
    if (!tableBody) return;

    let draggedElement = null;
    let draggedOverElement = null;

    // \u4E3A\u6240\u6709\u53EF\u62D6\u62FD\u884C\u6DFB\u52A0\u4E8B\u4EF6\u76D1\u542C
    const draggableRows = tableBody.querySelectorAll('.server-row-draggable');

    draggableRows.forEach(row => {
        row.addEventListener('dragstart', function(e) {
            draggedElement = this;
            this.style.opacity = '0.5';
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/html', this.outerHTML);
        });

        row.addEventListener('dragend', function(e) {
            this.style.opacity = '';
            draggedElement = null;
            draggedOverElement = null;

            // \u79FB\u9664\u6240\u6709\u62D6\u62FD\u6837\u5F0F
            draggableRows.forEach(r => {
                r.classList.remove('drag-over-top', 'drag-over-bottom');
            });
        });

        row.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';

            if (this === draggedElement) return;

            draggedOverElement = this;

            // \u79FB\u9664\u5176\u4ED6\u884C\u7684\u62D6\u62FD\u6837\u5F0F
            draggableRows.forEach(r => {
                if (r !== this) {
                    r.classList.remove('drag-over-top', 'drag-over-bottom');
                }
            });

            // \u786E\u5B9A\u63D2\u5165\u4F4D\u7F6E
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;

            if (e.clientY < midpoint) {
                this.classList.add('drag-over-top');
                this.classList.remove('drag-over-bottom');
            } else {
                this.classList.add('drag-over-bottom');
                this.classList.remove('drag-over-top');
            }
        });

        row.addEventListener('drop', function(e) {
            e.preventDefault();

            if (this === draggedElement) return;

            const draggedServerId = draggedElement.getAttribute('data-server-id');
            const targetServerId = this.getAttribute('data-server-id');

            // \u786E\u5B9A\u63D2\u5165\u4F4D\u7F6E
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;
            const insertBefore = e.clientY < midpoint;

            // \u6267\u884C\u62D6\u62FD\u6392\u5E8F
            performServerDragSort(draggedServerId, targetServerId, insertBefore);
        });
    });
}

// \u6267\u884C\u670D\u52A1\u5668\u62D6\u62FD\u6392\u5E8F
async function performServerDragSort(draggedServerId, targetServerId, insertBefore) {
    try {
        // \u83B7\u53D6\u5F53\u524D\u670D\u52A1\u5668\u5217\u8868\u7684ID\u987A\u5E8F
        const currentOrder = serverList.map(server => server.id);

        // \u8BA1\u7B97\u65B0\u7684\u6392\u5E8F
        const draggedIndex = currentOrder.indexOf(draggedServerId);
        const targetIndex = currentOrder.indexOf(targetServerId);

        if (draggedIndex === -1 || targetIndex === -1) {
            throw new Error('\u65E0\u6CD5\u627E\u5230\u670D\u52A1\u5668');
        }

        // \u521B\u5EFA\u65B0\u7684\u6392\u5E8F\u6570\u7EC4
        const newOrder = [...currentOrder];
        newOrder.splice(draggedIndex, 1); // \u79FB\u9664\u62D6\u62FD\u7684\u5143\u7D20

        // \u8BA1\u7B97\u63D2\u5165\u4F4D\u7F6E
        let insertIndex = targetIndex;
        if (draggedIndex < targetIndex) {
            insertIndex = targetIndex - 1;
        }
        if (!insertBefore) {
            insertIndex += 1;
        }

        newOrder.splice(insertIndex, 0, draggedServerId); // \u63D2\u5165\u5230\u65B0\u4F4D\u7F6E

        // \u53D1\u9001\u6279\u91CF\u6392\u5E8F\u8BF7\u6C42
        await apiRequest('/api/admin/servers/batch-reorder', {
            method: 'POST',
            body: JSON.stringify({ serverIds: newOrder })
        });

        // \u91CD\u65B0\u52A0\u8F7D\u670D\u52A1\u5668\u5217\u8868
        await loadServerList();
        showToast('success', '\u670D\u52A1\u5668\u6392\u5E8F\u5DF2\u66F4\u65B0');

    } catch (error) {
                showToast('danger', '\u62D6\u62FD\u6392\u5E8F\u5931\u8D25: ' + error.message);
        // \u91CD\u65B0\u52A0\u8F7D\u4EE5\u6062\u590D\u539F\u59CB\u72B6\u6001
        loadServerList();
    }
}


// Function to copy VPS installation script
async function copyVpsInstallScript(serverId, serverName, buttonElement) {
    const originalButtonHtml = buttonElement.innerHTML;
    buttonElement.disabled = true;
    buttonElement.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> \u751F\u6210\u4E2D...';

    try {
        // \u83B7\u53D6\u5305\u542B\u5B8C\u6574API\u5BC6\u94A5\u7684\u670D\u52A1\u5668\u4FE1\u606F
        const response = await apiRequest('/api/admin/servers?full_key=true');
        const server = response.servers.find(s => s.id === serverId);

        if (!server || !server.api_key) {
            throw new Error('\u672A\u627E\u5230\u670D\u52A1\u5668\u6216API\u5BC6\u94A5\uFF0C\u8BF7\u5237\u65B0\u9875\u9762\u91CD\u8BD5');
        }

        const apiKey = server.api_key;
        const workerUrl = window.location.origin;

        // \u4F7F\u7528GitHub\u4E0A\u7684\u811A\u672C\u5730\u5740
        const baseScriptUrl = "https://raw.githubusercontent.com/kadidalax/cf-vps-monitor/main/cf-vps-monitor.sh";
        // \u751F\u6210\u5B89\u88C5\u547D\u4EE4\uFF08\u8BA9\u811A\u672C\u81EA\u52A8\u4ECE\u670D\u52A1\u5668\u83B7\u53D6\u4E0A\u62A5\u95F4\u9694\uFF09
        const scriptCommand = 'wget ' + baseScriptUrl + ' -O cf-vps-monitor.sh && chmod +x cf-vps-monitor.sh && ./cf-vps-monitor.sh -i -k ' + apiKey + ' -s ' + serverId + ' -u ' + workerUrl;

        await navigator.clipboard.writeText(scriptCommand);

        buttonElement.innerHTML = '<i class="bi bi-check-lg"></i> \u5DF2\u590D\u5236!';
        buttonElement.classList.remove('btn-outline-info');
        buttonElement.classList.add('btn-success');

        showToast('success', '\u670D\u52A1\u5668 "' + serverName + '" \u7684\u5B89\u88C5\u811A\u672C\u5DF2\u590D\u5236\u5230\u526A\u8D34\u677F');

    } catch (error) {
                showToast('danger', '\u590D\u5236\u811A\u672C\u5931\u8D25: ' + error.message);
        buttonElement.innerHTML = '<i class="bi bi-x-lg"></i> \u590D\u5236\u5931\u8D25';
        buttonElement.classList.remove('btn-outline-info');
        buttonElement.classList.add('btn-danger');
    } finally {
        setTimeout(() => {
            buttonElement.disabled = false;
            buttonElement.innerHTML = originalButtonHtml;
            buttonElement.classList.remove('btn-success', 'btn-danger');
            buttonElement.classList.add('btn-outline-info');
        }, 3000); // Revert button state after 3 seconds
    }
}

// \u66F4\u65B0\u670D\u52A1\u5668\u663E\u793A\u72B6\u6001
async function updateServerVisibility(serverId, isPublic, originalState, toggleElement) {
    const startTime = Date.now();
        try {
        const data = await apiRequest('/api/admin/servers/' + serverId + '/visibility', {
            method: 'POST',
            body: JSON.stringify({ is_public: isPublic })
        });

        const requestTime = Date.now() - startTime;
                // \u66F4\u65B0\u672C\u5730\u6570\u636E
        const serverIndex = serverList.findIndex(s => s.id === serverId);
        if (serverIndex !== -1) {
            serverList[serverIndex].is_public = isPublic;
        }

        // \u6210\u529F\u540E\u8BBE\u7F6E\u6700\u7EC8\u6B63\u5E38\u72B6\u6001 - \u4F7F\u7528\u53EF\u9760\u7684\u6062\u590D\u673A\u5236
        function restoreButtonState(retryCount = 0) {
            const currentToggle = document.querySelector('.server-visibility-toggle[data-server-id="' + serverId + '"]');
            if (currentToggle) {
                                currentToggle.checked = isPublic;
                currentToggle.style.opacity = '1';
                currentToggle.disabled = false;
                delete currentToggle.dataset.updating;

                // \u76F4\u63A5\u663E\u793A\u6210\u529F\u63D0\u9192
                showToast('success', '\u670D\u52A1\u5668\u663E\u793A\u72B6\u6001\u5DF2' + (isPublic ? '\u5F00\u542F' : '\u5173\u95ED'));
            } else if (retryCount < 3) {
                                setTimeout(() => restoreButtonState(retryCount + 1), 100);
            } else {
                // \u9759\u9ED8\u5904\u7406\u6309\u94AE\u5143\u7D20\u672A\u627E\u5230
            }
        }

        // \u7ACB\u5373\u5C1D\u8BD5\u6062\u590D\uFF0C\u5982\u679C\u5931\u8D25\u5219\u91CD\u8BD5
        restoreButtonState();

    } catch (error) {
                // \u5931\u8D25\u65F6\u6062\u590D\u539F\u59CB\u72B6\u6001
        const currentToggle = document.querySelector('.server-visibility-toggle[data-server-id="' + serverId + '"]');
        if (currentToggle) {
            currentToggle.checked = originalState;
            currentToggle.style.opacity = '1';
            currentToggle.disabled = false;
            delete currentToggle.dataset.updating;

            // \u76F4\u63A5\u663E\u793A\u9519\u8BEF\u63D0\u9192\uFF0C\u4E0D\u9700\u8981\u7B49\u5F85\u72B6\u6001\u53D8\u5316
            showToast('danger', '\u66F4\u65B0\u663E\u793A\u72B6\u6001\u5931\u8D25: ' + error.message);
        } else {
            // \u5982\u679C\u627E\u4E0D\u5230\u5F00\u5173\u5143\u7D20\uFF0C\u7ACB\u5373\u663E\u793A\u9519\u8BEF
            showToast('danger', '\u66F4\u65B0\u663E\u793A\u72B6\u6001\u5931\u8D25: ' + error.message);
        }
    }
}

// \u79FB\u52A8\u670D\u52A1\u5668\u987A\u5E8F
async function moveServer(serverId, direction) {
    try {
        await apiRequest('/api/admin/servers/' + serverId + '/reorder', {
            method: 'POST',
            body: JSON.stringify({ direction })
        });

        // \u91CD\u65B0\u52A0\u8F7D\u5217\u8868\u4EE5\u53CD\u6620\u65B0\u987A\u5E8F
        await loadServerList();
        showToast('success', '\u670D\u52A1\u5668\u5DF2\u6210\u529F' + (direction === 'up' ? '\u4E0A\u79FB' : '\u4E0B\u79FB'));

    } catch (error) {
                showToast('danger', '\u79FB\u52A8\u670D\u52A1\u5668\u5931\u8D25: ' + error.message);
    }
}

// \u663E\u793A\u670D\u52A1\u5668\u6A21\u6001\u6846\uFF08\u6DFB\u52A0\u6A21\u5F0F\uFF09
function showServerModal() {
    // \u91CD\u7F6E\u8868\u5355\u548C\u6807\u8BB0
    document.getElementById('serverForm').reset();
    document.getElementById('serverId').value = '';
    document.getElementById('apiKeyGroup').classList.add('d-none');
    document.getElementById('serverIdDisplayGroup').classList.add('d-none');
    document.getElementById('workerUrlDisplayGroup').classList.add('d-none');
    hasAddedNewServer = false; // \u91CD\u7F6E\u65B0\u670D\u52A1\u5668\u6807\u8BB0

    // \u8BBE\u7F6E\u6A21\u6001\u6846\u6807\u9898
    document.getElementById('serverModalTitle').textContent = '\u6DFB\u52A0\u670D\u52A1\u5668';

    // \u663E\u793A\u6A21\u6001\u6846
    const serverModal = new bootstrap.Modal(document.getElementById('serverModal'));
    serverModal.show();
}

// \u7F16\u8F91\u670D\u52A1\u5668
function editServer(serverId) {
    const server = serverList.find(s => s.id === serverId);
    if (!server) return;

    // \u586B\u5145\u8868\u5355
    document.getElementById('serverId').value = server.id;
    document.getElementById('serverName').value = server.name;
    document.getElementById('serverDescription').value = server.description || '';
    document.getElementById('apiKeyGroup').classList.add('d-none');
    document.getElementById('serverIdDisplayGroup').classList.add('d-none');
    document.getElementById('workerUrlDisplayGroup').classList.add('d-none');

    // \u8BBE\u7F6E\u6A21\u6001\u6846\u6807\u9898
    document.getElementById('serverModalTitle').textContent = '\u7F16\u8F91\u670D\u52A1\u5668';

    // \u663E\u793A\u6A21\u6001\u6846
    const serverModal = new bootstrap.Modal(document.getElementById('serverModal'));
    serverModal.show();
}

// \u4FDD\u5B58\u670D\u52A1\u5668
async function saveServer() {
    const serverId = document.getElementById('serverId').value;
    const serverName = document.getElementById('serverName').value.trim();
    const serverDescription = document.getElementById('serverDescription').value.trim();
    // const enableFrequentNotifications = document.getElementById('serverEnableFrequentNotifications').checked; // Removed

    if (!serverName) {
        showToast('warning', '\u670D\u52A1\u5668\u540D\u79F0\u4E0D\u80FD\u4E3A\u7A7A');
        return;
    }

    try {
        let data;

        if (serverId) {
            // \u66F4\u65B0\u670D\u52A1\u5668
            data = await apiRequest('/api/admin/servers/' + serverId, {
                method: 'PUT',
                body: JSON.stringify({
                    name: serverName,
                    description: serverDescription
                })
            });
        } else {
            // \u6DFB\u52A0\u670D\u52A1\u5668
            data = await apiRequest('/api/admin/servers', {
                method: 'POST',
                body: JSON.stringify({
                    name: serverName,
                    description: serverDescription
                })
            });
        }

        // \u5982\u679C\u662F\u65B0\u6DFB\u52A0\u7684\u670D\u52A1\u5668\uFF0C\u6D41\u7545\u5730\u5207\u6362\u5230\u5BC6\u94A5\u663E\u793A\uFF08\u4E0D\u9690\u85CF\u6A21\u6001\u6846\uFF09
        if (!serverId && data.server && data.server.api_key) {
            hasAddedNewServer = true; // \u6807\u8BB0\u5DF2\u6DFB\u52A0\u65B0\u670D\u52A1\u5668

            // \u76F4\u63A5\u5728\u5F53\u524D\u6A21\u6001\u6846\u4E2D\u663E\u793A\u5BC6\u94A5\u4FE1\u606F\uFF0C\u63D0\u4F9B\u6D41\u7545\u7684\u7528\u6237\u4F53\u9A8C
            // \u4E0D\u9690\u85CF\u6A21\u6001\u6846\uFF0C\u800C\u662F\u5207\u6362\u5185\u5BB9\uFF0C\u8BA9\u7528\u6237\u611F\u89C9\u662F\u81EA\u7136\u7684\u8FC7\u6E21
            showApiKeyInCurrentModal(data.server);
            showToast('success', '\u670D\u52A1\u5668\u6DFB\u52A0\u6210\u529F');

            // \u5728\u540E\u53F0\u5F02\u6B65\u5237\u65B0\u670D\u52A1\u5668\u5217\u8868
            loadServerList().catch(error => {
                            });
        } else {
            // \u7F16\u8F91\u670D\u52A1\u5668\u7684\u60C5\u51B5\uFF0C\u6B63\u5E38\u9690\u85CF\u6A21\u6001\u6846\u5E76\u5237\u65B0\u5217\u8868
            const serverModal = bootstrap.Modal.getInstance(document.getElementById('serverModal'));
            serverModal.hide();

            await loadServerList();
            showToast('success', serverId ? '\u670D\u52A1\u5668\u66F4\u65B0\u6210\u529F' : '\u670D\u52A1\u5668\u6DFB\u52A0\u6210\u529F');
        }
    } catch (error) {
                showToast('danger', '\u4FDD\u5B58\u670D\u52A1\u5668\u5931\u8D25\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5');
    }
}

// \u67E5\u770BAPI\u5BC6\u94A5\uFF08\u83B7\u53D6\u5B8C\u6574\u5BC6\u94A5\u7248\u672C\uFF09
async function viewApiKey(serverId) {
    try {
        // \u8BF7\u6C42\u5305\u542B\u5B8C\u6574API\u5BC6\u94A5\u7684\u670D\u52A1\u5668\u4FE1\u606F
        const response = await apiRequest('/api/admin/servers?full_key=true');
        const server = response.servers.find(s => s.id === serverId);

        if (server && server.api_key) {
            showApiKey(server);
        } else {
            showToast('danger', '\u672A\u627E\u5230\u670D\u52A1\u5668\u4FE1\u606F\u6216API\u5BC6\u94A5\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5');
        }
    } catch (error) {
                showToast('danger', '\u67E5\u770BAPI\u5BC6\u94A5\u5931\u8D25\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5');
    }
}

// \u5728\u5F53\u524D\u6A21\u6001\u6846\u4E2D\u663E\u793AAPI\u5BC6\u94A5\uFF08\u7528\u4E8E\u6DFB\u52A0\u670D\u52A1\u5668\u540E\u7684\u6D41\u7545\u8FC7\u6E21\uFF09
function showApiKeyInCurrentModal(server) {
    // \u586B\u5145\u8868\u5355\u6570\u636E
    document.getElementById('serverId').value = server.id;
    document.getElementById('serverName').value = server.name;
    document.getElementById('serverDescription').value = server.description || '';

    // \u663E\u793AAPI\u5BC6\u94A5\u3001\u670D\u52A1\u5668ID\u548CWorker URL
    document.getElementById('apiKey').value = server.api_key;
    document.getElementById('apiKeyGroup').classList.remove('d-none');

    document.getElementById('serverIdDisplay').value = server.id;
    document.getElementById('serverIdDisplayGroup').classList.remove('d-none');

    document.getElementById('workerUrlDisplay').value = window.location.origin;
    document.getElementById('workerUrlDisplayGroup').classList.remove('d-none');

    // \u66F4\u65B0\u6A21\u6001\u6846\u6807\u9898
    document.getElementById('serverModalTitle').textContent = '\u670D\u52A1\u5668\u8BE6\u7EC6\u4FE1\u606F\u4E0E\u5BC6\u94A5';

    // \u6CE8\u610F\uFF1A\u4E0D\u521B\u5EFA\u65B0\u7684\u6A21\u6001\u6846\uFF0C\u800C\u662F\u5728\u5F53\u524D\u6A21\u6001\u6846\u4E2D\u5207\u6362\u5185\u5BB9
    // \u8FD9\u6837\u7528\u6237\u611F\u89C9\u662F\u81EA\u7136\u7684\u5185\u5BB9\u8FC7\u6E21\uFF0C\u800C\u4E0D\u662F\u7A81\u7136\u5F39\u51FA\u65B0\u7A97\u53E3
}

// \u663E\u793AAPI\u5BC6\u94A5\uFF08\u7528\u4E8E\u67E5\u770B\u5BC6\u94A5\u6309\u94AE\uFF09
function showApiKey(server) {
    // \u586B\u5145\u8868\u5355
    document.getElementById('serverId').value = server.id; // Hidden input for form submission if needed
    document.getElementById('serverName').value = server.name;
    document.getElementById('serverDescription').value = server.description || '';

    // Populate and show API Key, Server ID, and Worker URL
    document.getElementById('apiKey').value = server.api_key;
    document.getElementById('apiKeyGroup').classList.remove('d-none');

    document.getElementById('serverIdDisplay').value = server.id;
    document.getElementById('serverIdDisplayGroup').classList.remove('d-none');

    document.getElementById('workerUrlDisplay').value = window.location.origin;
    document.getElementById('workerUrlDisplayGroup').classList.remove('d-none');

    // \u8BBE\u7F6E\u6A21\u6001\u6846\u6807\u9898
    document.getElementById('serverModalTitle').textContent = '\u670D\u52A1\u5668\u8BE6\u7EC6\u4FE1\u606F\u4E0E\u5BC6\u94A5';

    // \u663E\u793A\u6A21\u6001\u6846
    const serverModal = new bootstrap.Modal(document.getElementById('serverModal'));
    serverModal.show();
}

// \u663E\u793A\u5220\u9664\u786E\u8BA4
function showDeleteConfirmation(serverId, serverName) {
    currentServerId = serverId;
    document.getElementById('deleteServerName').textContent = serverName;

    const deleteModal = new bootstrap.Modal(document.getElementById('deleteModal'));
    deleteModal.show();
}

// \u5220\u9664\u670D\u52A1\u5668
async function deleteServer(serverId) {
    try {
        await apiRequest('/api/admin/servers/' + serverId + '?confirm=true', {
            method: 'DELETE'
        });

        // \u9690\u85CF\u6A21\u6001\u6846
        const deleteModal = bootstrap.Modal.getInstance(document.getElementById('deleteModal'));
        deleteModal.hide();

        // \u91CD\u65B0\u52A0\u8F7D\u670D\u52A1\u5668\u5217\u8868
        loadServerList();
        showToast('success', '\u670D\u52A1\u5668\u5220\u9664\u6210\u529F');
    } catch (error) {
                showToast('danger', '\u5220\u9664\u670D\u52A1\u5668\u5931\u8D25\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5');
    }
}


// --- Site Monitoring Functions (Continued) ---

// \u66F4\u65B0\u7F51\u7AD9\u663E\u793A\u72B6\u6001
async function updateSiteVisibility(siteId, isPublic, originalState, toggleElement) {
    const startTime = Date.now();
        try {
        await apiRequest('/api/admin/sites/' + siteId + '/visibility', {
            method: 'POST',
            body: JSON.stringify({ is_public: isPublic })
        });

        const requestTime = Date.now() - startTime;
                        // \u66F4\u65B0\u672C\u5730\u6570\u636E
        const siteIndex = siteList.findIndex(s => s.id === siteId);
        if (siteIndex !== -1) {
            siteList[siteIndex].is_public = isPublic;
        }

        // \u6210\u529F\u540E\u8BBE\u7F6E\u6700\u7EC8\u6B63\u5E38\u72B6\u6001 - \u4F7F\u7528\u53EF\u9760\u7684\u6062\u590D\u673A\u5236
        function restoreButtonState(retryCount = 0) {
            const currentToggle = document.querySelector('.site-visibility-toggle[data-site-id="' + siteId + '"]');
            if (currentToggle) {
                                currentToggle.checked = isPublic;
                currentToggle.style.opacity = '1';
                currentToggle.disabled = false;
                delete currentToggle.dataset.updating;

                // \u76F4\u63A5\u663E\u793A\u6210\u529F\u63D0\u9192
                showToast('success', '\u7F51\u7AD9\u663E\u793A\u72B6\u6001\u5DF2' + (isPublic ? '\u5F00\u542F' : '\u5173\u95ED'));
            } else if (retryCount < 3) {
                                setTimeout(() => restoreButtonState(retryCount + 1), 100);
            } else {
                // \u9759\u9ED8\u5904\u7406\u7F51\u7AD9\u6309\u94AE\u5143\u7D20\u672A\u627E\u5230
            }
        }

        // \u7ACB\u5373\u5C1D\u8BD5\u6062\u590D\uFF0C\u5982\u679C\u5931\u8D25\u5219\u91CD\u8BD5
        restoreButtonState();

    } catch (error) {
                // \u5931\u8D25\u65F6\u6062\u590D\u539F\u59CB\u72B6\u6001
        const currentToggle = document.querySelector('.site-visibility-toggle[data-site-id="' + siteId + '"]');
        if (currentToggle) {
            currentToggle.checked = originalState;
            currentToggle.style.opacity = '1';
            currentToggle.disabled = false;
            delete currentToggle.dataset.updating;

            // \u76F4\u63A5\u663E\u793A\u9519\u8BEF\u63D0\u9192\uFF0C\u4E0D\u9700\u8981\u7B49\u5F85\u72B6\u6001\u53D8\u5316
            showToast('danger', '\u66F4\u65B0\u663E\u793A\u72B6\u6001\u5931\u8D25: ' + error.message);
        } else {
            // \u5982\u679C\u627E\u4E0D\u5230\u5F00\u5173\u5143\u7D20\uFF0C\u7ACB\u5373\u663E\u793A\u9519\u8BEF
            showToast('danger', '\u66F4\u65B0\u663E\u793A\u72B6\u6001\u5931\u8D25: ' + error.message);
        }
    }
}

// \u79FB\u52A8\u7F51\u7AD9\u987A\u5E8F
async function moveSite(siteId, direction) {
    try {
        await apiRequest('/api/admin/sites/' + siteId + '/reorder', {
            method: 'POST',
            body: JSON.stringify({ direction })
        });

        // \u91CD\u65B0\u52A0\u8F7D\u5217\u8868\u4EE5\u53CD\u6620\u65B0\u987A\u5E8F
        await loadSiteList();
        showToast('success', '\u7F51\u7AD9\u5DF2\u6210\u529F' + (direction === 'up' ? '\u4E0A\u79FB' : '\u4E0B\u79FB'));

    } catch (error) {
                showToast('danger', '\u79FB\u52A8\u7F51\u7AD9\u5931\u8D25: ' + error.message);
    }
}


// --- Password Management Functions ---

// \u663E\u793A\u5BC6\u7801\u4FEE\u6539\u6A21\u6001\u6846
function showPasswordModal() {
    // \u91CD\u7F6E\u8868\u5355
    document.getElementById('passwordForm').reset();

    const passwordModal = new bootstrap.Modal(document.getElementById('passwordModal'));
    passwordModal.show();
}

// \u4FEE\u6539\u5BC6\u7801
async function changePassword() {
    const currentPassword = document.getElementById('currentPassword').value;
    const newPassword = document.getElementById('newPassword').value;
    const confirmPassword = document.getElementById('confirmPassword').value;

    // \u9A8C\u8BC1\u8F93\u5165
    if (!currentPassword || !newPassword || !confirmPassword) {
        showToast('warning', '\u6240\u6709\u5BC6\u7801\u5B57\u6BB5\u90FD\u5FC5\u987B\u586B\u5199');
        return;
    }

    if (newPassword !== confirmPassword) {
        showToast('warning', '\u65B0\u5BC6\u7801\u548C\u786E\u8BA4\u5BC6\u7801\u4E0D\u5339\u914D');
        return;
    }

    try {
        await apiRequest('/api/auth/change-password', {
            method: 'POST',
            body: JSON.stringify({
                current_password: currentPassword,
                new_password: newPassword
            })
        });

        // \u9690\u85CF\u6A21\u6001\u6846
        const passwordModal = bootstrap.Modal.getInstance(document.getElementById('passwordModal'));
        passwordModal.hide();

        // \u6E05\u9664\u9ED8\u8BA4\u5BC6\u7801\u63D0\u9192\u6807\u8BB0\uFF0C\u8FD9\u6837\u5982\u679C\u7528\u6237\u518D\u6B21\u4F7F\u7528\u9ED8\u8BA4\u5BC6\u7801\u767B\u5F55\u4F1A\u91CD\u65B0\u63D0\u9192
        localStorage.removeItem('hasShownDefaultPasswordWarning');

        showToast('success', '\u5BC6\u7801\u4FEE\u6539\u6210\u529F');
    } catch (error) {
                showToast('danger', '\u5BC6\u7801\u4FEE\u6539\u8BF7\u6C42\u5931\u8D25\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5');
    }
}


// --- Auth Functions ---

// \u9000\u51FA\u767B\u5F55
function logout() {
    // \u6E05\u9664localStorage\u4E2D\u7684token\u548C\u63D0\u9192\u6807\u8BB0
    localStorage.removeItem('auth_token');
    localStorage.removeItem('hasShownDefaultPasswordWarning');

    // \u91CD\u5B9A\u5411\u5230\u767B\u5F55\u9875\u9762
    window.location.href = 'login.html';
}


// --- Site Monitoring Functions ---

// \u52A0\u8F7D\u76D1\u63A7\u7F51\u7AD9\u5217\u8868
async function loadSiteList() {
    try {
        const data = await apiRequest('/api/admin/sites');
        siteList = data.sites || [];

        // \u7B80\u5316\u903B\u8F91\uFF1A\u76F4\u63A5\u6E32\u67D3\uFF0C\u667A\u80FD\u72B6\u6001\u663E\u793A\u4F1A\u5904\u7406\u66F4\u65B0\u4E2D\u7684\u6309\u94AE
        renderSiteTable(siteList);
    } catch (error) {
                showToast('danger', '\u52A0\u8F7D\u76D1\u63A7\u7F51\u7AD9\u5217\u8868\u5931\u8D25: ' + error.message);
    }
}

// \u6E32\u67D3\u76D1\u63A7\u7F51\u7AD9\u8868\u683C
function renderSiteTable(sites) {
    const tableBody = document.getElementById('siteTableBody');

    // \u7B80\u5316\u72B6\u6001\u7BA1\u7406\uFF1A\u4E0D\u518D\u9700\u8981\u590D\u6742\u7684\u72B6\u6001\u4FDD\u5B58\u673A\u5236

    tableBody.innerHTML = '';

    if (sites.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="9" class="text-center">\u6682\u65E0\u76D1\u63A7\u7F51\u7AD9</td></tr>'; // Colspan updated
        // \u540C\u65F6\u66F4\u65B0\u79FB\u52A8\u7AEF\u5361\u7247
        renderMobileAdminSiteCards([]);
        return;
    }

    sites.forEach((site, index) => { // Added index for sorting buttons
        const row = document.createElement('tr');
        row.setAttribute('data-site-id', site.id);
        row.classList.add('site-row-draggable');
        row.draggable = true;

        const statusInfo = getSiteStatusBadge(site.last_status);
        const lastCheckTime = site.last_checked ? new Date(site.last_checked * 1000).toLocaleString() : '\u4ECE\u672A';
        const responseTime = site.last_response_time_ms !== null ? \`\${site.last_response_time_ms} ms\` : '-';

        // \u667A\u80FD\u72B6\u6001\u663E\u793A\uFF1A\u5B8C\u6574\u4FDD\u5B58\u66F4\u65B0\u4E2D\u6309\u94AE\u7684\u6240\u6709\u72B6\u6001
        const existingToggle = document.querySelector('.site-visibility-toggle[data-site-id="' + site.id + '"]');
        const isCurrentlyUpdating = existingToggle && existingToggle.dataset.updating === 'true';
        const displayState = isCurrentlyUpdating ? existingToggle.checked : site.is_public;
        const needsUpdatingState = isCurrentlyUpdating;

        row.innerHTML = \`
             <td>
                <div class="btn-group btn-group-sm">
                    <i class="bi bi-grip-vertical text-muted me-2" style="cursor: grab;" title="\u62D6\u62FD\u6392\u5E8F"></i>
                     <button class="btn btn-outline-secondary move-site-btn" data-id="\${site.id}" data-direction="up" \${index === 0 ? 'disabled' : ''} title="\u4E0A\u79FB">
                        <i class="bi bi-arrow-up"></i>
                    </button>
                     <button class="btn btn-outline-secondary move-site-btn" data-id="\${site.id}" data-direction="down" \${index === sites.length - 1 ? 'disabled' : ''} title="\u4E0B\u79FB">
                        <i class="bi bi-arrow-down"></i>
                    </button>
                </div>
            </td>
            <td>\${site.name || '-'}</td>
            <td><a href="\${site.url}" target="_blank" rel="noopener noreferrer">\${site.url}</a></td>
            <td><span class="badge \${statusInfo.class}">\${statusInfo.text}</span></td>
            <td>\${site.last_status_code || '-'}</td>
            <td>\${responseTime}</td>
            <td>\${lastCheckTime}</td>
            <td>
                <div class="form-check form-switch">
                    <input class="form-check-input site-visibility-toggle" type="checkbox" data-site-id="\${site.id}" \${displayState ? 'checked' : ''}\${needsUpdatingState ? ' data-updating="true"' : ''}>
                </div>
            </td>
            <td>
                <div class="btn-group">
                    <button class="btn btn-sm btn-outline-primary edit-site-btn" data-id="\${site.id}" title="\u7F16\u8F91">
                        <i class="bi bi-pencil"></i>
                    </button>
                    <button class="btn btn-sm btn-outline-danger delete-site-btn" data-id="\${site.id}" data-name="\${site.name || site.url}" data-url="\${site.url}" title="\u5220\u9664">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </td>
        \`;
        tableBody.appendChild(row);
    });

    // \u521D\u59CB\u5316\u62D6\u62FD\u6392\u5E8F
    initializeSiteDragSort();

    // Add event listeners for edit and delete buttons
    document.querySelectorAll('.edit-site-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const siteId = this.getAttribute('data-id');
            editSite(siteId);
        });
    });

    document.querySelectorAll('.delete-site-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const siteId = this.getAttribute('data-id');
            const siteName = this.getAttribute('data-name');
            const siteUrl = this.getAttribute('data-url');
            showDeleteSiteConfirmation(siteId, siteName, siteUrl);
        });
    });

    // Add event listeners for move buttons
    document.querySelectorAll('.move-site-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const siteId = this.getAttribute('data-id');
            const direction = this.getAttribute('data-direction');
            moveSite(siteId, direction);
        });
    });

    // \u4F18\u5316\u7684\u7F51\u7AD9\u663E\u793A\u5F00\u5173\u4E8B\u4EF6\u76D1\u542C - \u76F4\u63A5\u5904\u7406\u72B6\u6001\u5207\u6362
    document.querySelectorAll('.site-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('click', function(event) {
            // \u5982\u679C\u5F00\u5173\u6B63\u5728\u66F4\u65B0\u4E2D\uFF0C\u5FFD\u7565\u70B9\u51FB
            if (this.disabled || this.dataset.updating === 'true') {
                event.preventDefault();
                return;
            }

            const siteId = this.getAttribute('data-site-id');
            const targetState = this.checked; // \u70B9\u51FB\u540E\u7684\u72B6\u6001\u5C31\u662F\u76EE\u6807\u72B6\u6001
            const originalState = !this.checked; // \u539F\u59CB\u72B6\u6001\u662F\u76EE\u6807\u72B6\u6001\u7684\u76F8\u53CD

                        // \u7ACB\u5373\u8BBE\u7F6E\u4E3A\u52A0\u8F7D\u72B6\u6001
            this.disabled = true;
            this.style.opacity = '0.6';
            this.dataset.updating = 'true';

            updateSiteVisibility(siteId, targetState, originalState, this);
        });
    });

    // \u91CD\u65B0\u5E94\u7528\u6B63\u5728\u66F4\u65B0\u6309\u94AE\u7684\u89C6\u89C9\u72B6\u6001\uFF08\u56E0\u4E3A\u91CD\u65B0\u6E32\u67D3\u4F1A\u521B\u5EFA\u65B0\u5143\u7D20\uFF09
    document.querySelectorAll('.site-visibility-toggle[data-updating="true"]').forEach(toggle => {
        toggle.disabled = true;
        toggle.style.opacity = '0.6';
    });

    // \u540C\u65F6\u6E32\u67D3\u79FB\u52A8\u7AEF\u5361\u7247
    renderMobileAdminSiteCards(sites);
}

// \u521D\u59CB\u5316\u7F51\u7AD9\u62D6\u62FD\u6392\u5E8F
function initializeSiteDragSort() {
    const tableBody = document.getElementById('siteTableBody');
    if (!tableBody) return;

    let draggedElement = null;
    let draggedOverElement = null;

    // \u4E3A\u6240\u6709\u53EF\u62D6\u62FD\u884C\u6DFB\u52A0\u4E8B\u4EF6\u76D1\u542C
    const draggableRows = tableBody.querySelectorAll('.site-row-draggable');

    draggableRows.forEach(row => {
        row.addEventListener('dragstart', function(e) {
            draggedElement = this;
            this.style.opacity = '0.5';
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/html', this.outerHTML);
        });

        row.addEventListener('dragend', function(e) {
            this.style.opacity = '';
            draggedElement = null;
            draggedOverElement = null;

            // \u79FB\u9664\u6240\u6709\u62D6\u62FD\u6837\u5F0F
            draggableRows.forEach(r => {
                r.classList.remove('drag-over-top', 'drag-over-bottom');
            });
        });

        row.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';

            if (this === draggedElement) return;

            draggedOverElement = this;

            // \u79FB\u9664\u5176\u4ED6\u884C\u7684\u62D6\u62FD\u6837\u5F0F
            draggableRows.forEach(r => {
                if (r !== this) {
                    r.classList.remove('drag-over-top', 'drag-over-bottom');
                }
            });

            // \u786E\u5B9A\u63D2\u5165\u4F4D\u7F6E
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;

            if (e.clientY < midpoint) {
                this.classList.add('drag-over-top');
                this.classList.remove('drag-over-bottom');
            } else {
                this.classList.add('drag-over-bottom');
                this.classList.remove('drag-over-top');
            }
        });

        row.addEventListener('drop', function(e) {
            e.preventDefault();

            if (this === draggedElement) return;

            const draggedSiteId = draggedElement.getAttribute('data-site-id');
            const targetSiteId = this.getAttribute('data-site-id');

            // \u786E\u5B9A\u63D2\u5165\u4F4D\u7F6E
            const rect = this.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;
            const insertBefore = e.clientY < midpoint;

            // \u6267\u884C\u62D6\u62FD\u6392\u5E8F
            performSiteDragSort(draggedSiteId, targetSiteId, insertBefore);
        });
    });
}

// \u6267\u884C\u7F51\u7AD9\u62D6\u62FD\u6392\u5E8F
async function performSiteDragSort(draggedSiteId, targetSiteId, insertBefore) {
    try {
        // \u83B7\u53D6\u5F53\u524D\u7F51\u7AD9\u5217\u8868\u7684ID\u987A\u5E8F
        const currentOrder = siteList.map(site => site.id);

        // \u8BA1\u7B97\u65B0\u7684\u6392\u5E8F
        const draggedIndex = currentOrder.indexOf(draggedSiteId);
        const targetIndex = currentOrder.indexOf(targetSiteId);

        if (draggedIndex === -1 || targetIndex === -1) {
            throw new Error('\u65E0\u6CD5\u627E\u5230\u7F51\u7AD9');
        }

        // \u521B\u5EFA\u65B0\u7684\u6392\u5E8F\u6570\u7EC4
        const newOrder = [...currentOrder];
        newOrder.splice(draggedIndex, 1); // \u79FB\u9664\u62D6\u62FD\u7684\u5143\u7D20

        // \u8BA1\u7B97\u63D2\u5165\u4F4D\u7F6E
        let insertIndex = targetIndex;
        if (draggedIndex < targetIndex) {
            insertIndex = targetIndex - 1;
        }
        if (!insertBefore) {
            insertIndex += 1;
        }

        newOrder.splice(insertIndex, 0, draggedSiteId); // \u63D2\u5165\u5230\u65B0\u4F4D\u7F6E

        // \u53D1\u9001\u6279\u91CF\u6392\u5E8F\u8BF7\u6C42
        await apiRequest('/api/admin/sites/batch-reorder', {
            method: 'POST',
            body: JSON.stringify({ siteIds: newOrder })
        });

        // \u91CD\u65B0\u52A0\u8F7D\u7F51\u7AD9\u5217\u8868
        await loadSiteList();
        showToast('success', '\u7F51\u7AD9\u6392\u5E8F\u5DF2\u66F4\u65B0');

    } catch (error) {
                showToast('danger', '\u62D6\u62FD\u6392\u5E8F\u5931\u8D25: ' + error.message);
        // \u91CD\u65B0\u52A0\u8F7D\u4EE5\u6062\u590D\u539F\u59CB\u72B6\u6001
        loadSiteList();
    }
}

// \u83B7\u53D6\u7F51\u7AD9\u72B6\u6001\u5BF9\u5E94\u7684Badge\u6837\u5F0F\u548C\u6587\u672C
function getSiteStatusBadge(status) {
    switch (status) {
        case 'UP': return { class: 'bg-success', text: '\u6B63\u5E38' };
        case 'DOWN': return { class: 'bg-danger', text: '\u6545\u969C' };
        case 'TIMEOUT': return { class: 'bg-warning text-dark', text: '\u8D85\u65F6' };
        case 'ERROR': return { class: 'bg-danger', text: '\u9519\u8BEF' };
        case 'PENDING': return { class: 'bg-secondary', text: '\u5F85\u68C0\u6D4B' };
        default: return { class: 'bg-secondary', text: '\u672A\u77E5' };
    }
}


// \u663E\u793A\u6DFB\u52A0/\u7F16\u8F91\u7F51\u7AD9\u6A21\u6001\u6846 (handles both add and edit)
function showSiteModal(siteIdToEdit = null) {
    const form = document.getElementById('siteForm');
    form.reset();
    const modalTitle = document.getElementById('siteModalTitle');
    const siteIdInput = document.getElementById('siteId');

    if (siteIdToEdit) {
        const site = siteList.find(s => s.id === siteIdToEdit);
        if (site) {
            modalTitle.textContent = '\u7F16\u8F91\u76D1\u63A7\u7F51\u7AD9';
            siteIdInput.value = site.id;
            document.getElementById('siteName').value = site.name || '';
            document.getElementById('siteUrl').value = site.url;
            // document.getElementById('siteEnableFrequentNotifications').checked = site.enable_frequent_down_notifications || false; // Removed
        } else {
            showToast('danger', '\u672A\u627E\u5230\u8981\u7F16\u8F91\u7684\u7F51\u7AD9\u4FE1\u606F');
            return;
        }
    } else {
        modalTitle.textContent = '\u6DFB\u52A0\u76D1\u63A7\u7F51\u7AD9';
        siteIdInput.value = ''; // Clear ID for add mode
        // document.getElementById('siteEnableFrequentNotifications').checked = false; // Removed
    }

    const siteModal = new bootstrap.Modal(document.getElementById('siteModal'));
    siteModal.show();
}

// Function to call when edit button is clicked
function editSite(siteId) {
    showSiteModal(siteId);
}

// \u4FDD\u5B58\u7F51\u7AD9\uFF08\u6DFB\u52A0\u6216\u66F4\u65B0\uFF09
async function saveSite() {
    const siteId = document.getElementById('siteId').value; // Get ID from hidden input
    const siteName = document.getElementById('siteName').value.trim();
    const siteUrl = document.getElementById('siteUrl').value.trim();
    // const enableFrequentNotifications = document.getElementById('siteEnableFrequentNotifications').checked; // Removed

    if (!siteUrl) {
        showToast('warning', '\u8BF7\u8F93\u5165\u7F51\u7AD9URL');
        return;
    }
    if (!siteUrl.startsWith('http://') && !siteUrl.startsWith('https://')) {
         showToast('warning', 'URL\u5FC5\u987B\u4EE5 http:// \u6216 https:// \u5F00\u5934');
         return;
    }

    const requestBody = {
        url: siteUrl,
        name: siteName
        // enable_frequent_down_notifications: enableFrequentNotifications // Removed
    };
    let apiUrl = '/api/admin/sites';
    let method = 'POST';

    if (siteId) { // If siteId exists, it's an update
        apiUrl = \`/api/admin/sites/\${siteId}\`;
        method = 'PUT';
    }

    try {
        const responseData = await apiRequest(apiUrl, {
            method: method,
            body: JSON.stringify(requestBody)
        });

        const siteModalInstance = bootstrap.Modal.getInstance(document.getElementById('siteModal'));
        if (siteModalInstance) {
            siteModalInstance.hide();
        }

        await loadSiteList(); // Reload the list
        showToast('success', '\u76D1\u63A7\u7F51\u7AD9' + (siteId ? '\u66F4\u65B0' : '\u6DFB\u52A0') + '\u6210\u529F');

    } catch (error) {
                showToast('danger', '\u4FDD\u5B58\u7F51\u7AD9\u5931\u8D25: ' + error.message);
    }
}

// \u663E\u793A\u5220\u9664\u7F51\u7AD9\u786E\u8BA4\u6A21\u6001\u6846
function showDeleteSiteConfirmation(siteId, siteName, siteUrl) {
    currentSiteId = siteId;
    document.getElementById('deleteSiteName').textContent = siteName;
    document.getElementById('deleteSiteUrl').textContent = siteUrl;
    const deleteModal = new bootstrap.Modal(document.getElementById('deleteSiteModal'));
    deleteModal.show();
}


// \u5220\u9664\u7F51\u7AD9\u76D1\u63A7
async function deleteSite(siteId) {
    try {
        await apiRequest(\`/api/admin/sites/\${siteId}?confirm=true\`, {
            method: 'DELETE'
        });

        // Hide modal and reload list
        const deleteModal = bootstrap.Modal.getInstance(document.getElementById('deleteSiteModal'));
        deleteModal.hide();
        await loadSiteList(); // Reload list
        showToast('success', '\u7F51\u7AD9\u76D1\u63A7\u5DF2\u5220\u9664');
        currentSiteId = null; // Reset current ID

    } catch (error) {
                showToast('danger', '\u5220\u9664\u7F51\u7AD9\u5931\u8D25: ' + error.message);
    }
}


// --- Utility Functions ---

// \u7EDF\u4E00Toast\u5F39\u7A97\u51FD\u6570 (\u589E\u5F3A\u7248)
function showToast(type, message, options = {}) {
    const defaults = {
        success: 3000,
        info: 5000,
        warning: 8000,
        danger: 10000
    };

    const duration = options.duration || defaults[type] || 5000;
    const persistent = options.persistent || false;

    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = 'unified-toast ' + type;

    const icons = {
        success: 'bi-check-circle-fill',
        danger: 'bi-x-circle-fill',
        warning: 'bi-exclamation-triangle-fill',
        info: 'bi-info-circle-fill'
    };

    toast.innerHTML =
        '<i class="toast-icon bi ' + icons[type] + '"></i>' +
        '<div class="toast-content">' + message + '</div>' +
        '<button class="toast-close" onclick="hideToast(this.parentElement)">\xD7</button>' +
        (persistent ? '' : '<div class="toast-progress" style="animation-duration: ' + duration + 'ms"></div>');

    container.appendChild(toast);

    if (!persistent) {
        setTimeout(() => hideToast(toast), duration);
    }

    return toast;
}

function hideToast(toast) {
    if (!toast || toast.classList.contains('hiding')) return;
    toast.classList.add('hiding');
    setTimeout(function() {
        if (toast.parentNode) {
            toast.parentNode.removeChild(toast);
        }
    }, 300);
}





// --- Telegram Settings Functions ---

// \u52A0\u8F7DTelegram\u901A\u77E5\u8BBE\u7F6E
async function loadTelegramSettings() {
    try {
        const settings = await apiRequest('/api/admin/telegram-settings');
        if (settings) {
            document.getElementById('telegramBotToken').value = settings.bot_token || '';
            document.getElementById('telegramChatId').value = settings.chat_id || '';
            document.getElementById('enableTelegramNotifications').checked = !!settings.enable_notifications;
        }
    } catch (error) {
                showToast('danger', '\u52A0\u8F7DTelegram\u8BBE\u7F6E\u5931\u8D25: ' + error.message);
    }
}

// \u4FDD\u5B58Telegram\u901A\u77E5\u8BBE\u7F6E
async function saveTelegramSettings() {
    const botToken = document.getElementById('telegramBotToken').value.trim();
    const chatId = document.getElementById('telegramChatId').value.trim();
    let enableNotifications = document.getElementById('enableTelegramNotifications').checked;

    // If Bot Token or Chat ID is empty, automatically disable notifications
    if (!botToken || !chatId) {
        enableNotifications = false;
        document.getElementById('enableTelegramNotifications').checked = false; // Update the checkbox UI
        if (document.getElementById('enableTelegramNotifications').checked && (botToken || chatId)) { // Only show warning if user intended to enable
             showToast('warning', 'Bot Token \u548C Chat ID \u5747\u4E0D\u80FD\u4E3A\u7A7A\u624D\u80FD\u542F\u7528\u901A\u77E5\u3002\u901A\u77E5\u5DF2\u81EA\u52A8\u7981\u7528');
        }
    } else if (enableNotifications && (!botToken || !chatId)) { // This case should ideally not be hit due to above logic, but kept for safety
        showToast('warning', '\u542F\u7528\u901A\u77E5\u65F6\uFF0CBot Token \u548C Chat ID \u4E0D\u80FD\u4E3A\u7A7A');
        return;
    }


    try {
        await apiRequest('/api/admin/telegram-settings', {
            method: 'POST',
            body: JSON.stringify({
                bot_token: botToken,
                chat_id: chatId,
                enable_notifications: enableNotifications // Use the potentially modified value
            })
        });

        showToast('success', 'Telegram\u8BBE\u7F6E\u5DF2\u6210\u529F\u4FDD\u5B58');

    } catch (error) {
            showToast('danger', '\u4FDD\u5B58Telegram\u8BBE\u7F6E\u5931\u8D25: ' + error.message);
    }
}

// --- Background Settings Functions ---

// \u52A0\u8F7D\u80CC\u666F\u8BBE\u7F6E
async function loadBackgroundSettings() {
    try {
        const settings = await apiRequest('/api/background-settings');
        if (settings) {
            document.getElementById('enableCustomBackground').checked = !!settings.enabled;
            document.getElementById('backgroundImageUrl').value = settings.url || '';
            document.getElementById('pageOpacity').value = settings.opacity || 80;
            document.getElementById('opacityValue').textContent = settings.opacity || 80;

            // \u5E94\u7528\u5F53\u524D\u8BBE\u7F6E\uFF08\u4E0D\u4FDD\u5B58\u5230\u6570\u636E\u5E93\uFF09
            applyBackgroundSettings(settings.enabled, settings.url, settings.opacity, false);
        }
    } catch (error) {
                showToast('danger', '\u52A0\u8F7D\u80CC\u666F\u8BBE\u7F6E\u5931\u8D25: ' + error.message);
    }
}

// \u4FDD\u5B58\u80CC\u666F\u8BBE\u7F6E
async function saveBackgroundSettings() {
    const enabled = document.getElementById('enableCustomBackground').checked;
    const url = document.getElementById('backgroundImageUrl').value.trim();
    const opacity = parseInt(document.getElementById('pageOpacity').value, 10);

    // \u9A8C\u8BC1\u8F93\u5165
    if (enabled && url) {
        if (!url.startsWith('https://')) {
            showToast('warning', '\u80CC\u666F\u56FE\u7247URL\u5FC5\u987B\u4EE5https://\u5F00\u5934');
            return;
        }
    }

    if (isNaN(opacity) || opacity < 0 || opacity > 100) {
        showToast('warning', '\u900F\u660E\u5EA6\u5FC5\u987B\u662F0-100\u4E4B\u95F4\u7684\u6570\u5B57');
        return;
    }

    try {
        await apiRequest('/api/admin/background-settings', {
            method: 'POST',
            body: JSON.stringify({
                enabled: enabled,
                url: url,
                opacity: opacity
            })
        });

        // \u5E94\u7528\u8BBE\u7F6E\u5E76\u4FDD\u5B58\u5230localStorage
        applyBackgroundSettings(enabled, url, opacity, true);

        showToast('success', '\u80CC\u666F\u8BBE\u7F6E\u5DF2\u6210\u529F\u4FDD\u5B58');

    } catch (error) {
                showToast('danger', '\u4FDD\u5B58\u80CC\u666F\u8BBE\u7F6E\u5931\u8D25: ' + error.message);
    }
}

// \u5E94\u7528\u80CC\u666F\u8BBE\u7F6E
function applyBackgroundSettings(enabled, url, opacity, saveToCache = false) {
    const body = document.body;

    if (enabled && url) {
        // \u8BBE\u7F6E\u80CC\u666F\u56FE\u7247
        body.style.setProperty('--custom-background-url', \`url(\${url})\`);
        body.style.setProperty('--page-opacity', opacity / 100);
        body.classList.add('custom-background-enabled');


    } else {
        // \u79FB\u9664\u80CC\u666F\u56FE\u7247
        body.style.removeProperty('--custom-background-url');
        body.style.removeProperty('--page-opacity');
        body.classList.remove('custom-background-enabled');


    }

    // \u7F13\u5B58\u8BBE\u7F6E\u5230localStorage\uFF08\u53EF\u9009\uFF09
    if (saveToCache) {
        const settings = { enabled, url, opacity, timestamp: Date.now() };
        localStorage.setItem('background-settings-cache', JSON.stringify(settings));
    }
}

// \u5B9E\u65F6\u9884\u89C8\u900F\u660E\u5EA6\u53D8\u5316
function updateOpacityPreview() {
    const opacity = parseInt(document.getElementById('pageOpacity').value, 10);
    const enabled = document.getElementById('enableCustomBackground').checked;
    const url = document.getElementById('backgroundImageUrl').value.trim();

    // \u66F4\u65B0\u663E\u793A\u7684\u6570\u503C
    document.getElementById('opacityValue').textContent = opacity;

    // \u5B9E\u65F6\u9884\u89C8\uFF08\u4E0D\u4FDD\u5B58\uFF09
    if (enabled && url) {
        document.body.style.setProperty('--page-opacity', opacity / 100);

    }
}



// --- Global Settings Functions (VPS Report Interval) ---
async function loadDisplaySettings() {
    try {
        const settings = await apiRequest('/api/display-settings');
        displaySettings = {
            showServerSection: settings.showServerSection !== false,
            showSiteSection: settings.showSiteSection !== false
        };
    } catch (error) {
        displaySettings = { showServerSection: true, showSiteSection: true };
        showToast('danger', '\u52A0\u8F7D\u9996\u9875\u5C55\u793A\u8BBE\u7F6E\u5931\u8D25: ' + error.message);
    }

    const serverToggle = document.getElementById('showServerSectionToggle');
    const siteToggle = document.getElementById('showSiteSectionToggle');
    if (serverToggle) serverToggle.checked = displaySettings.showServerSection;
    if (siteToggle) siteToggle.checked = displaySettings.showSiteSection;
}

async function saveDisplaySettings(partialSettings) {
    const serverToggle = document.getElementById('showServerSectionToggle');
    const siteToggle = document.getElementById('showSiteSectionToggle');

    const nextSettings = {
        showServerSection: partialSettings.showServerSection ?? serverToggle.checked,
        showSiteSection: partialSettings.showSiteSection ?? siteToggle.checked
    };

    if (serverToggle) serverToggle.disabled = true;
    if (siteToggle) siteToggle.disabled = true;

    try {
        await apiRequest('/api/admin/display-settings', {
            method: 'POST',
            body: JSON.stringify(nextSettings)
        });

        displaySettings = nextSettings;
        showToast('success', '\u9996\u9875\u5C55\u793A\u8BBE\u7F6E\u5DF2\u4FDD\u5B58');
    } catch (error) {
        showToast('danger', '\u4FDD\u5B58\u9996\u9875\u5C55\u793A\u8BBE\u7F6E\u5931\u8D25: ' + error.message);
        await loadDisplaySettings();
    } finally {
        if (serverToggle) serverToggle.disabled = false;
        if (siteToggle) siteToggle.disabled = false;
    }
}

async function loadGlobalSettings() {
    try {
        const settings = await apiRequest('/api/admin/settings/vps-report-interval');
        if (settings && typeof settings.interval === 'number') {
            document.getElementById('vpsReportInterval').value = settings.interval;
        } else {
            document.getElementById('vpsReportInterval').value = 60; // Default if not set
        }
    } catch (error) {
                showToast('danger', '\u52A0\u8F7DVPS\u62A5\u544A\u95F4\u9694\u5931\u8D25: ' + error.message);
        document.getElementById('vpsReportInterval').value = 60; // Default on error
    }
}

async function saveVpsReportInterval() {
    const intervalInput = document.getElementById('vpsReportInterval');
    const interval = parseInt(intervalInput.value, 10);

    if (isNaN(interval) || interval < 1) { // Changed to interval < 1
        showToast('warning', 'VPS\u62A5\u544A\u95F4\u9694\u5FC5\u987B\u662F\u4E00\u4E2A\u5927\u4E8E\u6216\u7B49\u4E8E1\u7684\u6570\u5B57');
        return;
    }
    // Removed warning for interval < 10

    try {
        await apiRequest('/api/admin/settings/vps-report-interval', {
            method: 'POST',
            body: JSON.stringify({ interval: interval })
        });

        showToast('success', 'VPS\u6570\u636E\u66F4\u65B0\u9891\u7387\u5DF2\u6210\u529F\u4FDD\u5B58\u3002\u524D\u7AEF\u5237\u65B0\u95F4\u9694\u5DF2\u7ACB\u5373\u66F4\u65B0');

        // Immediately update the frontend refresh interval
        // Check if we're on a page that has VPS data updates running
        if (typeof initializeVpsDataUpdates === 'function') {
            try {
                await initializeVpsDataUpdates();
                            } catch (error) {
                            }
        }
    } catch (error) {
                showToast('danger', '\u4FDD\u5B58VPS\u62A5\u544A\u95F4\u9694\u5931\u8D25: ' + error.message);
    }
}

// --- \u81EA\u52A8\u6392\u5E8F\u529F\u80FD ---

// \u670D\u52A1\u5668\u81EA\u52A8\u6392\u5E8F
async function autoSortServers(sortBy) {
    try {
        await apiRequest('/api/admin/servers/auto-sort', {
            method: 'POST',
            body: JSON.stringify({ sortBy: sortBy, order: 'asc' })
        });

        // \u66F4\u65B0\u4E0B\u62C9\u83DC\u5355\u9009\u4E2D\u72B6\u6001
        updateServerSortDropdownSelection(sortBy);

        // \u91CD\u65B0\u52A0\u8F7D\u670D\u52A1\u5668\u5217\u8868
        await loadServerList();
        showToast('success', '\u670D\u52A1\u5668\u5DF2\u6309' + getSortDisplayName(sortBy) + '\u6392\u5E8F');

    } catch (error) {
                showToast('danger', '\u670D\u52A1\u5668\u81EA\u52A8\u6392\u5E8F\u5931\u8D25: ' + error.message);
    }
}

// \u7F51\u7AD9\u81EA\u52A8\u6392\u5E8F
async function autoSortSites(sortBy) {
    try {
        await apiRequest('/api/admin/sites/auto-sort', {
            method: 'POST',
            body: JSON.stringify({ sortBy: sortBy, order: 'asc' })
        });

        // \u66F4\u65B0\u4E0B\u62C9\u83DC\u5355\u9009\u4E2D\u72B6\u6001
        updateSiteSortDropdownSelection(sortBy);

        // \u91CD\u65B0\u52A0\u8F7D\u7F51\u7AD9\u5217\u8868
        await loadSiteList();
        showToast('success', '\u7F51\u7AD9\u5DF2\u6309' + getSortDisplayName(sortBy) + '\u6392\u5E8F');

    } catch (error) {
                showToast('danger', '\u7F51\u7AD9\u81EA\u52A8\u6392\u5E8F\u5931\u8D25: ' + error.message);
    }
}

// \u83B7\u53D6\u6392\u5E8F\u5B57\u6BB5\u7684\u663E\u793A\u540D\u79F0
function getSortDisplayName(sortBy) {
    const displayNames = {
        'custom': '\u81EA\u5B9A\u4E49',
        'name': '\u540D\u79F0',
        'status': '\u72B6\u6001',
        'created_at': '\u521B\u5EFA\u65F6\u95F4',
        'added_at': '\u6DFB\u52A0\u65F6\u95F4',
        'url': 'URL'
    };
    return displayNames[sortBy] || sortBy;
}

// \u66F4\u65B0\u670D\u52A1\u5668\u6392\u5E8F\u4E0B\u62C9\u83DC\u5355\u9009\u4E2D\u72B6\u6001
function updateServerSortDropdownSelection(selectedSortBy) {
    const dropdown = document.querySelector('#serverAutoSortDropdown + .dropdown-menu');
    if (!dropdown) return;

    // \u79FB\u9664\u6240\u6709active\u7C7B
    dropdown.querySelectorAll('.dropdown-item').forEach(item => {
        item.classList.remove('active');
    });

    // \u4E3A\u9009\u4E2D\u7684\u9879\u6DFB\u52A0active\u7C7B
    const selectedItem = dropdown.querySelector(\`[onclick="autoSortServers('\${selectedSortBy}')"]\`);
    if (selectedItem) {
        selectedItem.classList.add('active');
    }
}

// \u66F4\u65B0\u7F51\u7AD9\u6392\u5E8F\u4E0B\u62C9\u83DC\u5355\u9009\u4E2D\u72B6\u6001
function updateSiteSortDropdownSelection(selectedSortBy) {
    const dropdown = document.querySelector('#siteAutoSortDropdown + .dropdown-menu');
    if (!dropdown) return;

    // \u79FB\u9664\u6240\u6709active\u7C7B
    dropdown.querySelectorAll('.dropdown-item').forEach(item => {
        item.classList.remove('active');
    });

    // \u4E3A\u9009\u4E2D\u7684\u9879\u6DFB\u52A0active\u7C7B
    const selectedItem = dropdown.querySelector(\`[onclick="autoSortSites('\${selectedSortBy}')"]\`);
    if (selectedItem) {
        selectedItem.classList.add('active');
    }
}

// \u7BA1\u7406\u9875\u9762\u79FB\u52A8\u7AEF\u670D\u52A1\u5668\u5361\u7247\u6E32\u67D3\u51FD\u6570
function renderMobileAdminServerCards(servers) {
    const mobileContainer = document.getElementById('mobileAdminServerContainer');
    if (!mobileContainer) return;

    mobileContainer.innerHTML = '';

    if (!servers || servers.length === 0) {
        mobileContainer.innerHTML = '<div class="text-center p-3 text-muted">\u6682\u65E0\u670D\u52A1\u5668\u6570\u636E</div>';
        return;
    }

    servers.forEach(server => {
        const card = document.createElement('div');
        card.className = 'mobile-server-card';
        card.setAttribute('data-server-id', server.id);

        // \u72B6\u6001\u663E\u793A\u903B\u8F91\uFF08\u4E0EPC\u7AEF\u4E00\u81F4\uFF09
        let statusBadge = '<span class="badge bg-secondary">\u672A\u77E5</span>';
        let lastUpdateText = '\u4ECE\u672A';

        if (server.last_report) {
            const lastUpdate = new Date(server.last_report * 1000);
            lastUpdateText = lastUpdate.toLocaleString();

            // \u68C0\u67E5\u662F\u5426\u5728\u7EBF\uFF08\u6700\u540E\u62A5\u544A\u65F6\u95F4\u57285\u5206\u949F\u5185\uFF09
            const now = new Date();
            const diffMinutes = (now - lastUpdate) / (1000 * 60);

            if (diffMinutes <= 5) {
                statusBadge = '<span class="badge bg-success">\u5728\u7EBF</span>';
            } else {
                statusBadge = '<span class="badge bg-danger">\u79BB\u7EBF</span>';
            }
        }

        // \u5361\u7247\u5934\u90E8
        const cardHeader = document.createElement('div');
        cardHeader.className = 'mobile-card-header';
        cardHeader.innerHTML = \`
            <div class="mobile-card-header-left">
                \${statusBadge}
            </div>
            <h6 class="mobile-card-title text-center">\${server.name || '\u672A\u547D\u540D\u670D\u52A1\u5668'}</h6>
            <div class="mobile-card-header-right">
                <span class="me-2">\u663E\u793A</span>
                <div class="form-check form-switch d-inline-block">
                    <input class="form-check-input server-visibility-toggle" type="checkbox"
                           data-server-id="\${server.id}" \${server.is_public ? 'checked' : ''}>
                </div>
            </div>
        \`;

        // \u5361\u7247\u4E3B\u4F53
        const cardBody = document.createElement('div');
        cardBody.className = 'mobile-card-body';

        // \u63CF\u8FF0 - \u5355\u884C
        if (server.description) {
            const descRow = document.createElement('div');
            descRow.className = 'mobile-card-row';
            descRow.innerHTML = \`
                <span class="mobile-card-label">\u63CF\u8FF0</span>
                <span class="mobile-card-value">\${server.description}</span>
            \`;
            cardBody.appendChild(descRow);
        }



        // \u56DB\u4E2A\u6309\u94AE - \u4E24\u884C\u4E24\u5217\u5E03\u5C40
        const buttonsContainer = document.createElement('div');
        buttonsContainer.className = 'mobile-card-buttons-grid';
        buttonsContainer.innerHTML = \`
            <div class="d-flex gap-2 mb-2">
                <button class="btn btn-outline-secondary btn-sm flex-fill" onclick="showServerApiKey('\${server.id}')">
                    <i class="bi bi-key"></i> \u67E5\u770B\u5BC6\u94A5
                </button>
                <button class="btn btn-outline-info btn-sm flex-fill" onclick="copyVpsInstallScript('\${server.id}', '\${server.name}', this)">
                    <i class="bi bi-clipboard"></i> \u590D\u5236\u811A\u672C
                </button>
            </div>
            <div class="d-flex gap-2">
                <button class="btn btn-outline-primary btn-sm flex-fill" onclick="editServer('\${server.id}')">
                    <i class="bi bi-pencil"></i> \u7F16\u8F91
                </button>
                <button class="btn btn-outline-danger btn-sm flex-fill" onclick="deleteServer('\${server.id}')">
                    <i class="bi bi-trash"></i> \u5220\u9664
                </button>
            </div>
        \`;
        cardBody.appendChild(buttonsContainer);

        // \u6700\u540E\u66F4\u65B0\u65F6\u95F4 - \u5E95\u90E8\u5355\u884C\uFF08\u4E0EPC\u7AEF\u529F\u80FD\u4E00\u81F4\uFF09
        const lastUpdateRow = document.createElement('div');
        lastUpdateRow.className = 'mobile-card-row mobile-card-footer';
        lastUpdateRow.innerHTML = \`
            <span class="mobile-card-label">\u6700\u540E\u66F4\u65B0: \${lastUpdateText}</span>
        \`;
        cardBody.appendChild(lastUpdateRow);

        // \u7EC4\u88C5\u5361\u7247
        card.appendChild(cardHeader);
        card.appendChild(cardBody);

        mobileContainer.appendChild(card);
    });

    // \u4E3A\u79FB\u52A8\u7AEF\u663E\u793A\u5F00\u5173\u6DFB\u52A0\u4E8B\u4EF6\u76D1\u542C\u5668
    document.querySelectorAll('.server-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('change', function() {
            const serverId = this.dataset.serverId;
            const isPublic = this.checked;
            toggleServerVisibility(serverId, isPublic);
        });
    });
}

// \u5207\u6362\u670D\u52A1\u5668\u663E\u793A\u72B6\u6001
async function toggleServerVisibility(serverId, isPublic) {
    try {
        const toggle = document.querySelector(\`.server-visibility-toggle[data-server-id="\${serverId}"]\`);
        if (toggle) {
            toggle.disabled = true;
            toggle.style.opacity = '0.6';
        }

        await apiRequest(\`/api/admin/servers/\${serverId}/visibility\`, {
            method: 'POST',
            body: JSON.stringify({ is_public: isPublic })
        });

        // \u66F4\u65B0\u672C\u5730\u6570\u636E
        const serverIndex = serverList.findIndex(s => s.id === serverId);
        if (serverIndex !== -1) {
            serverList[serverIndex].is_public = isPublic;
        }

        if (toggle) {
            toggle.disabled = false;
            toggle.style.opacity = '1';
        }

        showToast('success', '\u670D\u52A1\u5668\u663E\u793A\u72B6\u6001\u5DF2' + (isPublic ? '\u5F00\u542F' : '\u5173\u95ED'));

    } catch (error) {
                // \u6062\u590D\u5F00\u5173\u72B6\u6001
        const toggle = document.querySelector(\`.server-visibility-toggle[data-server-id="\${serverId}"]\`);
        if (toggle) {
            toggle.checked = !isPublic;
            toggle.disabled = false;
            toggle.style.opacity = '1';
        }

        showToast('danger', '\u5207\u6362\u663E\u793A\u72B6\u6001\u5931\u8D25: ' + error.message);
    }
}

// \u7BA1\u7406\u9875\u9762\u79FB\u52A8\u7AEF\u7F51\u7AD9\u5361\u7247\u6E32\u67D3\u51FD\u6570
function renderMobileAdminSiteCards(sites) {
    const mobileContainer = document.getElementById('mobileAdminSiteContainer');
    if (!mobileContainer) return;

    mobileContainer.innerHTML = '';

    // \u6DFB\u52A0\u5C45\u4E2D\u7684\u6392\u5E8F\u548C\u6DFB\u52A0\u7F51\u7AD9\u6309\u94AE
    const mobileActionsContainer = document.createElement('div');
    mobileActionsContainer.className = 'text-center mb-3';
    mobileActionsContainer.innerHTML = \`
        <div class="d-flex gap-2 justify-content-center">
            <div class="dropdown">
                <button class="btn btn-outline-secondary dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false">
                    <i class="bi bi-sort-alpha-down"></i> \u81EA\u52A8\u6392\u5E8F
                </button>
                <ul class="dropdown-menu">
                    <li><a class="dropdown-item active" href="#" onclick="autoSortSites('custom')">\u81EA\u5B9A\u4E49\u6392\u5E8F</a></li>
                    <li><a class="dropdown-item" href="#" onclick="autoSortSites('name')">\u6309\u540D\u79F0\u6392\u5E8F</a></li>
                    <li><a class="dropdown-item" href="#" onclick="autoSortSites('url')">\u6309URL\u6392\u5E8F</a></li>
                    <li><a class="dropdown-item" href="#" onclick="autoSortSites('status')">\u6309\u72B6\u6001\u6392\u5E8F</a></li>
                </ul>
            </div>
            <button id="addSiteBtnMobile" class="btn btn-success" onclick="showSiteModal()">
                <i class="bi bi-plus-circle"></i> \u6DFB\u52A0\u76D1\u63A7\u7F51\u7AD9
            </button>
        </div>
    \`;
    mobileContainer.appendChild(mobileActionsContainer);

    if (!sites || sites.length === 0) {
        const noDataDiv = document.createElement('div');
        noDataDiv.className = 'text-center p-3 text-muted';
        noDataDiv.textContent = '\u6682\u65E0\u76D1\u63A7\u7F51\u7AD9\u6570\u636E';
        mobileContainer.appendChild(noDataDiv);
        return;
    }

    sites.forEach(site => {
        const card = document.createElement('div');
        card.className = 'mobile-site-card';

        const statusInfo = getSiteStatusBadge(site.last_status);
        const lastCheckTime = site.last_checked ? new Date(site.last_checked * 1000).toLocaleString() : '\u4ECE\u672A';
        const responseTime = site.last_response_time_ms !== null ? \`\${site.last_response_time_ms} ms\` : '-';

        // \u5361\u7247\u5934\u90E8 - \u5B8C\u5168\u53C2\u8003\u670D\u52A1\u5668\u5361\u7247\u5E03\u5C40\uFF1A\u72B6\u6001\u5728\u5DE6\u4E0A\u89D2\uFF0C\u7F51\u7AD9\u540D\u5728\u4E2D\u95F4\uFF0C\u663E\u793A\u5F00\u5173\u5728\u53F3\u4E0A\u89D2
        const cardHeader = document.createElement('div');
        cardHeader.className = 'mobile-card-header';
        cardHeader.innerHTML = \`
            <div class="mobile-card-header-left">
                <span class="badge \${statusInfo.class}">\${statusInfo.text}</span>
            </div>
            <h6 class="mobile-card-title text-center">\${site.name || '\u672A\u547D\u540D\u7F51\u7AD9'}</h6>
            <div class="mobile-card-header-right">
                <span class="me-2">\u663E\u793A</span>
                <div class="form-check form-switch d-inline-block">
                    <input class="form-check-input site-visibility-toggle" type="checkbox"
                           data-site-id="\${site.id}" \${site.is_public ? 'checked' : ''}>
                </div>
            </div>
        \`;

        // \u5361\u7247\u4E3B\u4F53
        const cardBody = document.createElement('div');
        cardBody.className = 'mobile-card-body';

        // URL \u548C\u7F51\u7AD9\u94FE\u63A5 - \u5355\u884C
        const urlRow = document.createElement('div');
        urlRow.className = 'mobile-card-row';
        urlRow.innerHTML = \`
            <span class="mobile-card-label" style="word-break: break-all;">
                URL: \${site.url}<a href="\${site.url}" target="_blank" rel="noopener noreferrer" class="text-decoration-none" style="margin-left: 4px;"><i class="bi bi-box-arrow-up-right"></i></a>
            </span>
        \`;
        cardBody.appendChild(urlRow);



        // \u6700\u540E\u68C0\u67E5 - \u5355\u884C
        const lastCheckRow = document.createElement('div');
        lastCheckRow.className = 'mobile-card-row';
        lastCheckRow.innerHTML = \`
            <span class="mobile-card-label">\u6700\u540E\u68C0\u67E5: \${lastCheckTime}</span>
        \`;
        cardBody.appendChild(lastCheckRow);

        // \u64CD\u4F5C\u6309\u94AE - \u7F16\u8F91\u548C\u5220\u9664
        const actionsRow = document.createElement('div');
        actionsRow.className = 'mobile-card-row';
        actionsRow.innerHTML = \`
            <div class="d-flex gap-2 w-100">
                <button class="btn btn-outline-primary btn-sm flex-fill" onclick="editSite('\${site.id}')">
                    <i class="bi bi-pencil"></i> \u7F16\u8F91
                </button>
                <button class="btn btn-outline-danger btn-sm flex-fill" onclick="deleteSite('\${site.id}')">
                    <i class="bi bi-trash"></i> \u5220\u9664
                </button>
            </div>
        \`;
        cardBody.appendChild(actionsRow);

        // \u7EC4\u88C5\u5361\u7247
        card.appendChild(cardHeader);
        card.appendChild(cardBody);

        mobileContainer.appendChild(card);
    });

    // \u4E3A\u79FB\u52A8\u7AEF\u7F51\u7AD9\u663E\u793A\u5F00\u5173\u6DFB\u52A0\u4E8B\u4EF6\u76D1\u542C\u5668
    document.querySelectorAll('.site-visibility-toggle').forEach(toggle => {
        toggle.addEventListener('change', function() {
            const siteId = this.dataset.siteId;
            const isPublic = this.checked;
            toggleSiteVisibility(siteId, isPublic);
        });
    });
}

// \u5207\u6362\u7F51\u7AD9\u663E\u793A\u72B6\u6001
async function toggleSiteVisibility(siteId, isPublic) {
    try {
        const toggle = document.querySelector(\`.site-visibility-toggle[data-site-id="\${siteId}"]\`);
        if (toggle) {
            toggle.disabled = true;
            toggle.style.opacity = '0.6';
        }

        await apiRequest(\`/api/admin/sites/\${siteId}/visibility\`, {
            method: 'POST',
            body: JSON.stringify({ is_public: isPublic })
        });

        // \u66F4\u65B0\u672C\u5730\u6570\u636E
        const siteIndex = siteList.findIndex(s => s.id === siteId);
        if (siteIndex !== -1) {
            siteList[siteIndex].is_public = isPublic;
        }

        if (toggle) {
            toggle.disabled = false;
            toggle.style.opacity = '1';
        }

        showToast('success', '\u7F51\u7AD9\u663E\u793A\u72B6\u6001\u5DF2' + (isPublic ? '\u5F00\u542F' : '\u5173\u95ED'));

    } catch (error) {
                // \u6062\u590D\u5F00\u5173\u72B6\u6001
        const toggle = document.querySelector(\`.site-visibility-toggle[data-site-id="\${siteId}"]\`);
        if (toggle) {
            toggle.checked = !isPublic;
            toggle.disabled = false;
            toggle.style.opacity = '1';
        }

        showToast('danger', '\u5207\u6362\u663E\u793A\u72B6\u6001\u5931\u8D25: ' + error.message);
    }
}

// \u79FB\u52A8\u7AEF\u67E5\u770B\u670D\u52A1\u5668API\u5BC6\u94A5
function showServerApiKey(serverId) {
    viewApiKey(serverId);
}

// ==================== \u5168\u5C40\u80CC\u666F\u8BBE\u7F6E\u540C\u6B65\u529F\u80FD ====================

// \u76D1\u542Cstorage\u4E8B\u4EF6\uFF0C\u5B9E\u73B0\u8DE8\u9875\u9762\u8BBE\u7F6E\u540C\u6B65
window.addEventListener('storage', function(e) {
    if (e.key === 'background-settings-cache' && e.newValue) {
        try {
            const newSettings = JSON.parse(e.newValue);
            // \u4F7F\u7528\u7BA1\u7406\u9875\u9762\u7684\u80CC\u666F\u8BBE\u7F6E\u5E94\u7528\u51FD\u6570
            applyBackgroundSettings(newSettings.enabled, newSettings.url, newSettings.opacity, false);
                    } catch (error) {
                    }
    }
});

// \u9875\u9762\u52A0\u8F7D\u65F6\u4E5F\u68C0\u67E5\u5E76\u5E94\u7528\u7F13\u5B58\u7684\u80CC\u666F\u8BBE\u7F6E
document.addEventListener('DOMContentLoaded', function() {
    // \u5EF6\u8FDF\u6267\u884C\uFF0C\u786E\u4FDDloadBackgroundSettings()\u5148\u6267\u884C
    setTimeout(function() {
        const cached = localStorage.getItem('background-settings-cache');
        if (cached) {
            try {
                const cachedData = JSON.parse(cached);
                const now = Date.now();
                const cacheAge = now - cachedData.timestamp;
                const CACHE_DURATION = 5 * 60 * 1000; // 5\u5206\u949F\u7F13\u5B58

                if (cacheAge < CACHE_DURATION) {
                    // \u7F13\u5B58\u6709\u6548\uFF0C\u786E\u4FDD\u8BBE\u7F6E\u5DF2\u5E94\u7528
                    applyBackgroundSettings(cachedData.enabled, cachedData.url, cachedData.opacity, false);
                                    }
            } catch (error) {
                            }
        }
    }, 100);
});
`;
}
__name(getAdminJs, "getAdminJs");

// D:/Service/nodejs/node_global/node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// D:/Service/nodejs/node_global/node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    return Response.json(error, {
      status: 500,
      headers: { "MF-Experimental-Error-Stack": "true" }
    });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-oxyRVo/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = worker_default;

// D:/Service/nodejs/node_global/node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-oxyRVo/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=worker.js.map
