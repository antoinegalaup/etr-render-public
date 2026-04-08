import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express from "express";
import { fileURLToPath } from "node:url";
import {
  validateSyncEvent,
  verifySyncRequest
} from "./lib/syncIngest.js";
import { ControlPlaneService } from "./lib/controlPlaneService.js";
import { getPublicPropertyCards, getPublicPropertyDetail } from "./lib/publicSiteCatalog.js";
import { createEtrGuestChatService } from "./lib/etrGuestChatService.js";
import { StaffAuthService } from "./lib/staffAuthService.js";
import { StaffOperationsService } from "./lib/staffOperationsService.js";
import { registerStaffRoutes } from "./routes/staffRoutes.js";

function readManifest(filePath) {
  const content = fs.readFileSync(filePath, "utf8");
  return JSON.parse(content);
}

function jsonOrText(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function secretsEqual(expected, provided) {
  const expectedBuffer = Buffer.from(`${expected || ""}`);
  const providedBuffer = Buffer.from(`${provided || ""}`);
  if (!expectedBuffer.length || expectedBuffer.length !== providedBuffer.length) {
    return false;
  }
  return crypto.timingSafeEqual(expectedBuffer, providedBuffer);
}

function buildInternalRouteGuard(internalApiSecret) {
  return function requireInternalRouteAuth(req, res, next) {
    if (!internalApiSecret) {
      return res.status(503).json({ error: "internal_api_secret_not_configured" });
    }
    const providedSecret = `${req.header("x-internal-api-secret") || ""}`.trim();
    if (!secretsEqual(internalApiSecret, providedSecret)) {
      return res.status(401).json({ error: "internal_api_access_denied" });
    }
    return next();
  };
}

function hasInternalRouteAccess(req, internalApiSecret) {
  if (!internalApiSecret) {
    return false;
  }
  const providedSecret = `${req.header("x-internal-api-secret") || ""}`.trim();
  return secretsEqual(internalApiSecret, providedSecret);
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(`${value ?? ""}`, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function getRateLimitClientKey(req) {
  const forwardedFor = `${req.header("x-forwarded-for") || ""}`
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)[0];
  return (
    forwardedFor ||
    `${req.ip || ""}`.trim() ||
    `${req.socket?.remoteAddress || ""}`.trim() ||
    "unknown"
  );
}

function buildPublicRateLimiter({
  keyPrefix,
  maxRequests,
  windowMs,
  internalApiSecret,
  enabled
}) {
  const buckets = new Map();
  let lastSweepAt = 0;

  return function publicRateLimiter(req, res, next) {
    if (!enabled || hasInternalRouteAccess(req, internalApiSecret)) {
      return next();
    }

    const now = Date.now();
    if (now - lastSweepAt >= windowMs) {
      for (const [bucketKey, bucket] of buckets.entries()) {
        if (!bucket || bucket.resetAt <= now) {
          buckets.delete(bucketKey);
        }
      }
      lastSweepAt = now;
    }

    const clientKey = `${keyPrefix}:${getRateLimitClientKey(req)}`;
    const current = buckets.get(clientKey);
    const bucket =
      current && current.resetAt > now
        ? current
        : { count: 0, resetAt: now + windowMs };

    bucket.count += 1;
    buckets.set(clientKey, bucket);

    const remaining = Math.max(0, maxRequests - bucket.count);
    res.setHeader("X-RateLimit-Limit", `${maxRequests}`);
    res.setHeader("X-RateLimit-Remaining", `${remaining}`);
    res.setHeader("X-RateLimit-Reset", `${Math.ceil(bucket.resetAt / 1000)}`);

    if (bucket.count > maxRequests) {
      const retryAfterSeconds = Math.max(1, Math.ceil((bucket.resetAt - now) / 1000));
      res.setHeader("Retry-After", `${retryAfterSeconds}`);
      return res.status(429).json({ error: "rate_limit_exceeded" });
    }

    return next();
  };
}

const LOCAL_PUBLIC_SITE_ORIGINS = new Set([
  "http://127.0.0.1:3000",
  "http://localhost:3000",
  "http://127.0.0.1:3002",
  "http://localhost:3002"
]);

function applyLocalPublicSiteCors(req, res) {
  const origin = String(req.headers.origin || "").trim();
  if (!LOCAL_PUBLIC_SITE_ORIGINS.has(origin)) {
    return;
  }
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "content-type");
}

export function createApp({
  rootDir = process.cwd(),
  manifestPath = path.resolve(process.cwd(), "architecture.json"),
  wordpressBaseUrl = process.env.WORDPRESS_BASE_URL || "",
  wordpressPublicBaseUrl =
    process.env.WORDPRESS_PUBLIC_BASE_URL ||
    process.env.NEXT_PUBLIC_WORDPRESS_ASSET_BASE_URL ||
    process.env.WORDPRESS_BASE_URL ||
    "",
  enableExecuteProxy = process.env.ENABLE_EXECUTE_PROXY === "true",
  syncIngestEnabled = process.env.SYNC_INGEST_ENABLED === "true",
  syncDatabaseUrl = process.env.SYNC_DATABASE_URL || process.env.DATABASE_URL || "",
  syncSchema = process.env.SYNC_SCHEMA || "sync",
  syncSecret = process.env.SYNC_HMAC_SECRET || "",
  syncClockSkewSeconds = Number.parseInt(process.env.SYNC_MAX_CLOCK_SKEW_SECONDS || "300", 10),
  exposeArchitectureRoutes = process.env.EXPOSE_ARCHITECTURE_ROUTES === "true",
  internalApiSecret =
    process.env.INTERNAL_API_SECRET ||
    process.env.PORTAL_SESSION_SECRET ||
    process.env.WORDPRESS_COMMAND_SECRET ||
    process.env.SYNC_HMAC_SECRET ||
    "",
  publicRateLimitEnabled = `${process.env.PUBLIC_RATE_LIMIT_ENABLED || "true"}`
    .trim()
    .toLowerCase() !== "false",
  publicRateLimitWindowMs = parsePositiveInt(process.env.PUBLIC_RATE_LIMIT_WINDOW_MS, 60000),
  publicRateLimitPropertiesMax = parsePositiveInt(process.env.PUBLIC_RATE_LIMIT_PROPERTIES_MAX, 120),
  publicRateLimitAvailabilityMax = parsePositiveInt(process.env.PUBLIC_RATE_LIMIT_AVAILABILITY_MAX, 60),
  publicRateLimitQuotesMax = parsePositiveInt(process.env.PUBLIC_RATE_LIMIT_QUOTES_MAX, 20),
  publicRateLimitCheckoutMax = parsePositiveInt(process.env.PUBLIC_RATE_LIMIT_CHECKOUT_MAX, 20),
  publicRateLimitChatMax = parsePositiveInt(process.env.PUBLIC_RATE_LIMIT_CHAT_MAX, 12),
  publicRateLimitPortalMax = parsePositiveInt(process.env.PUBLIC_RATE_LIMIT_PORTAL_MAX, 10),
  syncService = null,
  staffAuthService = null,
  staffOperationsService = null,
  guestChatService = null,
  guestChatResponsesClient = null,
  guestChatWelcomeBookStateReader = null
} = {}) {
  const app = express();
  app.use(
    express.json({
      limit: "1mb",
      verify: (req, _res, buf) => {
        req.rawBody = buf.toString("utf8");
      }
    })
  );
  app.use("/channel/support/chat", (req, res, next) => {
    applyLocalPublicSiteCors(req, res);
    if (req.method === "OPTIONS") {
      return res.status(204).end();
    }
    return next();
  });

  let ingestService = syncService;
  let publicGuestChatService = guestChatService;
  let internalStaffAuthService = staffAuthService;
  let internalStaffOperationsService = staffOperationsService;
  if (!ingestService && syncDatabaseUrl) {
    ingestService = new ControlPlaneService({
      databaseUrl: syncDatabaseUrl,
      schema: syncSchema,
      wordpressBaseUrl,
      wordpressPublicBaseUrl
    });
  }

  if (!internalStaffAuthService) {
    const candidate = new StaffAuthService();
    if (candidate.isConfigured()) {
      internalStaffAuthService = candidate;
    }
  }

  if (!internalStaffOperationsService && ingestService?.connect) {
    internalStaffOperationsService = new StaffOperationsService({
      controlPlaneService: ingestService,
      syncSchema,
      opsSchema: "ops"
    });
  }

  function getPublicGuestChatService() {
    if (publicGuestChatService) {
      return publicGuestChatService;
    }
    publicGuestChatService = createEtrGuestChatService({
      rootDir,
      openAiApiKey: process.env.OPENAI_API_KEY || "",
      model: process.env.OPENAI_CHAT_MODEL || "gpt-5.4-mini",
      wordpressPublicBaseUrl,
      liveBookingService: ingestService,
      welcomeBookStateReader: guestChatWelcomeBookStateReader,
      responsesClient: guestChatResponsesClient || undefined
    });
    return publicGuestChatService;
  }

  const publicPropertyRateLimit = buildPublicRateLimiter({
    keyPrefix: "channel-properties",
    maxRequests: publicRateLimitPropertiesMax,
    windowMs: publicRateLimitWindowMs,
    internalApiSecret,
    enabled: publicRateLimitEnabled
  });
  const publicAvailabilityRateLimit = buildPublicRateLimiter({
    keyPrefix: "channel-availability",
    maxRequests: publicRateLimitAvailabilityMax,
    windowMs: publicRateLimitWindowMs,
    internalApiSecret,
    enabled: publicRateLimitEnabled
  });
  const publicQuoteRateLimit = buildPublicRateLimiter({
    keyPrefix: "channel-quotes",
    maxRequests: publicRateLimitQuotesMax,
    windowMs: publicRateLimitWindowMs,
    internalApiSecret,
    enabled: publicRateLimitEnabled
  });
  const publicCheckoutRateLimit = buildPublicRateLimiter({
    keyPrefix: "channel-checkout",
    maxRequests: publicRateLimitCheckoutMax,
    windowMs: publicRateLimitWindowMs,
    internalApiSecret,
    enabled: publicRateLimitEnabled
  });
  const publicChatRateLimit = buildPublicRateLimiter({
    keyPrefix: "channel-chat",
    maxRequests: publicRateLimitChatMax,
    windowMs: publicRateLimitWindowMs,
    internalApiSecret,
    enabled: publicRateLimitEnabled
  });
  const publicPortalRateLimit = buildPublicRateLimiter({
    keyPrefix: "channel-portal",
    maxRequests: publicRateLimitPortalMax,
    windowMs: publicRateLimitWindowMs,
    internalApiSecret,
    enabled: publicRateLimitEnabled
  });

  app.get("/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  app.get("/", (_req, res) => {
    res.json({
      service: "etr-api",
      status: "ok",
      health: "/health"
    });
  });

  app.get("/architecture", (_req, res) => {
    if (!exposeArchitectureRoutes) {
      return res.status(404).json({ error: "not_found" });
    }
    res.json(readManifest(manifestPath));
  });

  app.get("/architecture/plugins", (_req, res) => {
    if (!exposeArchitectureRoutes) {
      return res.status(404).json({ error: "not_found" });
    }
    res.json(readManifest(manifestPath).plugins || []);
  });

  app.get("/architecture/pages", (_req, res) => {
    if (!exposeArchitectureRoutes) {
      return res.status(404).json({ error: "not_found" });
    }
    res.json(readManifest(manifestPath).pages || []);
  });

  app.get("/architecture/rest", (_req, res) => {
    if (!exposeArchitectureRoutes) {
      return res.status(404).json({ error: "not_found" });
    }
    res.json(readManifest(manifestPath).rest_routes || []);
  });

  app.post("/execute", async (req, res) => {
    if (!enableExecuteProxy) {
      return res.status(403).json({
        error:
          "Execute proxy is disabled. Set ENABLE_EXECUTE_PROXY=true to enable explicitly."
      });
    }

    if (!wordpressBaseUrl) {
      return res.status(500).json({
        error: "WORDPRESS_BASE_URL is required when execute proxy is enabled."
      });
    }

    const { target, method = "GET", headers = {}, body } = req.body || {};
    if (!target || typeof target !== "string" || !target.startsWith("/wp-json/")) {
      return res.status(400).json({
        error: "target must be a /wp-json/* route"
      });
    }

    const response = await fetch(`${wordpressBaseUrl}${target}`, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined
    });

    const raw = await response.text();
    return res.status(response.status).json({
      status: response.status,
      ok: response.ok,
      data: jsonOrText(raw)
    });
  });

  app.post("/sync/events", async (req, res) => {
    if (!syncIngestEnabled) {
      return res.status(403).json({
        error: "Sync ingest is disabled. Set SYNC_INGEST_ENABLED=true to enable."
      });
    }
    if (!ingestService) {
      return res.status(500).json({
        error: "SYNC_DATABASE_URL (or DATABASE_URL) is required for sync ingest."
      });
    }

    const signature = req.header("x-sync-signature") || "";
    const timestamp = req.header("x-sync-timestamp") || "";
    const nonce = req.header("x-sync-nonce") || "";
    const rawBody = req.rawBody || JSON.stringify(req.body || {});

    const verified = verifySyncRequest({
      secret: syncSecret,
      signature,
      timestamp,
      nonce,
      rawBody,
      maxSkewSeconds: Number.isNaN(syncClockSkewSeconds) ? 300 : syncClockSkewSeconds
    });
    if (!verified.ok) {
      return res.status(401).json({ error: `sync_auth_failed:${verified.reason}` });
    }

    try {
      const freshNonce = await ingestService.assertFreshNonce(nonce);
      if (!freshNonce) {
        return res.status(409).json({ error: "sync_replay_detected:nonce_reused" });
      }
    } catch (error) {
      return res.status(500).json({ error: `sync_nonce_check_failed:${error?.message || error}` });
    }

    const event = req.body || {};
    const validation = validateSyncEvent(event);
    if (!validation.valid) {
      return res.status(400).json({ error: `invalid_event:${validation.reason}` });
    }

    try {
      const result = await ingestService.processEvent(event);
      return res.status(202).json(result);
    } catch (error) {
      return res.status(500).json({ error: `sync_ingest_failed:${error?.message || error}` });
    }
  });

  app.post("/channel/quote", publicQuoteRateLimit, async (req, res) => {
    if (!ingestService?.createBookingQuote) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.createBookingQuote(req.body || {});
      return res.status(201).json(result);
    } catch (error) {
      return res.status(400).json({ error: `quote_failed:${error?.message || error}` });
    }
  });

  app.post("/channel/quotes", publicQuoteRateLimit, async (req, res) => {
    if (!ingestService?.createBookingQuote) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.createBookingQuote(req.body || {});
      return res.status(201).json(result);
    } catch (error) {
      return res.status(400).json({ error: `quote_failed:${error?.message || error}` });
    }
  });

  app.get("/channel/properties", publicPropertyRateLimit, async (_req, res) => {
    try {
      const properties = ingestService?.listPublicProperties
        ? await ingestService.listPublicProperties()
        : getPublicPropertyCards({ wordpressBaseUrl: wordpressPublicBaseUrl });
      return res.status(200).json(properties);
    } catch (error) {
      return res.status(500).json({ error: `properties_failed:${error?.message || error}` });
    }
  });

  app.get("/channel/properties/:roomId", publicPropertyRateLimit, async (req, res) => {
    try {
      const property = ingestService?.getPublicProperty
        ? await ingestService.getPublicProperty(req.params.roomId)
        : getPublicPropertyDetail(req.params.roomId, { wordpressBaseUrl: wordpressPublicBaseUrl });
      if (!property) {
        return res.status(404).json({ error: "property_not_found" });
      }
      return res.status(200).json(property);
    } catch (error) {
      return res.status(500).json({ error: `property_failed:${error?.message || error}` });
    }
  });

  app.post("/channel/support/chat", publicChatRateLimit, async (req, res) => {
    try {
      const result = await getPublicGuestChatService().reply(req.body || {});
      if (!result.ok) {
        return res.status(result.status || 400).json({
          error: result.error || "The AI concierge is unavailable right now."
        });
      }
      return res.status(result.status || 200).json({
        reply: result.message,
        property_slug: result.propertySlug,
        property_name: result.propertyName,
        route: result.route,
        confidence: result.confidence,
        source_citations: result.sourceCitations,
        needs_clarification: result.needsClarification,
        clarification: result.clarification || null,
        booking_prefill: result.bookingPrefill || null,
        available_actions: result.availableActions || []
      });
    } catch (error) {
      return res.status(500).json({ error: `guest_chat_failed:${error?.message || error}` });
    }
  });

  app.post("/channel/portal/login", publicPortalRateLimit, async (req, res) => {
    if (!ingestService?.getGuestPortalReservation) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.getGuestPortalReservation(req.body || {});
      if (!result) {
        return res.status(404).json({ error: "guest_portal_not_found" });
      }
      return res.status(200).json(result);
    } catch (error) {
      return res.status(400).json({ error: `portal_login_failed:${error?.message || error}` });
    }
  });

  app.post("/channel/availability", publicAvailabilityRateLimit, async (req, res) => {
    if (!ingestService?.getAvailabilityPreview) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.getAvailabilityPreview(req.body || {});
      return res.status(200).json(result);
    } catch (error) {
      return res.status(400).json({ error: `availability_failed:${error?.message || error}` });
    }
  });

  app.get("/channel/availability/calendar", publicAvailabilityRateLimit, async (req, res) => {
    if (!ingestService?.getAvailabilityCalendar) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.getAvailabilityCalendar({
        room_id: req.query.room_id,
        month_start: req.query.month_start,
        months: req.query.months
      });
      return res.status(200).json(result);
    } catch (error) {
      return res
        .status(400)
        .json({ error: `availability_calendar_failed:${error?.message || error}` });
    }
  });

  app.post("/channel/checkout/session", publicCheckoutRateLimit, async (req, res) => {
    if (!ingestService?.createCheckoutSessionForQuote) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.createCheckoutSessionForQuote(req.body || {});
      return res.status(201).json(result);
    } catch (error) {
      return res.status(400).json({ error: `checkout_failed:${error?.message || error}` });
    }
  });

  app.post("/webhooks/stripe", async (req, res) => {
    if (!ingestService?.ingestStripeWebhook) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.ingestStripeWebhook({
        rawBody: req.rawBody || JSON.stringify(req.body || {}),
        signatureHeader: req.header("stripe-signature") || ""
      });
      return res.status(202).json(result);
    } catch (error) {
      return res.status(400).json({ error: `stripe_webhook_failed:${error?.message || error}` });
    }
  });

  app.post("/webhooks/crypto", async (req, res) => {
    if (!ingestService?.ingestCryptoWebhook) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.ingestCryptoWebhook({
        rawBody: req.rawBody || JSON.stringify(req.body || {}),
        signatureHeader:
          req.header("x-nowpayments-sig") ||
          req.header("x-crypto-signature") ||
          req.header("x-signature") ||
          ""
      });
      return res.status(202).json(result);
    } catch (error) {
      return res.status(400).json({ error: `crypto_webhook_failed:${error?.message || error}` });
    }
  });

  const requireInternalRouteAuth = buildInternalRouteGuard(internalApiSecret);

  app.post("/meta/spend/sync", requireInternalRouteAuth, async (req, res) => {
    if (!ingestService?.upsertMetaSpend) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
      const result = await ingestService.upsertMetaSpend(rows);
      return res.status(202).json(result);
    } catch (error) {
      return res.status(400).json({ error: `meta_spend_sync_failed:${error?.message || error}` });
    }
  });

  app.post("/meta/audiences/export", requireInternalRouteAuth, async (req, res) => {
    if (!ingestService?.exportAudienceSegments) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const segments = Array.isArray(req.body?.segments) ? req.body.segments : [];
      const result = await ingestService.exportAudienceSegments(segments);
      if (req.body?.format === "csv") {
        res.setHeader("content-type", "text/csv; charset=utf-8");
        return res.status(200).send(result.csv);
      }
      return res.status(200).json(result);
    } catch (error) {
      return res.status(400).json({ error: `audience_export_failed:${error?.message || error}` });
    }
  });

  app.get("/channel/reservations/:externalBookingId", requireInternalRouteAuth, async (req, res) => {
    if (!ingestService?.getReservationStatus) {
      return res.status(503).json({ error: "control_plane_service_unavailable" });
    }
    try {
      const result = await ingestService.getReservationStatus(req.params.externalBookingId);
      if (!result) {
        return res.status(404).json({ error: "reservation_not_found" });
      }
      return res.status(200).json(result);
    } catch (error) {
      return res.status(400).json({ error: `reservation_failed:${error?.message || error}` });
    }
  });

  registerStaffRoutes(app, {
    staffAuthService: internalStaffAuthService,
    staffOperationsService: internalStaffOperationsService
  });

  return app;
}

const thisFile = fileURLToPath(import.meta.url);

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(thisFile)) {
  const port = Number.parseInt(process.env.PORT || "3000", 10);
  const app = createApp();
  app.listen(port, () => {
    console.log(`Wrapper API listening on http://localhost:${port}`);
  });
}
