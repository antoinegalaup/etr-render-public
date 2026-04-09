import express from "express";
import { setTimeout as sleep } from "node:timers/promises";

function readBearerToken(req) {
  const header = `${req.header("authorization") || ""}`.trim();
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

function buildActor(user = {}) {
  const staffRole = normalizedStaffRole(user);
  return {
    userId: user.id || null,
    email: user.email || "",
    source: "user",
    staffRole,
    isElevatedStaff: Boolean(staffRole && staffRole !== "employee")
  };
}

function normalizedStaffRole(user = {}) {
  return `${user.staff_role || user.staffRole || ""}`.trim().toLowerCase();
}

function assertWriteAccess(user = {}) {
  if (normalizedStaffRole(user) === "employee") {
    throw new Error("staff_write_access_denied");
  }
}

function errorStatus(error) {
  const message = `${error?.message || error || ""}`;
  if (
    message.includes("write_access_denied") ||
    message.includes("thread_access_denied") ||
    message.includes("action_access_denied")
  ) {
    return 403;
  }
  if (
    message.includes("access_denied") ||
    message.includes("auth_request_failed") ||
    message.includes("missing_staff_bearer_token")
  ) {
    return 401;
  }
  if (
    message.includes("validation_failed") ||
    message.includes("required") ||
    message.includes("invalid_") ||
    message.includes("not_found") ||
    message.includes("patch_empty")
  ) {
    return 400;
  }
  return 500;
}

function buildAuthRateLimiter({ enabled = true, windowMs = 60000, maxRequests = 8 } = {}) {
  const buckets = new Map();
  let lastSweepAt = 0;

  function clientKeyFor(req) {
    const ip =
      `${req.ip || ""}`.trim() ||
      `${req.socket?.remoteAddress || ""}`.trim() ||
      "unknown";
    const email = `${req.body?.email || ""}`.trim().toLowerCase();
    return email ? `${ip}:${email}` : ip;
  }

  return function authRateLimiter(req, res, next) {
    if (!enabled) {
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

    const key = clientKeyFor(req);
    const current = buckets.get(key);
    const bucket =
      current && current.resetAt > now
        ? current
        : { count: 0, resetAt: now + windowMs };

    bucket.count += 1;
    buckets.set(key, bucket);

    res.setHeader("X-RateLimit-Limit", `${maxRequests}`);
    res.setHeader("X-RateLimit-Remaining", `${Math.max(0, maxRequests - bucket.count)}`);
    res.setHeader("X-RateLimit-Reset", `${Math.ceil(bucket.resetAt / 1000)}`);

    if (bucket.count > maxRequests) {
      const retryAfterSeconds = Math.max(1, Math.ceil((bucket.resetAt - now) / 1000));
      res.setHeader("Retry-After", `${retryAfterSeconds}`);
      return res.status(429).json({ error: "staff_auth_rate_limit_exceeded" });
    }

    return next();
  };
}

export function registerStaffRoutes(
  app,
  { staffAuthService, staffOperationsService, authRateLimit = {} } = {}
) {
  if (!staffAuthService || !staffOperationsService) {
    return;
  }

  const router = express.Router();
  const authRateLimiter = buildAuthRateLimiter(authRateLimit);

  router.post("/session", authRateLimiter, async (req, res) => {
    try {
      const session = await staffAuthService.signIn({
        email: req.body?.email,
        password: req.body?.password
      });
      return res.status(200).json(session);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.post("/session/refresh", authRateLimiter, async (req, res) => {
    try {
      const session = await staffAuthService.refreshSession({
        refreshToken: req.body?.refresh_token || req.body?.refreshToken
      });
      return res.status(200).json(session);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.use(async (req, res, next) => {
    try {
      const accessToken = readBearerToken(req);
      if (!accessToken) {
        throw new Error("missing_staff_bearer_token");
      }
      req.staffUser = await staffAuthService.getUserForToken(accessToken);
      req.staffAccessToken = accessToken;
      return next();
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.get("/dashboard", async (req, res) => {
    try {
      const payload = await staffOperationsService.getDashboard({
        from: req.query.from,
        to: req.query.to
      });
      return res.status(200).json(payload);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.get("/people", async (_req, res) => {
    try {
      const people = await staffOperationsService.listPeople();
      return res.status(200).json({ people });
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.post("/people", async (req, res) => {
    try {
      assertWriteAccess(req.staffUser);
      const person = await staffOperationsService.createPerson(req.body || {}, buildActor(req.staffUser));
      return res.status(201).json(person);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.post("/tasks", async (req, res) => {
    try {
      assertWriteAccess(req.staffUser);
      const task = await staffOperationsService.createTask(req.body || {}, buildActor(req.staffUser));
      return res.status(201).json(task);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.patch("/tasks/:taskId", async (req, res) => {
    try {
      assertWriteAccess(req.staffUser);
      const task = await staffOperationsService.updateTask(
        req.params.taskId,
        req.body || {},
        buildActor(req.staffUser)
      );
      return res.status(200).json(task);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.post("/agent/threads", async (req, res) => {
    try {
      const thread = await staffOperationsService.createAgentThread(buildActor(req.staffUser), req.body || {});
      return res.status(201).json(thread);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.post("/agent/threads/:threadId/messages", async (req, res) => {
    try {
      const payload = await staffOperationsService.postAgentMessage(
        req.params.threadId,
        req.body || {},
        buildActor(req.staffUser)
      );
      return res.status(200).json(payload);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.post("/agent/actions/:actionId/confirm", async (req, res) => {
    try {
      assertWriteAccess(req.staffUser);
      const payload = await staffOperationsService.confirmAgentAction(
        req.params.actionId,
        buildActor(req.staffUser)
      );
      return res.status(200).json(payload);
    } catch (error) {
      return res.status(errorStatus(error)).json({
        error: error?.message || String(error)
      });
    }
  });

  router.get("/stream", async (req, res) => {
    const pollMs = Math.max(
      500,
      Math.min(Number.parseInt(`${req.query.poll_ms || 1500}`, 10) || 1500, 10000)
    );
    let closed = false;
    let cursor = `${req.query.cursor || "0"}`;

    req.on("close", () => {
      closed = true;
    });
    res.on("close", () => {
      closed = true;
    });
    res.on("error", () => {
      closed = true;
    });

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders?.();
    res.write(": connected\n\n");

    while (!closed) {
      if (res.writableEnded || res.destroyed) {
        break;
      }
      try {
        const events = await staffOperationsService.listChangeEventsAfter(cursor, {
          limit: 25
        });
        if (events.length) {
          for (const event of events) {
            if (closed || res.writableEnded || res.destroyed) {
              break;
            }
            cursor = event.cursor;
            res.write(`event: change\n`);
            res.write(`data: ${JSON.stringify(event)}\n\n`);
          }
          continue;
        }
      } catch (error) {
        res.write("event: error\n");
        res.write(`data: ${JSON.stringify({ error: error?.message || String(error) })}\n\n`);
      }
      await sleep(pollMs);
    }

    if (!res.writableEnded && !res.destroyed) {
      res.end();
    }
  });

  app.use("/v1/staff", router);
}
