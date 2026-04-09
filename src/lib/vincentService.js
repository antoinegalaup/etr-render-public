function trimText(value, fallback = "") {
  const normalized = `${value || ""}`.trim();
  return normalized || fallback;
}

async function jsonRequest({ url, method = "GET", headers = {}, body = null, timeoutMs = 30000 }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
      signal: controller.signal
    });
    const text = await response.text();
    let data = {};
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = { message: text };
      }
    }
    if (!response.ok) {
      const errorText =
        data?.detail ||
        data?.error ||
        data?.message ||
        `http_${response.status}`;
      throw new Error(`vincent_request_failed:${errorText}`);
    }
    return data;
  } finally {
    clearTimeout(timeout);
  }
}

export class VincentService {
  constructor(options = {}) {
    this.baseUrl = trimText(
      options.baseUrl || process.env.VINCENT_API_BASE_URL || ""
    ).replace(/\/$/, "");
    this.internalApiSecret = trimText(
      options.internalApiSecret ||
        process.env.VINCENT_API_SECRET ||
        process.env.INTERNAL_API_SECRET ||
        ""
    );
    this.timeoutMs =
      Number.parseInt(
        `${options.timeoutMs || process.env.VINCENT_API_TIMEOUT_MS || 30000}`,
        10
      ) || 30000;
  }

  isConfigured() {
    return Boolean(this.baseUrl);
  }

  async createSession({ purpose = "" } = {}) {
    if (!this.isConfigured()) {
      throw new Error("vincent_not_configured");
    }
    return jsonRequest({
      url: `${this.baseUrl}/agent/sessions`,
      method: "POST",
      headers: this.buildHeaders(),
      body: { purpose },
      timeoutMs: this.timeoutMs
    });
  }

  async sendMessage(sessionId, { message, maxOutputTokens = 700 } = {}) {
    if (!this.isConfigured()) {
      throw new Error("vincent_not_configured");
    }
    return jsonRequest({
      url: `${this.baseUrl}/agent/sessions/${encodeURIComponent(sessionId)}/responses`,
      method: "POST",
      headers: this.buildHeaders(),
      body: {
        message,
        max_output_tokens: maxOutputTokens
      },
      timeoutMs: this.timeoutMs
    });
  }

  buildHeaders() {
    const headers = {
      "content-type": "application/json"
    };
    if (this.internalApiSecret) {
      headers["x-internal-api-secret"] = this.internalApiSecret;
    }
    return headers;
  }
}
