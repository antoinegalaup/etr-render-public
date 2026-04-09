function trimText(value, fallback = "") {
  const normalized = `${value || ""}`.trim();
  return normalized || fallback;
}

function joinTextBlocks(content = []) {
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .map((entry) => (entry?.type === "text" ? trimText(entry.text) : ""))
    .filter(Boolean)
    .join("\n\n")
    .trim();
}

async function jsonRequest({ url, headers = {}, body = null, timeoutMs = 30000 }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "POST",
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
        data?.error?.message ||
        data?.error ||
        data?.detail ||
        data?.message ||
        `http_${response.status}`;
      throw new Error(`anthropic_request_failed:${errorText}`);
    }
    return data;
  } finally {
    clearTimeout(timeout);
  }
}

export class AnthropicAgentService {
  constructor(options = {}) {
    this.apiKey = trimText(options.apiKey || process.env.ANTHROPIC_API_KEY || "");
    this.model = trimText(
      options.model || process.env.ANTHROPIC_MODEL || "claude-sonnet-4-20250514"
    );
    this.baseUrl = trimText(
      options.baseUrl || process.env.ANTHROPIC_API_BASE_URL || "https://api.anthropic.com/v1"
    ).replace(/\/$/, "");
    this.timeoutMs =
      Number.parseInt(
        `${options.timeoutMs || process.env.ANTHROPIC_API_TIMEOUT_MS || 30000}`,
        10
      ) || 30000;
    this.version = trimText(
      options.version || process.env.ANTHROPIC_API_VERSION || "2023-06-01"
    );
  }

  isConfigured() {
    return Boolean(this.apiKey);
  }

  async sendConversation({ system, messages, maxOutputTokens = 700, temperature = 0.4 } = {}) {
    if (!this.isConfigured()) {
      throw new Error("anthropic_not_configured");
    }

    const response = await jsonRequest({
      url: `${this.baseUrl}/messages`,
      headers: {
        "content-type": "application/json",
        "x-api-key": this.apiKey,
        "anthropic-version": this.version
      },
      body: {
        model: this.model,
        max_tokens: maxOutputTokens,
        temperature,
        system,
        messages
      },
      timeoutMs: this.timeoutMs
    });

    return {
      reply:
        joinTextBlocks(response.content) ||
        trimText(response.completion) ||
        "Gael did not return a reply.",
      model: trimText(response.model, this.model),
      stop_reason: trimText(response.stop_reason),
      usage: response.usage || {}
    };
  }
}
