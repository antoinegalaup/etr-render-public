function parseList(value) {
  return `${value || ""}`
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
}

function normalizeEmail(value) {
  return `${value || ""}`.trim().toLowerCase();
}

function normalizeRoleEntries(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => `${entry || ""}`.trim().toLowerCase()).filter(Boolean);
  }
  const normalized = `${value || ""}`.trim().toLowerCase();
  return normalized ? [normalized] : [];
}

function buildStaffRole(user = {}) {
  const candidates = [
    ...normalizeRoleEntries(user.app_metadata?.role),
    ...normalizeRoleEntries(user.app_metadata?.roles),
    ...normalizeRoleEntries(user.user_metadata?.role),
    ...normalizeRoleEntries(user.user_metadata?.roles)
  ];
  return (
    candidates.find((value) =>
      ["staff", "admin", "operations", "ops", "manager", "employee"].includes(value)
    ) || null
  );
}

async function jsonRequest({ url, method = "GET", headers = {}, body = null }) {
  const response = await fetch(url, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined
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
      data?.msg ||
      data?.error_description ||
      data?.error ||
      data?.message ||
      `http_${response.status}`;
    throw new Error(`auth_request_failed:${errorText}`);
  }
  return data;
}

export class StaffAuthService {
  constructor(options = {}) {
    this.supabaseUrl = `${options.supabaseUrl || process.env.SUPABASE_URL || ""}`.trim();
    this.supabaseAnonKey = `${
      options.supabaseAnonKey || process.env.SUPABASE_ANON_KEY || ""
    }`.trim();
    this.allowedEmails = new Set(
      parseList(options.allowedEmails || process.env.STAFF_ALLOWED_EMAILS)
    );
    this.allowedEmailDomains = new Set(
      parseList(options.allowedEmailDomains || process.env.STAFF_ALLOWED_EMAIL_DOMAINS)
    );
    this.allowAnyAuthenticated =
      options.allowAnyAuthenticated === true ||
      `${process.env.STAFF_AUTH_RELAXED || ""}`.trim().toLowerCase() === "true";
  }

  isConfigured() {
    return Boolean(this.supabaseUrl && this.supabaseAnonKey);
  }

  async signIn({ email, password }) {
    if (!this.isConfigured()) {
      throw new Error("staff_auth_not_configured");
    }
    const payload = await jsonRequest({
      url: `${this.supabaseUrl.replace(/\/$/, "")}/auth/v1/token?grant_type=password`,
      method: "POST",
      headers: {
        apikey: this.supabaseAnonKey,
        "content-type": "application/json"
      },
      body: {
        email: `${email || ""}`.trim(),
        password: `${password || ""}`
      }
    });
    return this.normalizeSession(payload);
  }

  async refreshSession({ refreshToken }) {
    if (!this.isConfigured()) {
      throw new Error("staff_auth_not_configured");
    }
    const payload = await jsonRequest({
      url: `${this.supabaseUrl.replace(/\/$/, "")}/auth/v1/token?grant_type=refresh_token`,
      method: "POST",
      headers: {
        apikey: this.supabaseAnonKey,
        "content-type": "application/json"
      },
      body: {
        refresh_token: `${refreshToken || ""}`.trim()
      }
    });
    return this.normalizeSession(payload);
  }

  async getUserForToken(accessToken) {
    if (!this.isConfigured()) {
      throw new Error("staff_auth_not_configured");
    }
    const payload = await jsonRequest({
      url: `${this.supabaseUrl.replace(/\/$/, "")}/auth/v1/user`,
      headers: {
        apikey: this.supabaseAnonKey,
        authorization: `Bearer ${accessToken}`
      }
    });
    const user = this.normalizeUser(payload);
    if (!user.is_staff) {
      throw new Error("staff_access_denied");
    }
    return user;
  }

  isStaffUser(user = {}) {
    if (this.allowAnyAuthenticated) {
      return true;
    }
    if (user.app_metadata?.staff === true || user.user_metadata?.staff === true) {
      return true;
    }
    const staffRole = buildStaffRole(user);
    if (staffRole) {
      return true;
    }

    const email = normalizeEmail(user.email);
    if (!email) {
      return false;
    }
    if (this.allowedEmails.has(email)) {
      return true;
    }
    const domain = email.split("@")[1] || "";
    return this.allowedEmailDomains.has(domain);
  }

  normalizeUser(user = {}) {
    const email = normalizeEmail(user.email);
    const staffRole = buildStaffRole(user);
    return {
      id: `${user.id || ""}`,
      email,
      is_staff: this.isStaffUser(user),
      staff_role: staffRole,
      app_metadata: user.app_metadata || {},
      user_metadata: user.user_metadata || {}
    };
  }

  normalizeSession(payload = {}) {
    const user = this.normalizeUser(payload.user || {});
    if (!user.is_staff) {
      throw new Error("staff_access_denied");
    }
    const expiresIn = Number.parseInt(`${payload.expires_in || 0}`, 10) || 3600;
    const expiresAt =
      payload.expires_at && Number.isFinite(Number(payload.expires_at))
        ? new Date(Number(payload.expires_at) * 1000).toISOString()
        : new Date(Date.now() + expiresIn * 1000).toISOString();

    return {
      access_token: `${payload.access_token || ""}`,
      refresh_token: `${payload.refresh_token || ""}`,
      token_type: `${payload.token_type || "bearer"}`.toLowerCase(),
      expires_in: expiresIn,
      expires_at: expiresAt,
      user
    };
  }
}
