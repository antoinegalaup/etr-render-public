export function readString(value, fallback = "") {
  const text = `${value ?? ""}`.trim();
  return text.length > 0 ? text : fallback;
}

export function readBoolean(value, fallback = false) {
  if (value === undefined || value === null || `${value}`.trim() === "") {
    return fallback;
  }
  const normalized = `${value}`.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

export function readPositiveInt(value, fallback) {
  const parsed = Number.parseInt(`${value ?? ""}`, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

export function readEnum(value, allowedValues, fallback) {
  const normalized = readString(value, fallback);
  return allowedValues.includes(normalized) ? normalized : fallback;
}

export function redactConnectionString(value) {
  const text = readString(value);
  if (!text) {
    return "";
  }

  try {
    const url = new URL(text);
    if (url.password) {
      url.password = "redacted";
    }
    if (url.username) {
      url.username = "redacted";
    }
    return url.toString();
  } catch {
    return "[redacted-connection-string]";
  }
}
