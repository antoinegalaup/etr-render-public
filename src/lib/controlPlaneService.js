import crypto from "node:crypto";
import { randomUUID } from "node:crypto";
import { SyncIngestService, computeSyncSignature } from "./syncIngest.js";
import { getPublicPropertyCards, getPublicPropertyDetail } from "./publicSiteCatalog.js";
import {
  getLiveVikBookingAvailability,
  getLiveVikBookingProperties
} from "./vikbookingPublicCatalog.js";

function roundMoney(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) {
    return 0;
  }
  return Math.round(amount * 100) / 100;
}

function asArray(value) {
  if (Array.isArray(value)) {
    return value;
  }
  if (value === undefined || value === null) {
    return [];
  }
  return [value];
}

function toIsoOrNull(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function normalizeEmail(value) {
  return `${value || ""}`.trim().toLowerCase();
}

function normalizePhone(value) {
  return `${value || ""}`.replace(/[^0-9]/g, "");
}

function validateIdentifier(value, label = "identifier") {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(`${value || ""}`)) {
    throw new Error(`invalid_${label}:${value}`);
  }
  return `${value}`;
}

function quotePgIdentifier(value) {
  return `"${validateIdentifier(value).replace(/"/g, "\"\"")}"`;
}

function normalizeName(value) {
  return `${value || ""}`.trim().replace(/\s+/g, " ");
}

function countNights(checkinAt, checkoutAt) {
  const start = new Date(checkinAt);
  const end = new Date(checkoutAt);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return 0;
  }
  return Math.max(1, Math.round((end.getTime() - start.getTime()) / 86400000));
}

function clampInt(value, minimum, maximum, fallback = minimum) {
  const parsed = Number.parseInt(`${value ?? ""}`, 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(maximum, Math.max(minimum, parsed));
}

function formatUtcYmd(date) {
  return [
    date.getUTCFullYear(),
    `${date.getUTCMonth() + 1}`.padStart(2, "0"),
    `${date.getUTCDate()}`.padStart(2, "0")
  ].join("-");
}

function normalizeMonthStartYmd(value) {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return formatUtcYmd(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1)));
}

function addMonthsYmd(monthStart, offset) {
  const date = new Date(`${monthStart}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  date.setUTCMonth(date.getUTCMonth() + offset, 1);
  return formatUtcYmd(date);
}

function addDaysYmd(day, offset) {
  const date = new Date(`${day}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  date.setUTCDate(date.getUTCDate() + offset);
  return formatUtcYmd(date);
}

function diffMonthStarts(startMonth, endMonth) {
  const start = new Date(`${startMonth}T00:00:00.000Z`);
  const end = new Date(`${endMonth}T00:00:00.000Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return 0;
  }
  return (end.getUTCFullYear() - start.getUTCFullYear()) * 12 + (end.getUTCMonth() - start.getUTCMonth());
}

function startOfUtcDay(value) {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function buildRecommendedWindowFromPayload(payload = {}, occurredAt = null) {
  const checkinAt = toIsoOrNull(payload?.reservation?.checkin_at);
  const checkoutAt = toIsoOrNull(payload?.reservation?.checkout_at);
  if (checkinAt && checkoutAt) {
    return {
      from: checkinAt,
      to: checkoutAt
    };
  }
  const anchor = startOfUtcDay(occurredAt || new Date());
  if (!anchor) {
    return {};
  }
  const from = anchor.toISOString();
  const to = new Date(anchor.getTime() + 14 * 86400000).toISOString();
  return { from, to };
}

function buildChangedDomainsFromEvent(event = {}) {
  const domains = new Set();
  if (event?.payload?.reservation || event?.source_entity === "reservation") {
    domains.add("reservations");
  }
  if (event?.payload?.customer || event?.source_entity === "customer") {
    domains.add("customers");
  }
  if (Array.isArray(event?.payload?.rooms) && event.payload.rooms.length > 0) {
    domains.add("reservation_rooms");
  }
  if (event?.payload?.payment) {
    domains.add("payments");
  }
  return Array.from(domains);
}

const MONTH_LABEL_FORMATTER = new Intl.DateTimeFormat("en-US", {
  month: "long",
  year: "numeric",
  timeZone: "UTC"
});

const CALENDAR_DAY_STATUSES = new Set(["available", "limited", "blocked", "past"]);

function normalizeCalendarDay(day = {}, priceHint = 0) {
  const normalizedDate = `${day.date || ""}`.slice(0, 10);
  const fallbackDay = Number.parseInt(normalizedDate.slice(-2), 10) || 1;
  const status = CALENDAR_DAY_STATUSES.has(day.status) ? day.status : "blocked";
  const bookable = day.bookable === true || (day.bookable !== false && ["available", "limited"].includes(status));
  return {
    date: normalizedDate,
    day: Number.parseInt(`${day.day ?? fallbackDay}`, 10) || fallbackDay,
    status,
    bookable,
    occupancy: Number.parseInt(`${day.occupancy ?? 0}`, 10) || 0,
    available_units: Number.parseInt(`${day.available_units ?? 0}`, 10) || 0,
    price_hint: bookable && priceHint > 0 ? roundMoney(priceHint) : null
  };
}

function buildFallbackCalendarMonth({ monthStart, blockedDateCounts, priceHint = 0, firstWeekday = 0 }) {
  const monthDate = new Date(`${monthStart}T00:00:00.000Z`);
  const monthLabel = MONTH_LABEL_FORMATTER.format(monthDate);
  const firstDay = monthDate.getUTCDay();
  const weekdayOffset = (7 + firstDay - firstWeekday) % 7;
  const daysInMonth = new Date(Date.UTC(monthDate.getUTCFullYear(), monthDate.getUTCMonth() + 1, 0)).getUTCDate();
  const today = formatUtcYmd(new Date());
  const days = [];

  for (let day = 1; day <= daysInMonth; day += 1) {
    const date = formatUtcYmd(
      new Date(Date.UTC(monthDate.getUTCFullYear(), monthDate.getUTCMonth(), day))
    );
    const occupancy = blockedDateCounts.get(date) || 0;
    const status = date < today ? "past" : occupancy > 0 ? "blocked" : "available";
    const bookable = status === "available";
    days.push({
      date,
      day,
      status,
      bookable,
      occupancy,
      available_units: bookable ? 1 : 0,
      price_hint: bookable && priceHint > 0 ? roundMoney(priceHint) : null
    });
  }

  return {
    month_start: monthStart,
    month_label: monthLabel,
    weekday_offset: weekdayOffset,
    days
  };
}

function isoToUnixSeconds(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return Math.floor(date.getTime() / 1000);
}

function unixSecondsToIso(value) {
  const parsed = Number.parseInt(`${value ?? ""}`, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  return new Date(parsed * 1000).toISOString();
}

function toNumberOrNull(value) {
  if (value === undefined || value === null || `${value}`.trim() === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function splitFullName(value) {
  const normalized = normalizeName(value);
  if (!normalized) {
    return { firstName: null, lastName: null };
  }
  const parts = normalized.split(" ");
  if (parts.length === 1) {
    return { firstName: parts[0], lastName: null };
  }
  return {
    firstName: parts.slice(0, -1).join(" "),
    lastName: parts.slice(-1)[0] || null
  };
}

function parseCustomerDataBlob(value) {
  const lines = `${value || ""}`
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const parsed = {};
  for (const line of lines) {
    const separatorIndex = line.indexOf(":");
    if (separatorIndex <= 0) {
      if (!parsed.name) {
        parsed.name = line;
      }
      continue;
    }
    const key = line.slice(0, separatorIndex).trim().toLowerCase();
    const fieldValue = line.slice(separatorIndex + 1).trim();
    if (!fieldValue) {
      continue;
    }
    parsed[key] = fieldValue;
    if (key === "name" && !parsed.name) {
      parsed.name = fieldValue;
    }
    if ((key === "telephone" || key === "phone") && !parsed.phone) {
      parsed.phone = fieldValue;
    }
    if (key === "email" && !parsed.email) {
      parsed.email = fieldValue;
    }
  }
  return parsed;
}

function derivePaymentRail(paymentMethod = null) {
  const file = `${paymentMethod?.file || ""}`.trim().toLowerCase();
  if (!file) {
    return null;
  }
  if (file.includes("stripe")) {
    return "stripe";
  }
  if (file.includes("crypto") || file.includes("coin") || file.includes("bitcoin")) {
    return "crypto";
  }
  return null;
}

function pickCustomerFromOrder(order = {}, rooms = []) {
  const parsedBlob = parseCustomerDataBlob(order.custdata);
  const roomGuestName = normalizeName(
    [rooms[0]?.t_first_name, rooms[0]?.t_last_name].filter(Boolean).join(" ")
  );
  const fullName = parsedBlob.name || roomGuestName || normalizeName(order.custdata);
  const { firstName, lastName } = splitFullName(fullName);
  return {
    email: normalizeEmail(parsedBlob.email || order.custmail),
    phone: parsedBlob.phone || order.phone || null,
    first_name: firstName,
    last_name: lastName
  };
}

export function buildReservationBackfillEvent({
  order,
  linkedCustomerId = null,
  customer = null,
  rooms = [],
  paymentMethod = null
}) {
  const orderId = `${order?.id ?? ""}`.trim();
  if (!orderId) {
    throw new Error("missing_order_id");
  }

  const occurredAt =
    unixSecondsToIso(order?.ts) ||
    unixSecondsToIso(order?.checkin) ||
    new Date().toISOString();
  const fallbackCustomer = pickCustomerFromOrder(order, rooms);
  const customerId = linkedCustomerId ? `${linkedCustomerId}` : null;
  const normalizedCustomer =
    customerId || fallbackCustomer.email || fallbackCustomer.phone || fallbackCustomer.first_name
      ? {
          vikbooking_id: customerId,
          email: normalizeEmail(customer?.email || fallbackCustomer.email) || null,
          first_name: normalizeName(customer?.first_name || fallbackCustomer.first_name) || null,
          last_name: normalizeName(customer?.last_name || fallbackCustomer.last_name) || null,
          phone: customer?.phone || fallbackCustomer.phone || null,
          updated_at: occurredAt
        }
      : null;

  const reservation = {
    vikbooking_id: orderId,
    status: `${order?.status || ""}`.trim() || null,
    total: toNumberOrNull(order?.total ?? order?.payable),
    total_paid: toNumberOrNull(order?.totpaid),
    customer_vikbooking_id: customerId,
    checkin_at: unixSecondsToIso(order?.checkin),
    checkout_at: unixSecondsToIso(order?.checkout),
    updated_at: occurredAt,
    confirmnumber: `${order?.confirmnumber || ""}`.trim() || null,
    channel: `${order?.channel || ""}`.trim() || null,
    ota_id: `${order?.idorderota || ""}`.trim() || null,
    payment_rail: derivePaymentRail(paymentMethod),
    external_booking_id: `${order?.confirmnumber || order?.idorderota || ""}`.trim() || null
  };

  const normalizedRooms = rooms.map((room) => ({
    vikbooking_room_link_id: `${room?.id ?? room?.roomindex ?? room?.idroom ?? ""}`.trim(),
    vikbooking_room_id:
      room?.idroom === undefined || room?.idroom === null ? null : `${room.idroom}`,
    adults: room?.adults ?? null,
    children: room?.children ?? null,
    room_cost: toNumberOrNull(room?.room_cost ?? room?.cust_cost),
    updated_at: occurredAt
  })).filter((room) => room.vikbooking_room_link_id);

  const payload = {
    reservation,
    rooms: normalizedRooms,
    attribution: {
      source: "wp_raw_rows_backfill",
      channel: reservation.channel || null
    }
  };

  if (normalizedCustomer?.vikbooking_id) {
    payload.customer = normalizedCustomer;
  }

  const fingerprint = crypto
    .createHash("sha1")
    .update(JSON.stringify(payload))
    .digest("hex")
    .slice(0, 12);

  return {
    event_id: `backfill_reservation_${orderId}_${fingerprint}`,
    event_type: "reservation.backfilled",
    occurred_at: occurredAt,
    source_entity: "reservation",
    source_id: orderId,
    idempotency_key: `backfill:reservation:${orderId}:${fingerprint}`,
    payload
  };
}

export function hashAudienceField(value, normalizer = (input) => `${input || ""}`.trim()) {
  const normalized = normalizer(value);
  if (!normalized) {
    return "";
  }
  return crypto.createHash("sha256").update(normalized).digest("hex");
}

export function buildBookingQuote(input = {}) {
  const paymentRail = `${input.payment_rail || "stripe"}`.trim().toLowerCase();
  if (!["stripe", "crypto"].includes(paymentRail)) {
    throw new Error(`unsupported_payment_rail:${paymentRail}`);
  }

  const baseQuoteUsd = roundMoney(
    input.base_quote_usd ??
      input.website_base_quote_usd ??
      input.website_booking_total ??
      input.total
  );
  if (baseQuoteUsd <= 0) {
    throw new Error("invalid_base_quote_usd");
  }

  const currency = `${input.currency || "USD"}`.trim().toUpperCase();
  const externalCollectedTotal =
    paymentRail === "crypto" ? roundMoney(baseQuoteUsd * 1.1) : baseQuoteUsd;
  const websiteBookingTotal =
    paymentRail === "crypto" ? roundMoney(baseQuoteUsd * 0.9) : baseQuoteUsd;
  const etrTransferAmount =
    paymentRail === "stripe" ? roundMoney(baseQuoteUsd * 0.9) : websiteBookingTotal;
  const platformRetainedAmount = roundMoney(externalCollectedTotal - etrTransferAmount);

  return {
    payment_rail: paymentRail,
    currency,
    base_quote_usd: baseQuoteUsd,
    external_collected_total: externalCollectedTotal,
    website_booking_total: websiteBookingTotal,
    etr_transfer_amount: etrTransferAmount,
    platform_retained_amount: platformRetainedAmount
  };
}

export function verifyStripeSignature({
  rawBody,
  signatureHeader,
  secret,
  toleranceSeconds = 300
}) {
  if (!secret) {
    return { ok: true, skipped: true };
  }
  if (!signatureHeader) {
    return { ok: false, reason: "missing_signature" };
  }

  const pairs = Object.fromEntries(
    `${signatureHeader}`
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => {
        const [key, value] = part.split("=", 2);
        return [key, value];
      })
  );
  const timestamp = Number.parseInt(pairs.t || "", 10);
  if (!Number.isFinite(timestamp)) {
    return { ok: false, reason: "invalid_timestamp" };
  }
  const skew = Math.abs(Math.floor(Date.now() / 1000) - timestamp);
  if (skew > toleranceSeconds) {
    return { ok: false, reason: "timestamp_skew" };
  }

  const provided = `${signatureHeader}`
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.startsWith("v1="))
    .map((part) => part.slice(3));
  if (!provided.length) {
    return { ok: false, reason: "missing_v1_signature" };
  }

  const expected = crypto
    .createHmac("sha256", secret)
    .update(`${timestamp}.${rawBody}`)
    .digest("hex");
  const ok = provided.some((candidate) => safeHexEqual(candidate, expected));
  return ok ? { ok: true } : { ok: false, reason: "signature_mismatch" };
}

export function verifyCryptoSignature({ rawBody, signatureHeader, secret }) {
  if (!secret) {
    return { ok: true, skipped: true };
  }
  if (!signatureHeader) {
    return { ok: false, reason: "missing_signature" };
  }

  const expectedSha512 = crypto.createHmac("sha512", secret).update(rawBody).digest("hex");
  const expectedSha256 = crypto.createHmac("sha256", secret).update(rawBody).digest("hex");
  if (
    safeHexEqual(signatureHeader, expectedSha512) ||
    safeHexEqual(signatureHeader, expectedSha256)
  ) {
    return { ok: true };
  }
  return { ok: false, reason: "signature_mismatch" };
}

function safeHexEqual(a, b) {
  try {
    const left = Buffer.from(`${a}`.toLowerCase(), "hex");
    const right = Buffer.from(`${b}`.toLowerCase(), "hex");
    if (!left.length || left.length !== right.length) {
      return false;
    }
    return crypto.timingSafeEqual(left, right);
  } catch {
    return false;
  }
}

function roomIdsFromInput(input = {}) {
  if (Array.isArray(input.room_ids) && input.room_ids.length) {
    return input.room_ids.map((value) => `${value}`);
  }
  if (Array.isArray(input.rooms) && input.rooms.length) {
    return input.rooms.map((room) => `${room.id || room.room_id || room.vikbooking_room_id || ""}`).filter(Boolean);
  }
  if (input.room_id) {
    return [`${input.room_id}`];
  }
  return [];
}

function customerPayloadFromInput(input = {}) {
  const customer = input.customer || {};
  return {
    first_name: customer.first_name || input.first_name || "",
    last_name: customer.last_name || input.last_name || "",
    email: customer.email || input.email || "",
    phone: customer.phone || input.phone || ""
  };
}

function attributionPayloadFromInput(input = {}) {
  const attribution = input.attribution || {};
  const result = {
    utm_source: attribution.utm_source || input.utm_source || null,
    utm_medium: attribution.utm_medium || input.utm_medium || null,
    utm_campaign: attribution.utm_campaign || input.utm_campaign || null,
    utm_content: attribution.utm_content || input.utm_content || null,
    utm_term: attribution.utm_term || input.utm_term || null,
    fbclid: attribution.fbclid || input.fbclid || null,
    session_id: attribution.session_id || input.session_id || null,
    landing_path: attribution.landing_path || input.landing_path || null
  };
  return Object.fromEntries(Object.entries(result).filter(([, value]) => value !== null && value !== ""));
}

function segmentFromProfile(profile) {
  const segments = new Set();
  if (!profile) {
    return [];
  }
  if ((profile.total_revenue || 0) >= 5000 && /villa esencia/i.test(profile.primary_room_name || "")) {
    segments.add("high_value_villa_esencia");
  }
  if ((profile.confirmed_stays || 0) >= 2 && /keylime/i.test(profile.primary_room_name || "")) {
    segments.add("repeat_keylime");
  }
  if ((profile.confirmed_stays || 0) >= 2 && /lake/i.test(profile.primary_room_name || "") && (profile.direct_stays || 0) >= 1) {
    segments.add("repeat_direct_lake");
  }
  if ((profile.vrbo_stays || 0) >= 1 && (profile.contact_completeness || 0) >= 1) {
    segments.add("qualified_vrbo_guests");
  }
  if (profile.next_stay_at || (profile.last_stay_at && new Date(profile.last_stay_at).getTime() > Date.now() - 30 * 86400 * 1000)) {
    segments.add("direct_booking_exclusion");
  }
  return Array.from(segments);
}

function choosePrimaryRoom(rows) {
  const counts = new Map();
  for (const row of rows) {
    const key = `${row.room_name || row.room_id || ""}`.trim();
    if (!key) continue;
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  const sorted = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  return sorted[0]?.[0] || null;
}

function computeSeedQuality(profile) {
  let score = 0;
  if ((profile.confirmed_stays || 0) >= 1) score += 25;
  if ((profile.confirmed_stays || 0) >= 2) score += 20;
  if ((profile.total_revenue || 0) >= 2500) score += 20;
  if ((profile.direct_stays || 0) >= 1) score += 10;
  score += Math.min((profile.contact_completeness || 0) * 10, 20);
  return Math.min(score, 100);
}

function stripeEventToCharge(event) {
  const object = event?.data?.object || {};
  const metadata = object.metadata || object.payment_intent_data?.metadata || {};
  const amountMinor =
    object.amount_total ?? object.amount_received ?? object.amount ?? object.total_details?.amount_tip ?? 0;
  const grossAmount = roundMoney((Number(amountMinor || 0) || 0) / 100);
  return {
    externalBookingId: metadata.external_booking_id || metadata.externalBookingId || null,
    quoteId: metadata.quote_id || metadata.quoteId || null,
    providerChargeId: object.payment_intent || object.charge || object.id || null,
    currency: `${object.currency || "usd"}`.toUpperCase(),
    grossAmount,
    providerStatus:
      object.payment_status ||
      object.status ||
      (event.type.endsWith(".succeeded") || event.type === "checkout.session.completed" ? "succeeded" : "unknown"),
    rawPayload: event
  };
}

function cryptoPayloadToInvoice(payload = {}) {
  return {
    externalBookingId: payload.order_id || payload.external_booking_id || null,
    providerInvoiceId: payload.payment_id || payload.id || null,
    providerStatus: payload.payment_status || payload.status || null,
    asset: `${payload.pay_currency || payload.payCurrency || payload.asset || ""}`.toUpperCase() || null,
    network: payload.network || payload.network_name || null,
    txReference: payload.outcome_txid || payload.tx_hash || payload.txid || null,
    rawPayload: payload
  };
}

async function stripeApiRequest({ secretKey, path, method = "POST", params = {} }) {
  if (!secretKey) {
    throw new Error("missing_stripe_secret_key");
  }
  const body = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    if (Array.isArray(value)) {
      value.forEach((entry, index) => {
        body.append(`${key}[${index}]`, `${entry}`);
      });
      continue;
    }
    body.append(key, `${value}`);
  }

  const response = await fetch(`https://api.stripe.com${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${secretKey}`,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: body.toString()
  });
  const json = await response.json();
  if (!response.ok) {
    throw new Error(`stripe_api_failed:${json?.error?.message || response.status}`);
  }
  return json;
}

async function jsonApiRequest({
  url,
  method = "POST",
  headers = {},
  body = null,
  allowedStatusCodes = []
}) {
  const response = await fetch(url, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined
  });
  const text = await response.text();
  let data = text;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    // keep raw text
  }
  const allowNonOk = allowedStatusCodes.includes(response.status);
  if (!response.ok && !allowNonOk) {
    throw new Error(`http_${response.status}:${typeof data === "string" ? data : JSON.stringify(data)}`);
  }
  return data;
}

export class ControlPlaneService extends SyncIngestService {
  constructor(options = {}) {
    super(options);
    this.wordpressBaseUrl = `${options.wordpressBaseUrl || process.env.WORDPRESS_BASE_URL || ""}`.trim();
    this.wordpressPublicBaseUrl = `${
      options.wordpressPublicBaseUrl ||
      process.env.WORDPRESS_PUBLIC_BASE_URL ||
      process.env.NEXT_PUBLIC_WORDPRESS_ASSET_BASE_URL ||
      this.wordpressBaseUrl
    }`.trim();
    this.wordpressCommandEndpoint = `${
      options.wordpressCommandEndpoint ||
      process.env.WORDPRESS_COMMAND_ENDPOINT ||
      "/wp-json/wpshell-sync/v1/commands"
    }`.trim();
    this.wordpressCommandSecret = `${
      options.wordpressCommandSecret || process.env.WORDPRESS_COMMAND_SECRET || process.env.SYNC_HMAC_SECRET || ""
    }`.trim();
    this.stripeSecretKey = `${options.stripeSecretKey || process.env.STRIPE_SECRET_KEY || ""}`.trim();
    this.stripeWebhookSecret = `${
      options.stripeWebhookSecret || process.env.STRIPE_WEBHOOK_SECRET || ""
    }`.trim();
    this.stripeConnectedAccount = `${
      options.stripeConnectedAccount || process.env.STRIPE_ETR_CONNECTED_ACCOUNT || ""
    }`.trim();
    this.defaultSuccessUrl = `${
      options.defaultSuccessUrl || process.env.CHANNEL_SUCCESS_URL || ""
    }`.trim();
    this.defaultCancelUrl = `${
      options.defaultCancelUrl || process.env.CHANNEL_CANCEL_URL || ""
    }`.trim();
    this.cryptoApiBaseUrl = `${
      options.cryptoApiBaseUrl || process.env.CRYPTO_API_BASE_URL || "https://api.nowpayments.io/v1"
    }`.trim();
    this.cryptoApiKey = `${options.cryptoApiKey || process.env.CRYPTO_API_KEY || ""}`.trim();
    this.cryptoWebhookSecret = `${
      options.cryptoWebhookSecret || process.env.CRYPTO_WEBHOOK_SECRET || ""
    }`.trim();
  }

  async _ensureSchema() {
    await super._ensureSchema();
    const s = `"${this.schema}"`;
    await this._client.query(`
      ALTER TABLE ${s}.reservations
      ADD COLUMN IF NOT EXISTS confirmnumber TEXT,
      ADD COLUMN IF NOT EXISTS channel TEXT,
      ADD COLUMN IF NOT EXISTS ota_id TEXT,
      ADD COLUMN IF NOT EXISTS payment_rail TEXT,
      ADD COLUMN IF NOT EXISTS attribution JSONB,
      ADD COLUMN IF NOT EXISTS external_booking_id TEXT,
      ADD COLUMN IF NOT EXISTS website_booking_total NUMERIC,
      ADD COLUMN IF NOT EXISTS external_collected_total NUMERIC,
      ADD COLUMN IF NOT EXISTS platform_retained_amount NUMERIC,
      ADD COLUMN IF NOT EXISTS source_payload JSONB
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.guest_profiles (
        guest_key TEXT PRIMARY KEY,
        customer_supabase_uuid UUID REFERENCES ${s}.customers(supabase_uuid) ON DELETE SET NULL,
        primary_email TEXT,
        primary_phone TEXT,
        first_name TEXT,
        last_name TEXT,
        total_stays INT NOT NULL DEFAULT 0,
        confirmed_stays INT NOT NULL DEFAULT 0,
        total_revenue NUMERIC NOT NULL DEFAULT 0,
        direct_stays INT NOT NULL DEFAULT 0,
        vrbo_stays INT NOT NULL DEFAULT 0,
        last_stay_at TIMESTAMPTZ,
        next_stay_at TIMESTAMPTZ,
        primary_room_name TEXT,
        contact_completeness INT NOT NULL DEFAULT 0,
        seed_quality_score INT NOT NULL DEFAULT 0,
        profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.audience_segments (
        segment_key TEXT NOT NULL,
        guest_key TEXT NOT NULL REFERENCES ${s}.guest_profiles(guest_key) ON DELETE CASCADE,
        qualification_reason TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (segment_key, guest_key)
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.channel_quotes (
        quote_id TEXT PRIMARY KEY,
        external_booking_id TEXT UNIQUE NOT NULL,
        status TEXT NOT NULL,
        payment_rail TEXT NOT NULL,
        currency TEXT NOT NULL,
        room_ids JSONB NOT NULL,
        checkin_at TIMESTAMPTZ NOT NULL,
        checkout_at TIMESTAMPTZ NOT NULL,
        adults INT NOT NULL DEFAULT 1,
        children INT NOT NULL DEFAULT 0,
        customer JSONB NOT NULL DEFAULT '{}'::jsonb,
        attribution JSONB NOT NULL DEFAULT '{}'::jsonb,
        base_quote_usd NUMERIC NOT NULL,
        website_booking_total NUMERIC NOT NULL,
        external_collected_total NUMERIC NOT NULL,
        etr_transfer_amount NUMERIC NOT NULL,
        platform_retained_amount NUMERIC NOT NULL,
        availability_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        provider_session JSONB,
        website_reservation_id TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.platform_charges (
        charge_id TEXT PRIMARY KEY,
        external_booking_id TEXT NOT NULL,
        quote_id TEXT REFERENCES ${s}.channel_quotes(quote_id) ON DELETE SET NULL,
        provider TEXT NOT NULL,
        payment_rail TEXT NOT NULL,
        provider_charge_id TEXT UNIQUE,
        status TEXT NOT NULL,
        currency TEXT,
        gross_amount NUMERIC,
        processor_fee NUMERIC,
        website_booking_total NUMERIC,
        platform_retained_amount NUMERIC,
        website_reservation_id TEXT,
        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.connected_transfers (
        transfer_id TEXT PRIMARY KEY,
        charge_id TEXT REFERENCES ${s}.platform_charges(charge_id) ON DELETE SET NULL,
        external_booking_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        provider_transfer_id TEXT UNIQUE,
        destination_account_id TEXT,
        amount NUMERIC,
        currency TEXT,
        status TEXT NOT NULL,
        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.crypto_invoices (
        invoice_id TEXT PRIMARY KEY,
        external_booking_id TEXT NOT NULL,
        quote_id TEXT REFERENCES ${s}.channel_quotes(quote_id) ON DELETE SET NULL,
        provider TEXT NOT NULL,
        provider_invoice_id TEXT UNIQUE,
        status TEXT NOT NULL,
        asset TEXT,
        network TEXT,
        quote_amount NUMERIC,
        website_booking_total NUMERIC,
        retained_spread NUMERIC,
        tx_reference TEXT,
        website_reservation_id TEXT,
        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.manual_exceptions (
        id BIGSERIAL PRIMARY KEY,
        external_booking_id TEXT,
        quote_id TEXT,
        reason TEXT NOT NULL,
        details JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL DEFAULT 'open',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.meta_campaign_spend (
        campaign_key TEXT NOT NULL,
        spend_date DATE NOT NULL,
        spend NUMERIC NOT NULL DEFAULT 0,
        currency TEXT NOT NULL DEFAULT 'USD',
        impressions BIGINT,
        clicks BIGINT,
        raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (campaign_key, spend_date)
      )
    `);
  }

  async _upsertProjection(event) {
    await super._upsertProjection(event);
    const s = `"${this.schema}"`;
    const reservationId = `${event?.payload?.reservation?.vikbooking_id || event?.source_id || ""}`.trim();
    if (reservationId) {
      const reservation = event.payload?.reservation || {};
      const payment = event.payload?.payment || {};
      const attribution = event.payload?.attribution || {};
      await this._client.query(
        `
          UPDATE ${s}.reservations
          SET
            confirmnumber = COALESCE($2, confirmnumber),
            channel = COALESCE($3, channel),
            ota_id = COALESCE($4, ota_id),
            payment_rail = COALESCE($5, payment_rail),
            attribution = CASE
              WHEN $6::jsonb = '{}'::jsonb THEN attribution
              ELSE $6::jsonb
            END,
            external_booking_id = COALESCE($7, external_booking_id),
            website_booking_total = COALESCE($8, website_booking_total),
            external_collected_total = COALESCE($9, external_collected_total),
            platform_retained_amount = COALESCE($10, platform_retained_amount),
            source_payload = $11::jsonb,
            updated_at = NOW()
          WHERE vikbooking_id = $1
        `,
        [
          reservationId,
          reservation.confirmnumber || null,
          reservation.channel || null,
          reservation.ota_id || null,
          payment.payment_rail || reservation.payment_rail || null,
          JSON.stringify(attribution && Object.keys(attribution).length ? attribution : {}),
          reservation.external_booking_id || null,
          reservation.website_booking_total ?? null,
          reservation.external_collected_total ?? null,
          reservation.platform_retained_amount ?? null,
          JSON.stringify(event.payload || {})
        ]
      );
    }

    const customerVikbookingId =
      event?.payload?.customer?.vikbooking_id ||
      event?.payload?.reservation?.customer_vikbooking_id ||
      null;
    if (customerVikbookingId) {
      await this.refreshGuestProfileByCustomer(customerVikbookingId);
    }

    await this.emitOpsChangeEvent({
      type: event?.event_type || "sync.updated",
      changedDomains: buildChangedDomainsFromEvent(event),
      recommendedWindow: buildRecommendedWindowFromPayload(event?.payload, event?.occurred_at),
      payload: {
        source_entity: event?.source_entity || null,
        source_id: event?.source_id || null,
        event_id: event?.event_id || null
      }
    });
  }

  async createBookingQuote(input = {}) {
    if (!this._ready) await this.connect();
    const roomIds = roomIdsFromInput(input);
    if (!roomIds.length) {
      throw new Error("missing_room_ids");
    }
    const distinctRoomIds = [...new Set(roomIds)];
    if (distinctRoomIds.length > 1) {
      throw new Error("multi_room_not_supported_yet");
    }

    const externalBookingId =
      `${input.external_booking_id || input.externalBookingId || input.quote_id || ""}`.trim() ||
      `offsite_${randomUUID()}`;
    const checkinAt = toIsoOrNull(input.checkin_at || input.checkin || input.start_date);
    const checkoutAt = toIsoOrNull(input.checkout_at || input.checkout || input.end_date);
    if (!checkinAt || !checkoutAt || checkinAt >= checkoutAt) {
      throw new Error("invalid_stay_dates");
    }

    const pricing = buildBookingQuote(input);
    const customer = customerPayloadFromInput(input);
    const attribution = attributionPayloadFromInput(input);
    const availability = await this.checkWebsiteAvailability({
      room_ids: distinctRoomIds,
      checkin_at: checkinAt,
      checkout_at: checkoutAt
    });
    if (!availability.available) {
      throw new Error("room_not_available");
    }

    const quote = {
      quote_id: `quote_${randomUUID()}`,
      external_booking_id: externalBookingId,
      status: "quoted",
      room_ids: distinctRoomIds,
      checkin_at: checkinAt,
      checkout_at: checkoutAt,
      adults: Number.parseInt(input.adults || "1", 10) || 1,
      children: Number.parseInt(input.children || "0", 10) || 0,
      customer,
      attribution,
      availability_snapshot: availability,
      ...pricing
    };

    let providerSession = null;
    if (input.create_provider_session === true || input.create_provider_session === "true") {
      if (quote.payment_rail === "stripe") {
        providerSession = await this.createStripeCheckoutSession(quote, input);
      } else if (quote.payment_rail === "crypto") {
        providerSession = await this.createCryptoInvoiceIntent(quote, input);
      }
    }

    const s = `"${this.schema}"`;
    await this._client.query(
      `
        INSERT INTO ${s}.channel_quotes
          (
            quote_id,
            external_booking_id,
            status,
            payment_rail,
            currency,
            room_ids,
            checkin_at,
            checkout_at,
            adults,
            children,
            customer,
            attribution,
            base_quote_usd,
            website_booking_total,
            external_collected_total,
            etr_transfer_amount,
            platform_retained_amount,
            availability_snapshot,
            provider_session,
            updated_at
          )
        VALUES
          ($1, $2, $3, $4, $5, $6::jsonb, $7::timestamptz, $8::timestamptz, $9, $10, $11::jsonb, $12::jsonb, $13, $14, $15, $16, $17, $18::jsonb, $19::jsonb, NOW())
        ON CONFLICT (external_booking_id)
        DO UPDATE SET
          status = EXCLUDED.status,
          payment_rail = EXCLUDED.payment_rail,
          currency = EXCLUDED.currency,
          room_ids = EXCLUDED.room_ids,
          checkin_at = EXCLUDED.checkin_at,
          checkout_at = EXCLUDED.checkout_at,
          adults = EXCLUDED.adults,
          children = EXCLUDED.children,
          customer = EXCLUDED.customer,
          attribution = EXCLUDED.attribution,
          base_quote_usd = EXCLUDED.base_quote_usd,
          website_booking_total = EXCLUDED.website_booking_total,
          external_collected_total = EXCLUDED.external_collected_total,
          etr_transfer_amount = EXCLUDED.etr_transfer_amount,
          platform_retained_amount = EXCLUDED.platform_retained_amount,
          availability_snapshot = EXCLUDED.availability_snapshot,
          provider_session = EXCLUDED.provider_session,
          updated_at = NOW()
      `,
      [
        quote.quote_id,
        quote.external_booking_id,
        quote.status,
        quote.payment_rail,
        quote.currency,
        JSON.stringify(quote.room_ids),
        quote.checkin_at,
        quote.checkout_at,
        quote.adults,
        quote.children,
        JSON.stringify(quote.customer),
        JSON.stringify(quote.attribution),
        quote.base_quote_usd,
        quote.website_booking_total,
        quote.external_collected_total,
        quote.etr_transfer_amount,
        quote.platform_retained_amount,
        JSON.stringify(quote.availability_snapshot),
        JSON.stringify(providerSession || {})
      ]
    );

    return {
      ...quote,
      provider_session: providerSession
    };
  }

  async listPublicProperties() {
    const liveProperties = await getLiveVikBookingProperties({
      wordpressBaseUrl: this.wordpressPublicBaseUrl
    }).catch(() => null);
    if (liveProperties?.length) {
      return liveProperties.map((property) => ({
        room_id: property.room_id,
        slug: property.slug,
        name: property.name,
        tagline: property.tagline,
        location_label: property.location_label,
        short_description: property.short_description,
        capacity: property.capacity,
        hero_image: property.hero_image,
        base_price_hint: property.base_price_hint,
        badges: property.badges
      }));
    }
    return getPublicPropertyCards({
      wordpressBaseUrl: this.wordpressPublicBaseUrl
    });
  }

  async getPublicProperty(identifier) {
    const liveProperties = await getLiveVikBookingProperties({
      wordpressBaseUrl: this.wordpressPublicBaseUrl
    }).catch(() => null);
    if (liveProperties?.length) {
      const normalized = `${identifier || ""}`.trim().toLowerCase();
      return (
        liveProperties.find(
          (property) =>
            `${property.room_id}` === normalized ||
            property.slug.toLowerCase() === normalized ||
            property.name.toLowerCase() === normalized
        ) || null
      );
    }
    return getPublicPropertyDetail(identifier, {
      wordpressBaseUrl: this.wordpressPublicBaseUrl
    });
  }

  async getAvailabilityPreview(input = {}) {
    const roomIds = roomIdsFromInput(input);
    if (!roomIds.length) {
      throw new Error("missing_room_ids");
    }
    const distinctRoomIds = [...new Set(roomIds)];
    const checkinAt = toIsoOrNull(input.checkin_at || input.checkin || input.start_date);
    const checkoutAt = toIsoOrNull(input.checkout_at || input.checkout || input.end_date);
    if (!checkinAt || !checkoutAt || checkinAt >= checkoutAt) {
      throw new Error("invalid_stay_dates");
    }

    const availability = await this.checkWebsiteAvailability({
      room_ids: distinctRoomIds,
      checkin_at: checkinAt,
      checkout_at: checkoutAt
    });
    const property = await this.getPublicProperty(distinctRoomIds[0]);
    const nights = countNights(checkinAt, checkoutAt);
    const baseQuoteUsd = roundMoney(
      input.base_quote_usd ??
        (property?.price_summary?.starting_from ? property.price_summary.starting_from * nights : 0)
    );
    const pricing =
      baseQuoteUsd > 0 ? buildBookingQuote({ ...input, base_quote_usd: baseQuoteUsd }) : null;

    return {
      available: availability.available,
      source: availability.source,
      details: availability.details,
      room_ids: distinctRoomIds,
      checkin_at: checkinAt,
      checkout_at: checkoutAt,
      nights,
      adults: Number.parseInt(input.adults || "1", 10) || 1,
      children: Number.parseInt(input.children || "0", 10) || 0,
      property: property
        ? {
            room_id: property.room_id,
            slug: property.slug,
            name: property.name,
            hero_image: property.hero_image,
            base_price_hint: property.base_price_hint
          }
        : null,
      pricing
    };
  }

  async getAvailabilityCalendar(input = {}) {
    const roomIds = roomIdsFromInput(input);
    const roomId = roomIds[0] || input.room_id || input.roomId;
    if (!roomId) {
      throw new Error("missing_room_ids");
    }

    const months = clampInt(input.months, 1, 6, 2);
    const monthStart = normalizeMonthStartYmd(input.month_start || input.monthStart);
    if (!monthStart) {
      throw new Error("invalid_calendar_month");
    }

    const property = await this.getPublicProperty(roomId);
    const priceHint = roundMoney(
      property?.base_price_hint || property?.price_summary?.starting_from || 0
    );

    if (this.wordpressBaseUrl && this.wordpressCommandSecret) {
      try {
        const response = await this.sendWordPressCommand({
          command_id: `calendar_${randomUUID()}`,
          command_type: "availability.calendar",
          room_id: `${roomId}`,
          month_start: monthStart,
          months
        });
        return {
          room_id: Number.parseInt(`${roomId}`, 10) || roomId,
          room_name: response.room_name || property?.name || null,
          source: "website",
          first_weekday: Number.parseInt(`${response.first_weekday ?? 0}`, 10) || 0,
          months: Array.isArray(response.months)
            ? response.months
                .map((month, index) => {
                  const fallbackMonthStart = addMonthsYmd(monthStart, index);
                  const normalizedMonthStart = normalizeMonthStartYmd(
                    month?.month_start || fallbackMonthStart
                  );
                  if (!normalizedMonthStart) {
                    return null;
                  }
                  return {
                    month_start: normalizedMonthStart,
                    month_label:
                      month?.month_label ||
                      MONTH_LABEL_FORMATTER.format(
                        new Date(`${normalizedMonthStart}T00:00:00.000Z`)
                      ),
                    weekday_offset: Number.parseInt(`${month?.weekday_offset ?? 0}`, 10) || 0,
                    days: Array.isArray(month?.days)
                      ? month.days.map((day) => normalizeCalendarDay(day, priceHint))
                      : []
                  };
                })
                .filter(Boolean)
            : []
        };
      } catch (error) {
        return this.getShadowAvailabilityCalendar(
          {
            room_id: roomId,
            month_start: monthStart,
            months
          },
          priceHint,
          `website_calendar_failed:${error.message}`
        );
      }
    }

    return this.getShadowAvailabilityCalendar(
      {
        room_id: roomId,
        month_start: monthStart,
        months
      },
      priceHint
    );
  }

  async checkWebsiteAvailability(input = {}) {
    if (this.wordpressBaseUrl && this.wordpressCommandSecret) {
      try {
        const response = await this.sendWordPressCommand({
          command_id: `availability_${randomUUID()}`,
          command_type: "availability.check",
          room_ids: roomIdsFromInput(input),
          checkin_at: input.checkin_at,
          checkout_at: input.checkout_at
        });
        return {
          available: !!response.available,
          source: "website",
          details: response.details || {}
        };
      } catch (error) {
        return this.checkVikBookingAvailability(
          input,
          `website_preflight_failed:${error.message}`
        );
      }
    }
    return this.checkVikBookingAvailability(input);
  }

  async checkVikBookingAvailability(input = {}, fallbackReason = null) {
    const liveDetails = await getLiveVikBookingAvailability(roomIdsFromInput(input), {
      checkinUnix: isoToUnixSeconds(input.checkin_at),
      checkoutUnix: isoToUnixSeconds(input.checkout_at)
    }).catch(() => null);
    if (liveDetails?.length) {
      return {
        available: liveDetails.every((detail) => detail.available),
        source: "vikbooking_mysql",
        details: {
          rooms: liveDetails,
          fallback_reason: fallbackReason
        }
      };
    }
    return this.checkShadowAvailability(input, fallbackReason);
  }

  async checkShadowAvailability(input = {}, fallbackReason = null) {
    if (!this._ready) await this.connect();
    const roomId = roomIdsFromInput(input)[0];
    const checkinAt = toIsoOrNull(input.checkin_at);
    const checkoutAt = toIsoOrNull(input.checkout_at);
    const s = `"${this.schema}"`;
    const { rows } = await this._client.query(
      `
        SELECT COUNT(*)::int AS overlap_count
        FROM ${s}.reservations r
        JOIN ${s}.reservation_rooms rr
          ON rr.reservation_supabase_uuid = r.supabase_uuid
        WHERE rr.vikbooking_room_id = $1
          AND COALESCE(r.status, '') NOT IN ('cancelled', 'closed')
          AND r.checkin_at < $3::timestamptz
          AND r.checkout_at > $2::timestamptz
      `,
      [roomId, checkinAt, checkoutAt]
    );
    const overlapCount = rows[0]?.overlap_count || 0;
    return {
      available: overlapCount === 0,
      source: "shadow",
      details: {
        overlap_count: overlapCount,
        fallback_reason: fallbackReason
      }
    };
  }

  async getShadowAvailabilityCalendar(input = {}, priceHint = 0, fallbackReason = null) {
    if (!this._ready) await this.connect();

    const roomId = roomIdsFromInput(input)[0] || input.room_id;
    if (!roomId) {
      throw new Error("missing_room_ids");
    }

    const monthStart = normalizeMonthStartYmd(input.month_start || input.monthStart);
    if (!monthStart) {
      throw new Error("invalid_calendar_month");
    }

    const months = clampInt(input.months, 1, 6, 2);
    const rangeEnd = addMonthsYmd(monthStart, months);
    const s = `"${this.schema}"`;
    const { rows } = await this._client.query(
      `
        SELECT r.checkin_at, r.checkout_at
        FROM ${s}.reservations r
        JOIN ${s}.reservation_rooms rr
          ON rr.reservation_supabase_uuid = r.supabase_uuid
        WHERE rr.vikbooking_room_id = $1
          AND COALESCE(r.status, '') NOT IN ('cancelled', 'closed')
          AND r.checkin_at < $3::timestamptz
          AND r.checkout_at > $2::timestamptz
      `,
      [roomId, `${monthStart}T00:00:00.000Z`, `${rangeEnd}T00:00:00.000Z`]
    );

    const blockedDateCounts = new Map();
    for (const row of rows) {
      const checkinDay = toIsoOrNull(row.checkin_at)?.slice(0, 10);
      const checkoutDay = toIsoOrNull(row.checkout_at)?.slice(0, 10);
      if (!checkinDay || !checkoutDay) {
        continue;
      }
      let cursor = checkinDay;
      while (cursor && cursor < checkoutDay) {
        if (cursor >= monthStart && cursor < rangeEnd) {
          blockedDateCounts.set(cursor, (blockedDateCounts.get(cursor) || 0) + 1);
        }
        cursor = addDaysYmd(cursor, 1);
      }
    }

    const property = await this.getPublicProperty(roomId);
    return {
      room_id: Number.parseInt(`${roomId}`, 10) || roomId,
      room_name: property?.name || null,
      source: "shadow",
      first_weekday: 0,
      fallback_reason: fallbackReason,
      months: Array.from({ length: months }, (_, index) => {
        const calendarMonthStart = addMonthsYmd(monthStart, index);
        return calendarMonthStart
          ? buildFallbackCalendarMonth({
              monthStart: calendarMonthStart,
              blockedDateCounts,
              priceHint
            })
          : null;
      }).filter(Boolean)
    };
  }

  async createStripeCheckoutSession(quote, input = {}) {
    if (!this.stripeSecretKey) {
      return {
        provider: "stripe",
        status: "not_configured"
      };
    }
    const methodTypes = this.selectStripePaymentMethodTypes(input);
    const successUrl = `${input.success_url || this.defaultSuccessUrl}`.trim();
    const cancelUrl = `${input.cancel_url || this.defaultCancelUrl}`.trim();
    if (!successUrl || !cancelUrl) {
      throw new Error("missing_channel_redirect_urls");
    }

    const session = await stripeApiRequest({
      secretKey: this.stripeSecretKey,
      path: "/v1/checkout/sessions",
      params: {
        mode: "payment",
        success_url: successUrl,
        cancel_url: cancelUrl,
        "line_items[0][price_data][currency]": quote.currency.toLowerCase(),
        "line_items[0][price_data][product_data][name]":
          input.product_name ||
          `ETR booking ${quote.checkin_at.slice(0, 10)} to ${quote.checkout_at.slice(0, 10)}`,
        "line_items[0][price_data][unit_amount]": Math.round(quote.external_collected_total * 100),
        "line_items[0][quantity]": 1,
        "metadata[external_booking_id]": quote.external_booking_id,
        "metadata[quote_id]": quote.quote_id,
        "metadata[payment_rail]": "stripe",
        payment_method_types: methodTypes
      }
    });

    return {
      provider: "stripe",
      status: session.status || "created",
      id: session.id,
      url: session.url || null,
      payment_method_types: methodTypes
    };
  }

  selectStripePaymentMethodTypes(input = {}) {
    const country = `${input.customer_country || input.country || ""}`.trim().toUpperCase();
    if (country === "US" || country === "USA") {
      return ["us_bank_account", "card"];
    }
    return ["card"];
  }

  async createCryptoInvoiceIntent(quote, input = {}) {
    if (!this.cryptoApiKey) {
      return {
        provider: "nowpayments",
        status: "not_configured"
      };
    }
    const payload = {
      price_amount: quote.external_collected_total,
      price_currency: quote.currency.toLowerCase(),
      pay_currency: `${input.asset || "USDC"}`.toLowerCase(),
      order_id: quote.external_booking_id,
      order_description:
        input.product_name ||
        `ETR booking ${quote.checkin_at.slice(0, 10)} to ${quote.checkout_at.slice(0, 10)}`,
      ipn_callback_url: `${input.ipn_callback_url || ""}`.trim() || undefined,
      success_url: `${input.success_url || this.defaultSuccessUrl}`.trim() || undefined,
      cancel_url: `${input.cancel_url || this.defaultCancelUrl}`.trim() || undefined
    };

    const response = await jsonApiRequest({
      url: `${this.cryptoApiBaseUrl.replace(/\/$/, "")}/payment`,
      headers: {
        "Content-Type": "application/json",
        "x-api-key": this.cryptoApiKey
      },
      body: payload
    });

    return {
      provider: "nowpayments",
      status: response.payment_status || response.status || "created",
      id: response.payment_id || response.id || null,
      invoice_url: response.invoice_url || response.pay_url || null,
      asset: payload.pay_currency.toUpperCase()
    };
  }

  async createCheckoutSessionForQuote(input = {}) {
    if (!this._ready) await this.connect();
    const externalBookingId =
      `${input.external_booking_id || input.externalBookingId || ""}`.trim();
    if (!externalBookingId) {
      throw new Error("missing_external_booking_id");
    }

    const quote = await this.getQuoteByExternalBookingId(externalBookingId);
    if (!quote) {
      throw new Error("quote_not_found");
    }

    const paymentRail = `${input.payment_rail || quote.payment_rail || "stripe"}`
      .trim()
      .toLowerCase();
    if (!["stripe", "crypto"].includes(paymentRail)) {
      throw new Error(`unsupported_payment_rail:${paymentRail}`);
    }

    if (quote.provider_session?.id && input.refresh !== true && input.refresh !== "true") {
      return {
        quote_id: quote.quote_id,
        external_booking_id: quote.external_booking_id,
        payment_rail: paymentRail,
        reused: true,
        provider_session: quote.provider_session
      };
    }

    const checkoutInput = {
      ...input,
      payment_rail: paymentRail
    };
    const providerSession =
      paymentRail === "crypto"
        ? await this.createCryptoInvoiceIntent(quote, checkoutInput)
        : await this.createStripeCheckoutSession(quote, checkoutInput);

    const s = `"${this.schema}"`;
    await this._client.query(
      `
        UPDATE ${s}.channel_quotes
        SET payment_rail = $2, provider_session = $3::jsonb, updated_at = NOW()
        WHERE quote_id = $1
      `,
      [quote.quote_id, paymentRail, JSON.stringify(providerSession || {})]
    );

    return {
      quote_id: quote.quote_id,
      external_booking_id: quote.external_booking_id,
      payment_rail: paymentRail,
      provider_session: providerSession
    };
  }

  async ingestStripeWebhook({ rawBody, signatureHeader }) {
    const verified = verifyStripeSignature({
      rawBody,
      signatureHeader,
      secret: this.stripeWebhookSecret
    });
    if (!verified.ok) {
      throw new Error(`stripe_signature_failed:${verified.reason}`);
    }
    const event = JSON.parse(rawBody || "{}");
    const normalized = stripeEventToCharge(event);
    if (!normalized.externalBookingId) {
      return {
        accepted: true,
        skipped: true,
        reason: "missing_external_booking_id"
      };
    }
    if (
      ![
        "checkout.session.completed",
        "payment_intent.succeeded",
        "charge.succeeded",
        "payment_intent.payment_failed",
        "charge.failed"
      ].includes(event.type)
    ) {
      return {
        accepted: true,
        skipped: true,
        reason: `unhandled_event:${event.type}`
      };
    }

    if (event.type.endsWith(".failed")) {
      await this.recordManualException({
        externalBookingId: normalized.externalBookingId,
        reason: "stripe_payment_failed",
        details: event
      });
      return {
        accepted: true,
        status: "failed_recorded"
      };
    }

    return this.handleSuccessfulPayment({
      provider: "stripe",
      paymentRail: "stripe",
      externalBookingId: normalized.externalBookingId,
      providerReference: normalized.providerChargeId,
      grossAmount: normalized.grossAmount,
      currency: normalized.currency,
      providerStatus: normalized.providerStatus,
      rawPayload: normalized.rawPayload
    });
  }

  async ingestCryptoWebhook({ rawBody, signatureHeader }) {
    const verified = verifyCryptoSignature({
      rawBody,
      signatureHeader,
      secret: this.cryptoWebhookSecret
    });
    if (!verified.ok) {
      throw new Error(`crypto_signature_failed:${verified.reason}`);
    }
    const payload = JSON.parse(rawBody || "{}");
    const normalized = cryptoPayloadToInvoice(payload);
    if (!normalized.externalBookingId) {
      return {
        accepted: true,
        skipped: true,
        reason: "missing_external_booking_id"
      };
    }
    if (!["finished", "confirmed", "completed"].includes(`${normalized.providerStatus || ""}`.toLowerCase())) {
      return {
        accepted: true,
        skipped: true,
        reason: `non_final_status:${normalized.providerStatus || "unknown"}`
      };
    }

    return this.handleSuccessfulPayment({
      provider: "nowpayments",
      paymentRail: "crypto",
      externalBookingId: normalized.externalBookingId,
      providerReference: normalized.providerInvoiceId,
      providerStatus: normalized.providerStatus,
      asset: normalized.asset,
      network: normalized.network,
      txReference: normalized.txReference,
      rawPayload: normalized.rawPayload
    });
  }

  async handleSuccessfulPayment({
    provider,
    paymentRail,
    externalBookingId,
    providerReference,
    grossAmount = null,
    currency = "USD",
    providerStatus = "succeeded",
    asset = null,
    network = null,
    txReference = null,
    rawPayload = {}
  }) {
    if (!this._ready) await this.connect();
    const quote = await this.getQuoteByExternalBookingId(externalBookingId);
    if (!quote) {
      await this.recordManualException({
        externalBookingId,
        reason: "missing_quote_for_paid_booking",
        details: rawPayload
      });
      return {
        accepted: true,
        status: "manual_exception",
        reason: "missing_quote"
      };
    }

    if (quote.website_reservation_id) {
      return {
        accepted: true,
        status: "already_finalized",
        website_reservation_id: quote.website_reservation_id
      };
    }

    if (paymentRail === "stripe") {
      await this.upsertPlatformCharge({
        chargeId: `charge_${providerReference || externalBookingId}`,
        externalBookingId,
        quoteId: quote.quote_id,
        provider,
        paymentRail,
        providerChargeId: providerReference,
        status: providerStatus,
        currency,
        grossAmount: grossAmount ?? quote.external_collected_total,
        websiteBookingTotal: quote.website_booking_total,
        platformRetainedAmount: quote.platform_retained_amount,
        rawPayload
      });
    } else {
      await this.upsertCryptoInvoice({
        invoiceId: `invoice_${providerReference || externalBookingId}`,
        externalBookingId,
        quoteId: quote.quote_id,
        provider,
        providerInvoiceId: providerReference,
        status: providerStatus,
        asset,
        network,
        quoteAmount: quote.external_collected_total,
        websiteBookingTotal: quote.website_booking_total,
        retainedSpread: quote.platform_retained_amount,
        txReference,
        rawPayload
      });
    }

    const bookingResult = await this.createWebsiteReservationFromQuote(quote, {
      provider,
      providerReference,
      paymentRail,
      providerStatus,
      grossAmount: grossAmount ?? quote.external_collected_total,
      currency,
      asset,
      network,
      txReference
    });

    if (!bookingResult.executed || !bookingResult.reservation_id) {
      await this.recordManualException({
        externalBookingId,
        quoteId: quote.quote_id,
        reason: "website_reservation_create_failed",
        details: bookingResult
      });
      return {
        accepted: true,
        status: "manual_exception",
        reason: "website_reservation_create_failed"
      };
    }

    await this.markQuoteBooked(quote.quote_id, bookingResult.reservation_id);

    if (paymentRail === "stripe") {
      await this.attachReservationIdToCharge(externalBookingId, bookingResult.reservation_id);
      await this.createConnectedTransferForQuote(quote, providerReference, bookingResult.reservation_id);
    } else {
      await this.attachReservationIdToCryptoInvoice(externalBookingId, bookingResult.reservation_id);
    }

    return {
      accepted: true,
      status: "booking_created",
      website_reservation_id: bookingResult.reservation_id
    };
  }

  async createWebsiteReservationFromQuote(quote, payment = {}) {
    const roomIds = asArray(quote.room_ids);
    const payload = {
      command_id: `reservation_${quote.external_booking_id}`,
      command_type: "reservation.create",
      reservation: {
        external_booking_id: quote.external_booking_id,
        room_ids: roomIds,
        checkin_at: quote.checkin_at,
        checkout_at: quote.checkout_at,
        adults: quote.adults,
        children: quote.children,
        currency: quote.currency,
        status: "confirmed",
        channel: `platform_${quote.payment_rail}`,
        website_booking_total: quote.website_booking_total,
        external_collected_total: quote.external_collected_total,
        platform_retained_amount: quote.platform_retained_amount
      },
      customer: quote.customer,
      payment: {
        provider: payment.provider || quote.payment_rail,
        payment_rail: quote.payment_rail,
        provider_reference: payment.providerReference || null,
        provider_status: payment.providerStatus || "succeeded",
        gross_amount: payment.grossAmount ?? quote.external_collected_total,
        website_booking_total: quote.website_booking_total,
        platform_retained_amount: quote.platform_retained_amount,
        currency: payment.currency || quote.currency,
        asset: payment.asset || null,
        network: payment.network || null,
        tx_reference: payment.txReference || null,
        final_sale: true
      },
      attribution: quote.attribution || {}
    };
    return this.sendWordPressCommand(payload);
  }

  async sendWordPressCommand(payload) {
    if (!this.wordpressBaseUrl || !this.wordpressCommandSecret) {
      throw new Error("wordpress_command_not_configured");
    }
    const rawBody = JSON.stringify(payload);
    const timestamp = `${Math.floor(Date.now() / 1000)}`;
    const nonce = `cmd_${randomUUID()}`;
    const signature = computeSyncSignature({
      secret: this.wordpressCommandSecret,
      timestamp,
      nonce,
      rawBody
    });
    return jsonApiRequest({
      url: `${this.wordpressBaseUrl.replace(/\/$/, "")}${this.wordpressCommandEndpoint}`,
      headers: {
        "Content-Type": "application/json",
        "X-Sync-Signature": signature,
        "X-Sync-Timestamp": timestamp,
        "X-Sync-Nonce": nonce
      },
      body: payload,
      allowedStatusCodes: [409]
    });
  }

  async emitOpsChangeEvent({
    type = "sync.updated",
    changedDomains = [],
    recommendedWindow = null,
    payload = {}
  } = {}) {
    if (!this._client) {
      return null;
    }
    try {
      const { rows } = await this._client.query(
        `
          INSERT INTO ops.change_events
            (event_type, changed_domains, recommended_window, payload)
          VALUES ($1, $2::text[], $3::jsonb, $4::jsonb)
          RETURNING id::text AS cursor, created_at
        `,
        [
          `${type || "sync.updated"}`.trim() || "sync.updated",
          Array.from(
            new Set(
              (Array.isArray(changedDomains) ? changedDomains : [])
                .map((entry) => `${entry || ""}`.trim())
                .filter(Boolean)
            )
          ),
          JSON.stringify(recommendedWindow || {}),
          JSON.stringify(payload || {})
        ]
      );
      return rows[0] || null;
    } catch (error) {
      if (["42P01", "42703", "3F000"].includes(error?.code)) {
        return null;
      }
      throw error;
    }
  }

  async upsertMetaSpend(rows = []) {
    if (!this._ready) await this.connect();
    const s = `"${this.schema}"`;
    let count = 0;
    for (const row of rows) {
      const campaignKey = `${row.campaign_key || row.campaign_id || row.campaign || ""}`.trim();
      const spendDate = `${row.spend_date || row.date || ""}`.trim();
      if (!campaignKey || !spendDate) {
        continue;
      }
      count += 1;
      await this._client.query(
        `
          INSERT INTO ${s}.meta_campaign_spend
            (campaign_key, spend_date, spend, currency, impressions, clicks, raw_payload, updated_at)
          VALUES ($1, $2::date, $3, $4, $5, $6, $7::jsonb, NOW())
          ON CONFLICT (campaign_key, spend_date)
          DO UPDATE SET
            spend = EXCLUDED.spend,
            currency = EXCLUDED.currency,
            impressions = EXCLUDED.impressions,
            clicks = EXCLUDED.clicks,
            raw_payload = EXCLUDED.raw_payload,
            updated_at = NOW()
        `,
        [
          campaignKey,
          spendDate,
          roundMoney(row.spend),
          `${row.currency || "USD"}`.toUpperCase(),
          row.impressions ?? null,
          row.clicks ?? null,
          JSON.stringify(row)
        ]
      );
    }
    return { accepted: true, imported: count };
  }

  async exportAudienceSegments(segmentKeys = []) {
    if (!this._ready) await this.connect();
    const requestedSegments = segmentKeys.length ? segmentKeys : [
      "high_value_villa_esencia",
      "repeat_keylime",
      "repeat_direct_lake",
      "qualified_vrbo_guests",
      "direct_booking_exclusion"
    ];
    const s = `"${this.schema}"`;
    const { rows } = await this._client.query(
      `
        SELECT
          a.segment_key,
          g.guest_key,
          g.primary_email,
          g.primary_phone,
          g.first_name,
          g.last_name
        FROM ${s}.audience_segments a
        JOIN ${s}.guest_profiles g
          ON g.guest_key = a.guest_key
        WHERE a.segment_key = ANY($1::text[])
        ORDER BY a.segment_key, g.guest_key
      `,
      [requestedSegments]
    );

    const header = ["segment_key", "guest_key", "email", "phone", "fn", "ln"];
    const csvRows = [header.join(",")];
    for (const row of rows) {
      csvRows.push(
        [
          row.segment_key,
          row.guest_key,
          hashAudienceField(row.primary_email, normalizeEmail),
          hashAudienceField(row.primary_phone, normalizePhone),
          hashAudienceField(row.first_name, normalizeName),
          hashAudienceField(row.last_name, normalizeName)
        ].join(",")
      );
    }

    return {
      accepted: true,
      segments: requestedSegments,
      row_count: rows.length,
      csv: csvRows.join("\n")
    };
  }

  async refreshGuestProfileByCustomer(customerVikbookingId) {
    if (!this._ready) await this.connect();
    const s = `"${this.schema}"`;
    const { rows } = await this._client.query(
      `
        SELECT
          c.supabase_uuid AS customer_supabase_uuid,
          c.email,
          c.phone,
          c.first_name,
          c.last_name,
          r.status,
          r.total,
          r.checkin_at,
          r.checkout_at,
          r.channel,
          rr.vikbooking_room_id AS room_id,
          COALESCE((r.source_payload -> 'rooms' -> 0 ->> 'room_name'), rr.vikbooking_room_id) AS room_name
        FROM ${s}.customers c
        LEFT JOIN ${s}.reservations r
          ON r.customer_vikbooking_id = c.vikbooking_id
        LEFT JOIN ${s}.reservation_rooms rr
          ON rr.reservation_supabase_uuid = r.supabase_uuid
        WHERE c.vikbooking_id = $1
      `,
      [`${customerVikbookingId}`]
    );
    if (!rows.length) {
      return null;
    }

    const customer = rows[0];
    const guestKey = `customer:${customer.customer_supabase_uuid}`;
    const confirmedRows = rows.filter((row) => `${row.status || ""}`.toLowerCase() === "confirmed");
    const activeRows = rows.filter((row) => !["cancelled"].includes(`${row.status || ""}`.toLowerCase()));
    const totalRevenue = roundMoney(
      confirmedRows.reduce((sum, row) => sum + Number(row.total || 0), 0)
    );
    const directStays = confirmedRows.filter(
      (row) => !/vrbo/i.test(`${row.channel || ""}`)
    ).length;
    const vrboStays = confirmedRows.filter((row) => /vrbo/i.test(`${row.channel || ""}`)).length;
    const lastStayAt = confirmedRows
      .map((row) => row.checkout_at)
      .filter(Boolean)
      .sort()
      .slice(-1)[0] || null;
    const nextStayAt = activeRows
      .map((row) => row.checkin_at)
      .filter((value) => value && new Date(value).getTime() > Date.now())
      .sort()[0] || null;

    const profile = {
      guest_key: guestKey,
      customer_supabase_uuid: customer.customer_supabase_uuid,
      primary_email: customer.email || null,
      primary_phone: customer.phone || null,
      first_name: customer.first_name || null,
      last_name: customer.last_name || null,
      total_stays: activeRows.length,
      confirmed_stays: confirmedRows.length,
      total_revenue: totalRevenue,
      direct_stays: directStays,
      vrbo_stays: vrboStays,
      last_stay_at: lastStayAt,
      next_stay_at: nextStayAt,
      primary_room_name: choosePrimaryRoom(confirmedRows.length ? confirmedRows : activeRows),
      contact_completeness: [customer.email, customer.phone].filter(Boolean).length,
      seed_quality_score: 0
    };
    profile.seed_quality_score = computeSeedQuality(profile);

    await this._client.query(
      `
        INSERT INTO ${s}.guest_profiles
          (
            guest_key,
            customer_supabase_uuid,
            primary_email,
            primary_phone,
            first_name,
            last_name,
            total_stays,
            confirmed_stays,
            total_revenue,
            direct_stays,
            vrbo_stays,
            last_stay_at,
            next_stay_at,
            primary_room_name,
            contact_completeness,
            seed_quality_score,
            profile_json,
            updated_at
          )
        VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::timestamptz, $13::timestamptz, $14, $15, $16, $17::jsonb, NOW())
        ON CONFLICT (guest_key)
        DO UPDATE SET
          primary_email = EXCLUDED.primary_email,
          primary_phone = EXCLUDED.primary_phone,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          total_stays = EXCLUDED.total_stays,
          confirmed_stays = EXCLUDED.confirmed_stays,
          total_revenue = EXCLUDED.total_revenue,
          direct_stays = EXCLUDED.direct_stays,
          vrbo_stays = EXCLUDED.vrbo_stays,
          last_stay_at = EXCLUDED.last_stay_at,
          next_stay_at = EXCLUDED.next_stay_at,
          primary_room_name = EXCLUDED.primary_room_name,
          contact_completeness = EXCLUDED.contact_completeness,
          seed_quality_score = EXCLUDED.seed_quality_score,
          profile_json = EXCLUDED.profile_json,
          updated_at = NOW()
      `,
      [
        profile.guest_key,
        profile.customer_supabase_uuid,
        profile.primary_email,
        profile.primary_phone,
        profile.first_name,
        profile.last_name,
        profile.total_stays,
        profile.confirmed_stays,
        profile.total_revenue,
        profile.direct_stays,
        profile.vrbo_stays,
        profile.last_stay_at,
        profile.next_stay_at,
        profile.primary_room_name,
        profile.contact_completeness,
        profile.seed_quality_score,
        JSON.stringify(profile)
      ]
    );

    await this._client.query(`DELETE FROM ${s}.audience_segments WHERE guest_key = $1`, [guestKey]);
    for (const segmentKey of segmentFromProfile(profile)) {
      await this._client.query(
        `
          INSERT INTO ${s}.audience_segments (segment_key, guest_key, qualification_reason, updated_at)
          VALUES ($1, $2, $3, NOW())
          ON CONFLICT (segment_key, guest_key)
          DO UPDATE SET qualification_reason = EXCLUDED.qualification_reason, updated_at = NOW()
        `,
        [segmentKey, guestKey, `seed_quality:${profile.seed_quality_score}`]
      );
    }

    return profile;
  }

  async getQuoteByExternalBookingId(externalBookingId) {
    if (!this._ready) await this.connect();
    const s = `"${this.schema}"`;
    const { rows } = await this._client.query(
      `SELECT * FROM ${s}.channel_quotes WHERE external_booking_id = $1 LIMIT 1`,
      [externalBookingId]
    );
    if (!rows[0]) {
      return null;
    }
    return this.hydrateQuote(rows[0]);
  }

  async getGuestPortalReservation(input = {}) {
    if (!this._ready) await this.connect();
    const reference = `${input.reference || input.booking_reference || input.external_booking_id || ""}`.trim();
    const email = `${input.email || ""}`.trim().toLowerCase();
    if (!reference || !email) {
      throw new Error("missing_portal_credentials");
    }

    const s = `"${this.schema}"`;
    const { rows } = await this._client.query(
      `
        SELECT *
        FROM ${s}.channel_quotes
        WHERE (
          external_booking_id = $1
          OR website_reservation_id = $1
          OR quote_id = $1
        )
          AND LOWER(COALESCE(customer ->> 'email', '')) = $2
        ORDER BY updated_at DESC
        LIMIT 1
      `,
      [reference, email]
    );

    if (!rows[0]) {
      return null;
    }

    const quote = this.hydrateQuote(rows[0]);
    const reservation = await this.getReservationStatus(quote.external_booking_id);
    const property = quote.room_ids?.[0] ? await this.getPublicProperty(quote.room_ids[0]) : null;
    const matchedReferenceType =
      quote.website_reservation_id === reference
        ? "website_reservation_id"
        : quote.quote_id === reference
          ? "quote_id"
          : "external_booking_id";

    return {
      reference,
      matched_reference_type: matchedReferenceType,
      guest: {
        email: quote.customer?.email || email,
        first_name: quote.customer?.first_name || null,
        last_name: quote.customer?.last_name || null,
        phone: quote.customer?.phone || null
      },
      reservation,
      property
    };
  }

  async getReservationStatus(externalBookingId) {
    if (!this._ready) await this.connect();
    const quote = await this.getQuoteByExternalBookingId(externalBookingId);
    if (!quote) {
      return null;
    }

    const s = `"${this.schema}"`;
    const [chargeResult, invoiceResult] = await Promise.all([
      this._client.query(
        `
          SELECT provider, status, currency, gross_amount, website_reservation_id, provider_charge_id, updated_at
          FROM ${s}.platform_charges
          WHERE external_booking_id = $1
          ORDER BY updated_at DESC
          LIMIT 1
        `,
        [externalBookingId]
      ),
      this._client.query(
        `
          SELECT provider, status, asset, network, quote_amount, website_reservation_id, provider_invoice_id, updated_at
          FROM ${s}.crypto_invoices
          WHERE external_booking_id = $1
          ORDER BY updated_at DESC
          LIMIT 1
        `,
        [externalBookingId]
      )
    ]);

    return {
      quote_id: quote.quote_id,
      external_booking_id: quote.external_booking_id,
      status: quote.website_reservation_id ? "booking_created" : quote.status,
      payment_rail: quote.payment_rail,
      customer: quote.customer,
      stay: {
        room_ids: quote.room_ids,
        checkin_at: quote.checkin_at,
        checkout_at: quote.checkout_at,
        adults: quote.adults,
        children: quote.children
      },
      amounts: {
        currency: quote.currency,
        website_booking_total: quote.website_booking_total,
        external_collected_total: quote.external_collected_total,
        platform_retained_amount: quote.platform_retained_amount
      },
      provider_session: quote.provider_session,
      website_reservation_id: quote.website_reservation_id || null,
      latest_charge: chargeResult.rows[0] || null,
      latest_invoice: invoiceResult.rows[0] || null
    };
  }

  hydrateQuote(row) {
    return {
      ...row,
      room_ids: Array.isArray(row.room_ids) ? row.room_ids : JSON.parse(row.room_ids || "[]"),
      customer: typeof row.customer === "object" ? row.customer : JSON.parse(row.customer || "{}"),
      attribution:
        typeof row.attribution === "object" ? row.attribution : JSON.parse(row.attribution || "{}"),
      availability_snapshot:
        typeof row.availability_snapshot === "object"
          ? row.availability_snapshot
          : JSON.parse(row.availability_snapshot || "{}"),
      provider_session:
        typeof row.provider_session === "object"
          ? row.provider_session
          : JSON.parse(row.provider_session || "{}")
    };
  }

  async markQuoteBooked(quoteId, reservationId) {
    const s = `"${this.schema}"`;
    await this._client.query(
      `
        UPDATE ${s}.channel_quotes
        SET status = 'booking_created', website_reservation_id = $2, updated_at = NOW()
        WHERE quote_id = $1
      `,
      [quoteId, `${reservationId}`]
    );
  }

  async upsertPlatformCharge({
    chargeId,
    externalBookingId,
    quoteId,
    provider,
    paymentRail,
    providerChargeId,
    status,
    currency,
    grossAmount,
    websiteBookingTotal,
    platformRetainedAmount,
    rawPayload
  }) {
    const s = `"${this.schema}"`;
    await this._client.query(
      `
        INSERT INTO ${s}.platform_charges
          (
            charge_id,
            external_booking_id,
            quote_id,
            provider,
            payment_rail,
            provider_charge_id,
            status,
            currency,
            gross_amount,
            website_booking_total,
            platform_retained_amount,
            raw_payload,
            updated_at
          )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, NOW())
        ON CONFLICT (charge_id)
        DO UPDATE SET
          status = EXCLUDED.status,
          provider_charge_id = COALESCE(EXCLUDED.provider_charge_id, ${s}.platform_charges.provider_charge_id),
          currency = COALESCE(EXCLUDED.currency, ${s}.platform_charges.currency),
          gross_amount = COALESCE(EXCLUDED.gross_amount, ${s}.platform_charges.gross_amount),
          website_booking_total = COALESCE(EXCLUDED.website_booking_total, ${s}.platform_charges.website_booking_total),
          platform_retained_amount = COALESCE(EXCLUDED.platform_retained_amount, ${s}.platform_charges.platform_retained_amount),
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
      `,
      [
        chargeId,
        externalBookingId,
        quoteId,
        provider,
        paymentRail,
        providerChargeId,
        status,
        currency,
        grossAmount,
        websiteBookingTotal,
        platformRetainedAmount,
        JSON.stringify(rawPayload || {})
      ]
    );
  }

  async upsertCryptoInvoice({
    invoiceId,
    externalBookingId,
    quoteId,
    provider,
    providerInvoiceId,
    status,
    asset,
    network,
    quoteAmount,
    websiteBookingTotal,
    retainedSpread,
    txReference,
    rawPayload
  }) {
    const s = `"${this.schema}"`;
    await this._client.query(
      `
        INSERT INTO ${s}.crypto_invoices
          (
            invoice_id,
            external_booking_id,
            quote_id,
            provider,
            provider_invoice_id,
            status,
            asset,
            network,
            quote_amount,
            website_booking_total,
            retained_spread,
            tx_reference,
            raw_payload,
            updated_at
          )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, NOW())
        ON CONFLICT (invoice_id)
        DO UPDATE SET
          status = EXCLUDED.status,
          provider_invoice_id = COALESCE(EXCLUDED.provider_invoice_id, ${s}.crypto_invoices.provider_invoice_id),
          asset = COALESCE(EXCLUDED.asset, ${s}.crypto_invoices.asset),
          network = COALESCE(EXCLUDED.network, ${s}.crypto_invoices.network),
          quote_amount = COALESCE(EXCLUDED.quote_amount, ${s}.crypto_invoices.quote_amount),
          website_booking_total = COALESCE(EXCLUDED.website_booking_total, ${s}.crypto_invoices.website_booking_total),
          retained_spread = COALESCE(EXCLUDED.retained_spread, ${s}.crypto_invoices.retained_spread),
          tx_reference = COALESCE(EXCLUDED.tx_reference, ${s}.crypto_invoices.tx_reference),
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
      `,
      [
        invoiceId,
        externalBookingId,
        quoteId,
        provider,
        providerInvoiceId,
        status,
        asset,
        network,
        quoteAmount,
        websiteBookingTotal,
        retainedSpread,
        txReference,
        JSON.stringify(rawPayload || {})
      ]
    );
  }

  async attachReservationIdToCharge(externalBookingId, reservationId) {
    const s = `"${this.schema}"`;
    await this._client.query(
      `
        UPDATE ${s}.platform_charges
        SET website_reservation_id = $2, updated_at = NOW()
        WHERE external_booking_id = $1
      `,
      [externalBookingId, `${reservationId}`]
    );
  }

  async attachReservationIdToCryptoInvoice(externalBookingId, reservationId) {
    const s = `"${this.schema}"`;
    await this._client.query(
      `
        UPDATE ${s}.crypto_invoices
        SET website_reservation_id = $2, updated_at = NOW()
        WHERE external_booking_id = $1
      `,
      [externalBookingId, `${reservationId}`]
    );
  }

  async createConnectedTransferForQuote(quote, providerChargeId, reservationId) {
    if (!this.stripeConnectedAccount || !this.stripeSecretKey) {
      await this.recordManualException({
        externalBookingId: quote.external_booking_id,
        quoteId: quote.quote_id,
        reason: "stripe_transfer_not_configured",
        details: {
          provider_charge_id: providerChargeId,
          reservation_id: reservationId
        }
      });
      return null;
    }

    const transfer = await stripeApiRequest({
      secretKey: this.stripeSecretKey,
      path: "/v1/transfers",
      params: {
        amount: Math.round(Number(quote.etr_transfer_amount || 0) * 100),
        currency: `${quote.currency || "USD"}`.toLowerCase(),
        destination: this.stripeConnectedAccount,
        description: `ETR share for ${quote.external_booking_id}`,
        "metadata[external_booking_id]": quote.external_booking_id,
        "metadata[website_reservation_id]": `${reservationId}`,
        source_transaction: providerChargeId || undefined
      }
    });

    const s = `"${this.schema}"`;
    await this._client.query(
      `
        INSERT INTO ${s}.connected_transfers
          (
            transfer_id,
            charge_id,
            external_booking_id,
            provider,
            provider_transfer_id,
            destination_account_id,
            amount,
            currency,
            status,
            raw_payload,
            updated_at
          )
        VALUES ($1, $2, $3, 'stripe', $4, $5, $6, $7, $8, $9::jsonb, NOW())
        ON CONFLICT (transfer_id)
        DO UPDATE SET
          provider_transfer_id = COALESCE(EXCLUDED.provider_transfer_id, ${s}.connected_transfers.provider_transfer_id),
          destination_account_id = COALESCE(EXCLUDED.destination_account_id, ${s}.connected_transfers.destination_account_id),
          amount = COALESCE(EXCLUDED.amount, ${s}.connected_transfers.amount),
          currency = COALESCE(EXCLUDED.currency, ${s}.connected_transfers.currency),
          status = EXCLUDED.status,
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
      `,
      [
        `transfer_${transfer.id || randomUUID()}`,
        `charge_${providerChargeId || quote.external_booking_id}`,
        quote.external_booking_id,
        transfer.id || null,
        this.stripeConnectedAccount,
        quote.etr_transfer_amount,
        quote.currency,
        transfer.status || "pending",
        JSON.stringify(transfer)
      ]
    );

    return transfer;
  }

  async recordManualException({
    externalBookingId = null,
    quoteId = null,
    reason,
    details = {}
  }) {
    if (!this._ready) await this.connect();
    const s = `"${this.schema}"`;
    await this._client.query(
      `
        INSERT INTO ${s}.manual_exceptions
          (external_booking_id, quote_id, reason, details, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4::jsonb, 'open', NOW(), NOW())
      `,
      [externalBookingId, quoteId, reason, JSON.stringify(details || {})]
    );
  }

  async backfillSyncFromRawRows(options = {}) {
    if (!this._ready) await this.connect();

    const rawSchema = validateIdentifier(options.rawSchema || "public", "raw_schema");
    const rawTable = validateIdentifier(options.rawTable || "wp_raw_rows", "raw_table");
    const sourcePrefix = `${options.sourcePrefix || "wp_"}`.trim() || "wp_";
    const rawRelation = `${quotePgIdentifier(rawSchema)}.${quotePgIdentifier(rawTable)}`;

    const fetchRawRows = async (sourceTable) => {
      const { rows } = await this._client.query(
        `
          SELECT source_pk, row_json
          FROM ${rawRelation}
          WHERE source_table = $1
          ORDER BY LENGTH(source_pk), source_pk
        `,
        [sourceTable]
      );
      return rows.map((row) => row.row_json || {});
    };

    const [
      rawOrders,
      rawOrderRooms,
      rawCustomers,
      rawCustomerLinks,
      rawPaymentMethods
    ] = await Promise.all([
      fetchRawRows(`${sourcePrefix}vikbooking_orders`),
      fetchRawRows(`${sourcePrefix}vikbooking_ordersrooms`),
      fetchRawRows(`${sourcePrefix}vikbooking_customers`),
      fetchRawRows(`${sourcePrefix}vikbooking_customers_orders`),
      fetchRawRows(`${sourcePrefix}vikbooking_gpayments`)
    ]);

    const customersById = new Map();
    const customersByEmail = new Map();
    for (const customer of rawCustomers) {
      const customerId = `${customer?.id ?? ""}`.trim();
      if (!customerId) {
        continue;
      }
      customersById.set(customerId, customer);
      const email = normalizeEmail(customer?.email);
      if (email && !customersByEmail.has(email)) {
        customersByEmail.set(email, customer);
      }
    }

    const customerIdByOrderId = new Map();
    for (const link of rawCustomerLinks) {
      const orderId = `${link?.idorder ?? ""}`.trim();
      const customerId = `${link?.idcustomer ?? ""}`.trim();
      if (orderId && customerId) {
        customerIdByOrderId.set(orderId, customerId);
      }
    }

    const roomsByOrderId = new Map();
    for (const room of rawOrderRooms) {
      const orderId = `${room?.idorder ?? ""}`.trim();
      if (!orderId) {
        continue;
      }
      if (!roomsByOrderId.has(orderId)) {
        roomsByOrderId.set(orderId, []);
      }
      roomsByOrderId.get(orderId).push(room);
    }

    const paymentMethodsById = new Map();
    for (const paymentMethod of rawPaymentMethods) {
      const paymentId = `${paymentMethod?.id ?? ""}`.trim();
      if (paymentId) {
        paymentMethodsById.set(paymentId, paymentMethod);
      }
    }

    const summary = {
      orders_found: rawOrders.length,
      processed: 0,
      duplicates: 0,
      linked_customers: 0,
      email_matched_customers: 0,
      rooms_projected: 0
    };

    for (const order of rawOrders) {
      const orderId = `${order?.id ?? ""}`.trim();
      if (!orderId) {
        continue;
      }

      let customerId = customerIdByOrderId.get(orderId) || null;
      let customer = customerId ? customersById.get(customerId) || null : null;
      if (customerId && customer) {
        summary.linked_customers += 1;
      }

      if (!customerId) {
        const fallbackEmail = normalizeEmail(order?.custmail);
        const emailMatchedCustomer = fallbackEmail ? customersByEmail.get(fallbackEmail) || null : null;
        if (emailMatchedCustomer?.id !== undefined && emailMatchedCustomer?.id !== null) {
          customerId = `${emailMatchedCustomer.id}`;
          customer = emailMatchedCustomer;
          summary.email_matched_customers += 1;
        }
      }

      const rooms = roomsByOrderId.get(orderId) || [];
      summary.rooms_projected += rooms.length;
      const paymentMethod =
        order?.idpayment === undefined || order?.idpayment === null || `${order.idpayment}`.trim() === ""
          ? null
          : paymentMethodsById.get(`${order.idpayment}`) || null;

      const event = buildReservationBackfillEvent({
        order,
        linkedCustomerId: customerId,
        customer,
        rooms,
        paymentMethod
      });
      const result = await this.processEvent(event);
      if (result?.duplicate) {
        summary.duplicates += 1;
      } else if (result?.accepted) {
        summary.processed += 1;
      }
    }

    return summary;
  }
}
