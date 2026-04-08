import crypto from "node:crypto";
import { Client } from "pg";
import { buildPgConnectionOptions } from "./pgConnection.js";

function validateIdentifier(value, label) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Invalid ${label}: ${value}`);
  }
  return value;
}

function quoteIdentifier(value) {
  return `"${validateIdentifier(value, "identifier").replace(/"/g, "\"\"")}"`;
}

function toIsoOrNull(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function nowIso() {
  return new Date().toISOString();
}

export function computeSyncSignature({ secret, timestamp, nonce, rawBody }) {
  return crypto
    .createHmac("sha256", secret)
    .update(`${timestamp}.${nonce}.${rawBody}`)
    .digest("hex");
}

export function verifySyncRequest({
  secret,
  signature,
  timestamp,
  nonce,
  rawBody,
  maxSkewSeconds = 300
}) {
  if (!secret) {
    return { ok: false, reason: "missing_secret" };
  }
  if (!signature || !timestamp || !nonce) {
    return { ok: false, reason: "missing_headers" };
  }
  const ts = Number.parseInt(timestamp, 10);
  if (Number.isNaN(ts)) {
    return { ok: false, reason: "invalid_timestamp" };
  }
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - ts) > maxSkewSeconds) {
    return { ok: false, reason: "timestamp_skew" };
  }
  const expected = computeSyncSignature({ secret, timestamp, nonce, rawBody });
  const sigA = Buffer.from(expected, "hex");
  let sigB;
  try {
    sigB = Buffer.from(signature, "hex");
  } catch {
    return { ok: false, reason: "invalid_signature_encoding" };
  }
  if (sigA.length !== sigB.length) {
    return { ok: false, reason: "signature_length_mismatch" };
  }
  if (!crypto.timingSafeEqual(sigA, sigB)) {
    return { ok: false, reason: "signature_mismatch" };
  }
  return { ok: true };
}

export function validateSyncEvent(event) {
  const required = [
    "event_id",
    "event_type",
    "occurred_at",
    "source_entity",
    "source_id",
    "idempotency_key",
    "payload"
  ];
  for (const key of required) {
    if (!(key in (event || {}))) {
      return { valid: false, reason: `missing_field:${key}` };
    }
  }
  if (typeof event.event_id !== "string" || event.event_id.length < 8) {
    return { valid: false, reason: "invalid_event_id" };
  }
  if (typeof event.idempotency_key !== "string" || event.idempotency_key.length < 8) {
    return { valid: false, reason: "invalid_idempotency_key" };
  }
  if (typeof event.payload !== "object" || event.payload === null) {
    return { valid: false, reason: "invalid_payload" };
  }
  return { valid: true };
}

export class SyncIngestService {
  constructor({
    databaseUrl,
    schema = "sync",
    projectionVersion = 1,
    nonceTtlSeconds = 600
  }) {
    this.databaseUrl = databaseUrl;
    this.schema = validateIdentifier(schema, "schema");
    this.projectionVersion = projectionVersion;
    this.nonceTtlSeconds = nonceTtlSeconds;
    this._client = null;
    this._ready = false;
  }

  async connect() {
    if (this._client) return;
    this._client = new Client(buildPgConnectionOptions(this.databaseUrl));
    await this._client.connect();
    await this._ensureSchema();
    this._ready = true;
  }

  async close() {
    if (!this._client) return;
    await this._client.end();
    this._client = null;
    this._ready = false;
  }

  async _ensureSchema() {
    const s = quoteIdentifier(this.schema);
    await this._client.query(`CREATE SCHEMA IF NOT EXISTS ${s}`);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.raw_events (
        event_id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        source_entity TEXT NOT NULL,
        source_id TEXT NOT NULL,
        idempotency_key TEXT UNIQUE NOT NULL,
        payload JSONB NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.id_map (
        entity_type TEXT NOT NULL,
        vikbooking_id TEXT NOT NULL,
        supabase_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (entity_type, vikbooking_id)
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.reservations (
        supabase_uuid UUID PRIMARY KEY,
        vikbooking_id TEXT UNIQUE NOT NULL,
        status TEXT,
        total NUMERIC,
        total_paid NUMERIC,
        customer_vikbooking_id TEXT,
        checkin_at TIMESTAMPTZ,
        checkout_at TIMESTAMPTZ,
        source_updated_at TIMESTAMPTZ,
        projection_version INT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.reservation_rooms (
        reservation_supabase_uuid UUID NOT NULL REFERENCES ${s}.reservations(supabase_uuid) ON DELETE CASCADE,
        vikbooking_room_link_id TEXT NOT NULL,
        vikbooking_room_id TEXT,
        adults INT,
        children INT,
        room_cost NUMERIC,
        source_updated_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (reservation_supabase_uuid, vikbooking_room_link_id)
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.customers (
        supabase_uuid UUID PRIMARY KEY,
        vikbooking_id TEXT UNIQUE NOT NULL,
        email TEXT,
        first_name TEXT,
        last_name TEXT,
        phone TEXT,
        source_updated_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.payments (
        payment_key TEXT PRIMARY KEY,
        reservation_supabase_uuid UUID REFERENCES ${s}.reservations(supabase_uuid) ON DELETE CASCADE,
        payment_status TEXT,
        payment_amount NUMERIC,
        currency TEXT,
        source_updated_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.sync_state (
        stream_key TEXT PRIMARY KEY,
        watermark TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.sync_errors (
        id BIGSERIAL PRIMARY KEY,
        event_id TEXT,
        reason TEXT NOT NULL,
        details JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await this._client.query(`
      CREATE TABLE IF NOT EXISTS ${s}.nonce_log (
        nonce TEXT PRIMARY KEY,
        seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL
      )
    `);
  }

  async assertFreshNonce(nonce) {
    if (!this._ready) await this.connect();
    const s = quoteIdentifier(this.schema);
    await this._client.query(`DELETE FROM ${s}.nonce_log WHERE expires_at <= NOW()`);
    const expiry = new Date(Date.now() + this.nonceTtlSeconds * 1000).toISOString();
    try {
      await this._client.query(
        `INSERT INTO ${s}.nonce_log (nonce, expires_at) VALUES ($1, $2::timestamptz)`,
        [nonce, expiry]
      );
      return true;
    } catch (err) {
      if (err?.code === "23505") {
        return false;
      }
      throw err;
    }
  }

  async processEvent(event) {
    if (!this._ready) await this.connect();
    const validation = validateSyncEvent(event);
    if (!validation.valid) {
      await this.recordError(event?.event_id || null, validation.reason, { event });
      return { accepted: false, reason: validation.reason };
    }

    const s = quoteIdentifier(this.schema);
    await this._client.query("BEGIN");
    try {
      const ins = await this._client.query(
        `
          INSERT INTO ${s}.raw_events
            (event_id, event_type, occurred_at, source_entity, source_id, idempotency_key, payload)
          VALUES ($1, $2, $3::timestamptz, $4, $5, $6, $7::jsonb)
          ON CONFLICT (event_id) DO NOTHING
          RETURNING event_id
        `,
        [
          event.event_id,
          event.event_type,
          toIsoOrNull(event.occurred_at) || nowIso(),
          event.source_entity,
          `${event.source_id}`,
          event.idempotency_key,
          JSON.stringify(event.payload)
        ]
      );

      if (ins.rowCount === 0) {
        await this._client.query("ROLLBACK");
        return {
          accepted: true,
          duplicate: true,
          processed_at: nowIso(),
          projection_version: this.projectionVersion
        };
      }

      await this._upsertProjection(event);
      await this._updateWatermark(event);
      await this._client.query("COMMIT");

      return {
        accepted: true,
        processed_at: nowIso(),
        projection_version: this.projectionVersion
      };
    } catch (error) {
      await this._client.query("ROLLBACK");
      await this.recordError(event.event_id, "projection_failure", {
        message: error?.message || `${error}`,
        event_type: event.event_type
      });
      throw error;
    }
  }

  async _ensureMap(entityType, vikbookingId) {
    const s = quoteIdentifier(this.schema);
    await this._client.query(
      `
        INSERT INTO ${s}.id_map (entity_type, vikbooking_id)
        VALUES ($1, $2)
        ON CONFLICT (entity_type, vikbooking_id)
        DO UPDATE SET last_seen_at = NOW()
      `,
      [entityType, `${vikbookingId}`]
    );
    const { rows } = await this._client.query(
      `SELECT supabase_uuid FROM ${s}.id_map WHERE entity_type = $1 AND vikbooking_id = $2`,
      [entityType, `${vikbookingId}`]
    );
    return rows[0]?.supabase_uuid;
  }

  async _upsertProjection(event) {
    const reservation = event.payload?.reservation || null;
    const customer = event.payload?.customer || null;
    const rooms = Array.isArray(event.payload?.rooms) ? event.payload.rooms : [];
    const payment = event.payload?.payment || null;
    const s = quoteIdentifier(this.schema);

    let reservationUuid = null;
    if (reservation?.vikbooking_id || event.source_entity === "reservation") {
      const reservationId = `${reservation?.vikbooking_id || event.source_id}`;
      reservationUuid = await this._ensureMap("reservation", reservationId);
      await this._client.query(
        `
          INSERT INTO ${s}.reservations
            (supabase_uuid, vikbooking_id, status, total, total_paid, customer_vikbooking_id, checkin_at, checkout_at, source_updated_at, projection_version, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz, $8::timestamptz, $9::timestamptz, $10, NOW())
          ON CONFLICT (supabase_uuid)
          DO UPDATE SET
            status = COALESCE(EXCLUDED.status, ${s}.reservations.status),
            total = COALESCE(EXCLUDED.total, ${s}.reservations.total),
            total_paid = COALESCE(EXCLUDED.total_paid, ${s}.reservations.total_paid),
            customer_vikbooking_id = COALESCE(EXCLUDED.customer_vikbooking_id, ${s}.reservations.customer_vikbooking_id),
            checkin_at = COALESCE(EXCLUDED.checkin_at, ${s}.reservations.checkin_at),
            checkout_at = COALESCE(EXCLUDED.checkout_at, ${s}.reservations.checkout_at),
            source_updated_at = COALESCE(EXCLUDED.source_updated_at, ${s}.reservations.source_updated_at),
            projection_version = EXCLUDED.projection_version,
            updated_at = NOW()
        `,
        [
          reservationUuid,
          reservationId,
          reservation?.status || null,
          reservation?.total ?? null,
          reservation?.total_paid ?? null,
          reservation?.customer_vikbooking_id ? `${reservation.customer_vikbooking_id}` : null,
          toIsoOrNull(reservation?.checkin_at),
          toIsoOrNull(reservation?.checkout_at),
          toIsoOrNull(reservation?.updated_at || event.occurred_at),
          this.projectionVersion
        ]
      );
    }

    if (customer?.vikbooking_id) {
      const customerUuid = await this._ensureMap("customer", customer.vikbooking_id);
      await this._client.query(
        `
          INSERT INTO ${s}.customers
            (supabase_uuid, vikbooking_id, email, first_name, last_name, phone, source_updated_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz, NOW())
          ON CONFLICT (supabase_uuid)
          DO UPDATE SET
            email = COALESCE(EXCLUDED.email, ${s}.customers.email),
            first_name = COALESCE(EXCLUDED.first_name, ${s}.customers.first_name),
            last_name = COALESCE(EXCLUDED.last_name, ${s}.customers.last_name),
            phone = COALESCE(EXCLUDED.phone, ${s}.customers.phone),
            source_updated_at = COALESCE(EXCLUDED.source_updated_at, ${s}.customers.source_updated_at),
            updated_at = NOW()
        `,
        [
          customerUuid,
          `${customer.vikbooking_id}`,
          customer.email || null,
          customer.first_name || null,
          customer.last_name || null,
          customer.phone || null,
          toIsoOrNull(customer.updated_at || event.occurred_at)
        ]
      );
    }

    if (reservationUuid && rooms.length > 0) {
      for (const room of rooms) {
        const linkId = `${room.vikbooking_room_link_id || room.id || room.roomindex || room.vikbooking_room_id || rowHash(room)}`;
        await this._client.query(
          `
            INSERT INTO ${s}.reservation_rooms
              (reservation_supabase_uuid, vikbooking_room_link_id, vikbooking_room_id, adults, children, room_cost, source_updated_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz, NOW())
            ON CONFLICT (reservation_supabase_uuid, vikbooking_room_link_id)
            DO UPDATE SET
              vikbooking_room_id = COALESCE(EXCLUDED.vikbooking_room_id, ${s}.reservation_rooms.vikbooking_room_id),
              adults = COALESCE(EXCLUDED.adults, ${s}.reservation_rooms.adults),
              children = COALESCE(EXCLUDED.children, ${s}.reservation_rooms.children),
              room_cost = COALESCE(EXCLUDED.room_cost, ${s}.reservation_rooms.room_cost),
              source_updated_at = COALESCE(EXCLUDED.source_updated_at, ${s}.reservation_rooms.source_updated_at),
              updated_at = NOW()
          `,
          [
            reservationUuid,
            linkId,
            room.vikbooking_room_id ? `${room.vikbooking_room_id}` : null,
            room.adults ?? null,
            room.children ?? null,
            room.room_cost ?? null,
            toIsoOrNull(room.updated_at || event.occurred_at)
          ]
        );
      }
    }

    if (reservationUuid && payment) {
      const paymentKey = `${event.source_entity}:${event.source_id}:payment:${payment.id || payment.reference || "default"}`;
      await this._client.query(
        `
          INSERT INTO ${s}.payments
            (payment_key, reservation_supabase_uuid, payment_status, payment_amount, currency, source_updated_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6::timestamptz, NOW())
          ON CONFLICT (payment_key)
          DO UPDATE SET
            payment_status = COALESCE(EXCLUDED.payment_status, ${s}.payments.payment_status),
            payment_amount = COALESCE(EXCLUDED.payment_amount, ${s}.payments.payment_amount),
            currency = COALESCE(EXCLUDED.currency, ${s}.payments.currency),
            source_updated_at = COALESCE(EXCLUDED.source_updated_at, ${s}.payments.source_updated_at),
            updated_at = NOW()
        `,
        [
          paymentKey,
          reservationUuid,
          payment.status || null,
          payment.amount ?? null,
          payment.currency || null,
          toIsoOrNull(payment.updated_at || event.occurred_at)
        ]
      );
    }
  }

  async _updateWatermark(event) {
    const s = quoteIdentifier(this.schema);
    const streamKey = `entity:${event.source_entity}`;
    const watermark = `${event.payload?.watermark || event.source_id || event.event_id}`;
    await this._client.query(
      `
        INSERT INTO ${s}.sync_state (stream_key, watermark, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (stream_key)
        DO UPDATE SET watermark = EXCLUDED.watermark, updated_at = NOW()
      `,
      [streamKey, watermark]
    );
  }

  async recordError(eventId, reason, details = null) {
    if (!this._client) return;
    const s = quoteIdentifier(this.schema);
    await this._client.query(
      `
        INSERT INTO ${s}.sync_errors (event_id, reason, details)
        VALUES ($1, $2, $3::jsonb)
      `,
      [eventId, reason, JSON.stringify(details || {})]
    );
  }
}

function rowHash(input) {
  return crypto.createHash("sha1").update(JSON.stringify(input || {})).digest("hex").slice(0, 16);
}
