import crypto from "node:crypto";
import mysql from "mysql2/promise";
import { Client } from "pg";
import { buildPgConnectionOptions } from "./pgConnection.js";

const DEFAULT_BASE_TABLES = [
  "vikbooking_orders",
  "vikbooking_ordersrooms",
  "vikbooking_customers",
  "vikbooking_rooms",
  "vikbooking_gpayments",
  "vikbooking_orderhistory"
];

const UPDATED_AT_CANDIDATES = [
  "updated_at",
  "modified",
  "last_update",
  "last_exec",
  "dt",
  "ts",
  "createdon",
  "created_at"
];

const NUMERIC_TYPES = new Set([
  "tinyint",
  "smallint",
  "mediumint",
  "int",
  "integer",
  "bigint",
  "decimal",
  "numeric"
]);

function firstNonEmpty(...values) {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const text = `${value}`.trim();
    if (text.length > 0) return text;
  }
  return "";
}

function validateIdentifier(value, label) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Invalid ${label}: ${value}`);
  }
  return value;
}

function quotePgIdentifier(value) {
  return `"${validateIdentifier(value, "Postgres identifier").replace(/"/g, "\"\"")}"`;
}

function quoteMysqlIdentifier(value) {
  return `\`${validateIdentifier(value, "MySQL identifier").replace(/`/g, "``")}\``;
}

function toPositiveInt(value, fallback) {
  const n = Number.parseInt(value, 10);
  if (Number.isNaN(n) || n <= 0) return fallback;
  return n;
}

function parseBoolean(value) {
  if (value === undefined || value === null) return false;
  const text = `${value}`.trim().toLowerCase();
  return ["1", "true", "yes", "y", "on"].includes(text);
}

function splitList(value) {
  return `${value || ""}`
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export function sanitizeJsonText(value) {
  return `${value}`
    .replace(/\u0000/g, "")
    .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/g, "\uFFFD")
    .replace(/(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "\uFFFD");
}

export function normalizeSourceTables(rawTables, prefix = "wp_") {
  const input = `${rawTables || ""}`.trim();
  const base = input
    ? splitList(input)
    : DEFAULT_BASE_TABLES;

  return Array.from(
    new Set(
      base.map((table) => {
        if (table.startsWith(prefix)) return table;
        return `${prefix}${table}`;
      })
    )
  ).map((table) => validateIdentifier(table, "source table"));
}

function wildcardToRegex(pattern) {
  const escaped = pattern.replace(/[|\\{}()[\]^$+?.]/g, "\\$&");
  const withWildcards = escaped.replace(/\*/g, ".*").replace(/\\\?/g, ".");
  return new RegExp(`^${withWildcards}$`);
}

function parseMatcherToken(token) {
  const trimmed = `${token || ""}`.trim();
  if (!trimmed) return null;
  if (trimmed.startsWith("/") && trimmed.lastIndexOf("/") > 0) {
    const lastSlash = trimmed.lastIndexOf("/");
    const body = trimmed.slice(1, lastSlash);
    const flags = trimmed.slice(lastSlash + 1);
    return new RegExp(body, flags);
  }
  return wildcardToRegex(trimmed);
}

export function compileTableMatcher(rawPattern) {
  const tokens = splitList(rawPattern);
  if (!tokens.length) return null;
  const regexes = tokens.map(parseMatcherToken).filter(Boolean);
  if (!regexes.length) return null;
  return (value) => regexes.some((regex) => regex.test(value));
}

export function applyTableFilters(tables, includePattern, excludePattern) {
  const include = compileTableMatcher(includePattern);
  const exclude = compileTableMatcher(excludePattern);
  return tables.filter((table) => {
    if (include && !include(table)) return false;
    if (exclude && exclude(table)) return false;
    return true;
  });
}

export async function discoverSourceTables(mysqlConn, database, prefix = "wp_") {
  const likePattern = `${prefix}%`;
  const sql = `
    SELECT TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = ?
      AND TABLE_TYPE = 'BASE TABLE'
      AND TABLE_NAME LIKE ?
    ORDER BY TABLE_NAME
  `;
  const [rows] = await mysqlConn.query(sql, [database, likePattern]);
  return rows
    .map((row) => `${row.TABLE_NAME || ""}`.trim())
    .filter(Boolean)
    .map((table) => validateIdentifier(table, "source table"));
}

export function rowDigest(row) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(row, jsonReplacer))
    .digest("hex")
    .slice(0, 24);
}

export function buildSourcePk(row, pkColumns = []) {
  if (pkColumns.length > 0) {
    return sanitizeJsonText(pkColumns.map((column) => `${row[column] ?? ""}`).join("::"));
  }
  if (row.id !== undefined && row.id !== null) return sanitizeJsonText(`${row.id}`);
  if (row.ID !== undefined && row.ID !== null) return sanitizeJsonText(`${row.ID}`);
  return rowDigest(row);
}

export function coerceUpdatedAt(row) {
  const keys = Object.keys(row).reduce((acc, key) => {
    acc[key.toLowerCase()] = key;
    return acc;
  }, {});

  for (const candidate of UPDATED_AT_CANDIDATES) {
    const key = keys[candidate];
    if (!key) continue;
    const value = row[key];
    if (value === null || value === undefined || value === "") continue;
    if (value instanceof Date) return value;
    if (typeof value === "number") {
      if (value > 9_999_999_999) return new Date(value);
      if (value > 0) return new Date(value * 1000);
    }
    if (typeof value === "string") {
      const maybeNum = Number.parseInt(value, 10);
      if (!Number.isNaN(maybeNum) && `${maybeNum}` === value) {
        if (maybeNum > 9_999_999_999) return new Date(maybeNum);
        if (maybeNum > 0) return new Date(maybeNum * 1000);
      }
      const parsed = new Date(value);
      if (!Number.isNaN(parsed.getTime())) return parsed;
    }
  }

  return null;
}

function jsonReplacer(_key, value) {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (Buffer.isBuffer(value)) return `base64:${value.toString("base64")}`;
  if (typeof value === "string") return sanitizeJsonText(value);
  return value;
}

async function readState(pgClient, schema, table, stateKey) {
  const sql = `SELECT state_value FROM ${quotePgIdentifier(schema)}.${quotePgIdentifier(table)} WHERE state_key = $1`;
  const { rows } = await pgClient.query(sql, [stateKey]);
  return rows[0]?.state_value ?? null;
}

async function writeState(pgClient, schema, table, stateKey, stateValue) {
  const sql = `
    INSERT INTO ${quotePgIdentifier(schema)}.${quotePgIdentifier(table)} (state_key, state_value, updated_at)
    VALUES ($1, $2, NOW())
    ON CONFLICT (state_key)
    DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = NOW()
  `;
  await pgClient.query(sql, [stateKey, stateValue]);
}

async function ensureTargetTables(pgClient, schema, rawTable, stateTable) {
  const rawIdxName = validateIdentifier(`idx_${rawTable}_source_updated`, "index name");
  const stateIdxName = validateIdentifier(`idx_${stateTable}_updated`, "index name");
  await pgClient.query(`CREATE SCHEMA IF NOT EXISTS ${quotePgIdentifier(schema)}`);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS ${quotePgIdentifier(schema)}.${quotePgIdentifier(rawTable)} (
      source_table TEXT NOT NULL,
      source_pk TEXT NOT NULL,
      row_json JSONB NOT NULL,
      source_updated_at TIMESTAMPTZ NULL,
      synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (source_table, source_pk)
    )
  `);
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS ${quotePgIdentifier(schema)}.${quotePgIdentifier(stateTable)} (
      state_key TEXT PRIMARY KEY,
      state_value TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pgClient.query(`
    CREATE INDEX IF NOT EXISTS ${quotePgIdentifier(rawIdxName)}
    ON ${quotePgIdentifier(schema)}.${quotePgIdentifier(rawTable)} (source_table, source_updated_at DESC)
  `);
  await pgClient.query(`
    CREATE INDEX IF NOT EXISTS ${quotePgIdentifier(stateIdxName)}
    ON ${quotePgIdentifier(schema)}.${quotePgIdentifier(stateTable)} (updated_at DESC)
  `);
}

async function fetchPrimaryKeyColumns(mysqlConn, database, table) {
  const sql = `
    SELECT k.COLUMN_NAME
    FROM information_schema.KEY_COLUMN_USAGE k
    WHERE k.TABLE_SCHEMA = ?
      AND k.TABLE_NAME = ?
      AND k.CONSTRAINT_NAME = 'PRIMARY'
    ORDER BY k.ORDINAL_POSITION
  `;
  const [rows] = await mysqlConn.query(sql, [database, table]);
  return rows.map((row) => row.COLUMN_NAME).filter(Boolean);
}

async function fetchColumnDataType(mysqlConn, database, table, column) {
  const sql = `
    SELECT DATA_TYPE
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND COLUMN_NAME = ?
    LIMIT 1
  `;
  const [rows] = await mysqlConn.query(sql, [database, table, column]);
  return rows[0]?.DATA_TYPE ? `${rows[0].DATA_TYPE}`.toLowerCase() : null;
}

function isNumericType(dataType) {
  if (!dataType) return false;
  return NUMERIC_TYPES.has(dataType);
}

async function upsertRows(pgClient, schema, rawTable, sourceTable, rows, pkColumns) {
  const sql = `
    INSERT INTO ${quotePgIdentifier(schema)}.${quotePgIdentifier(rawTable)}
      (source_table, source_pk, row_json, source_updated_at, synced_at)
    VALUES ($1, $2, $3::jsonb, $4::timestamptz, NOW())
    ON CONFLICT (source_table, source_pk)
    DO UPDATE SET
      row_json = EXCLUDED.row_json,
      source_updated_at = EXCLUDED.source_updated_at,
      synced_at = NOW()
  `;

  for (const row of rows) {
    const sourcePk = buildSourcePk(row, pkColumns);
    const sourceUpdatedAt = coerceUpdatedAt(row);
    await pgClient.query(sql, [
      sourceTable,
      sourcePk,
      JSON.stringify(row, jsonReplacer),
      sourceUpdatedAt ? sourceUpdatedAt.toISOString() : null
    ]);
  }
}

export async function runDbTransfer(opts) {
  const sourceAll = Boolean(opts.sourceAll) || parseBoolean(process.env.DB_TRANSFER_SOURCE_ALL);
  const sourceConfig = {
    host: opts.sourceHost || process.env.DB_TRANSFER_SOURCE_HOST || process.env.MYSQL_HOST,
    port: toPositiveInt(
      opts.sourcePort || process.env.DB_TRANSFER_SOURCE_PORT || process.env.MYSQL_PORT,
      3306
    ),
    user: opts.sourceUser || process.env.DB_TRANSFER_SOURCE_USER || process.env.MYSQL_USER,
    password:
      opts.sourcePassword || process.env.DB_TRANSFER_SOURCE_PASSWORD || process.env.MYSQL_PASSWORD,
    database:
      opts.sourceDatabase || process.env.DB_TRANSFER_SOURCE_DATABASE || process.env.MYSQL_DATABASE,
    prefix:
      opts.sourcePrefix || process.env.DB_TRANSFER_SOURCE_PREFIX || process.env.WP_DB_PREFIX || "wp_",
    includePattern: firstNonEmpty(opts.sourceInclude, process.env.DB_TRANSFER_SOURCE_INCLUDE),
    excludePattern: firstNonEmpty(opts.sourceExclude, process.env.DB_TRANSFER_SOURCE_EXCLUDE),
    batchSize: toPositiveInt(
      opts.batchSize || process.env.DB_TRANSFER_BATCH_SIZE,
      500
    )
  };

  const envTargetRawTable = firstNonEmpty(process.env.DB_TRANSFER_TARGET_RAW_TABLE);
  const envTargetStateTable = firstNonEmpty(process.env.DB_TRANSFER_TARGET_STATE_TABLE);
  const targetConfig = {
    url:
      opts.targetUrl ||
      process.env.DB_TRANSFER_TARGET_URL ||
      process.env.SUPABASE_DB_URL ||
      process.env.DATABASE_URL,
    schema: opts.targetSchema || process.env.DB_TRANSFER_TARGET_SCHEMA || "public",
    rawTable:
      firstNonEmpty(opts.targetRawTable) ||
      envTargetRawTable ||
      (sourceAll ? "wp_raw_rows" : "vb_raw_rows"),
    stateTable:
      firstNonEmpty(opts.targetStateTable) ||
      envTargetStateTable ||
      (sourceAll ? "wp_sync_state" : "vb_sync_state")
  };

  const full = Boolean(opts.full);

  if (!sourceConfig.host || !sourceConfig.user || !sourceConfig.database) {
    throw new Error("Missing source MySQL config. Set --source-* flags or DB_TRANSFER_SOURCE_* env vars.");
  }
  if (!targetConfig.url) {
    throw new Error("Missing Postgres target URL. Set --target-url or DB_TRANSFER_TARGET_URL/DATABASE_URL.");
  }

  validateIdentifier(targetConfig.schema, "target schema");
  validateIdentifier(targetConfig.rawTable, "target raw table");
  validateIdentifier(targetConfig.stateTable, "target state table");

  const mysqlConn = await mysql.createConnection({
    host: sourceConfig.host,
    port: sourceConfig.port,
    user: sourceConfig.user,
    password: sourceConfig.password,
    database: sourceConfig.database
  });
  const pgClient = new Client(buildPgConnectionOptions(targetConfig.url));

  const summary = {
    mode: full ? "full" : "incremental",
    source_tables: [],
    transferred_rows: 0,
    per_table: {}
  };

  try {
    await pgClient.connect();
    await ensureTargetTables(
      pgClient,
      targetConfig.schema,
      targetConfig.rawTable,
      targetConfig.stateTable
    );

    let sourceTables;
    if (firstNonEmpty(opts.sourceTables).length > 0) {
      sourceTables = normalizeSourceTables(opts.sourceTables, sourceConfig.prefix);
    } else if (sourceAll) {
      sourceTables = await discoverSourceTables(
        mysqlConn,
        sourceConfig.database,
        sourceConfig.prefix
      );
    } else {
      sourceTables = normalizeSourceTables("", sourceConfig.prefix);
    }
    sourceTables = applyTableFilters(
      sourceTables,
      sourceConfig.includePattern,
      sourceConfig.excludePattern
    );
    if (!sourceTables.length) {
      throw new Error("No source tables resolved. Check --source-* filters and prefix.");
    }
    summary.source_tables = sourceTables;

    for (const sourceTable of sourceTables) {
      const pkColumns = await fetchPrimaryKeyColumns(
        mysqlConn,
        sourceConfig.database,
        sourceTable
      );
      const singlePk = pkColumns.length === 1 ? pkColumns[0] : null;
      const singlePkType = singlePk
        ? await fetchColumnDataType(mysqlConn, sourceConfig.database, sourceTable, singlePk)
        : null;
      const incrementalByPk = !full && singlePk && isNumericType(singlePkType);
      const stateKey = `table:${sourceTable}:last_pk`;
      let lastPk = incrementalByPk
        ? Number.parseInt((await readState(pgClient, targetConfig.schema, targetConfig.stateTable, stateKey)) || "0", 10) || 0
        : 0;
      let offset = 0;
      let tableRows = 0;

      while (true) {
        let sql;
        let params;
        if (incrementalByPk) {
          sql = `SELECT * FROM ${quoteMysqlIdentifier(sourceTable)} WHERE ${quoteMysqlIdentifier(singlePk)} > ? ORDER BY ${quoteMysqlIdentifier(singlePk)} ASC LIMIT ?`;
          params = [lastPk, sourceConfig.batchSize];
        } else if (pkColumns.length > 0) {
          sql = `SELECT * FROM ${quoteMysqlIdentifier(sourceTable)} ORDER BY ${pkColumns
            .map((col) => quoteMysqlIdentifier(col))
            .join(", ")} LIMIT ? OFFSET ?`;
          params = [sourceConfig.batchSize, offset];
        } else {
          sql = `SELECT * FROM ${quoteMysqlIdentifier(sourceTable)} LIMIT ? OFFSET ?`;
          params = [sourceConfig.batchSize, offset];
        }

        const [rows] = await mysqlConn.query(sql, params);
        if (!rows.length) break;

        await pgClient.query("BEGIN");
        try {
          await upsertRows(
            pgClient,
            targetConfig.schema,
            targetConfig.rawTable,
            sourceTable,
            rows,
            pkColumns
          );

          if (incrementalByPk) {
            lastPk = Number(rows[rows.length - 1][singlePk]) || lastPk;
            await writeState(
              pgClient,
              targetConfig.schema,
              targetConfig.stateTable,
              stateKey,
              `${lastPk}`
            );
          }
          await pgClient.query("COMMIT");
        } catch (error) {
          await pgClient.query("ROLLBACK");
          throw error;
        }

        tableRows += rows.length;
        summary.transferred_rows += rows.length;
        if (!incrementalByPk) {
          offset += rows.length;
        }
        if (rows.length < sourceConfig.batchSize) break;
      }

      summary.per_table[sourceTable] = {
        rows: tableRows,
        mode: incrementalByPk ? "incremental_by_primary_key" : "full_scan"
      };
      console.log(`${sourceTable}: ${tableRows} row(s) transferred.`);
    }

    console.log(`Transfer complete. ${summary.transferred_rows} row(s) upserted.`);
    return summary;
  } finally {
    await mysqlConn.end();
    await pgClient.end();
  }
}
