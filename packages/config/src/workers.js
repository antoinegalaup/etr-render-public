import {
  readBoolean,
  readEnum,
  readPositiveInt,
  readString,
  redactConnectionString
} from "./env.js";

const WORKER_MODES = ["once", "service"];

export function readWorkerConfig(env = process.env) {
  const mode = readEnum(env.WORKER_MODE, WORKER_MODES, "service");
  const vikbookingSyncEnabled = readBoolean(env.WORKER_VIKBOOKING_SYNC_ENABLED, false);
  const vikbookingSyncIntervalMs = readPositiveInt(
    env.WORKER_VIKBOOKING_SYNC_INTERVAL_MS,
    5 * 60 * 1000
  );
  const commandQueueEnabled = readBoolean(env.WORKER_COMMAND_QUEUE_ENABLED, false);
  const commandQueueIntervalMs = readPositiveInt(
    env.WORKER_COMMAND_QUEUE_INTERVAL_MS,
    15 * 1000
  );
  const reconciliationEnabled = readBoolean(env.WORKER_RECONCILIATION_ENABLED, false);
  const reconciliationIntervalMs = readPositiveInt(
    env.WORKER_RECONCILIATION_INTERVAL_MS,
    60 * 1000
  );
  const databaseUrl = readString(
    env.WORKER_DATABASE_URL ||
      env.SYNC_DATABASE_URL ||
      env.DB_TRANSFER_TARGET_URL ||
      env.SUPABASE_DB_URL ||
      env.DATABASE_URL
  );

  return {
    mode,
    databaseUrl,
    jobs: {
      vikbookingSync: {
        enabled: vikbookingSyncEnabled,
        intervalMs: vikbookingSyncIntervalMs,
        dbTransfer: {
          sourceHost: readString(env.DB_TRANSFER_SOURCE_HOST || env.MYSQL_HOST),
          sourcePort: readPositiveInt(
            env.DB_TRANSFER_SOURCE_PORT || env.MYSQL_PORT,
            3306
          ),
          sourceUser: readString(env.DB_TRANSFER_SOURCE_USER || env.MYSQL_USER),
          sourcePassword: readString(
            env.DB_TRANSFER_SOURCE_PASSWORD || env.MYSQL_PASSWORD
          ),
          sourceDatabase: readString(
            env.DB_TRANSFER_SOURCE_DATABASE || env.MYSQL_DATABASE
          ),
          sourcePrefix: readString(
            env.DB_TRANSFER_SOURCE_PREFIX || env.WP_DB_PREFIX,
            "wp_"
          ),
          sourceAll: readBoolean(env.DB_TRANSFER_SOURCE_ALL, false),
          sourceTables: readString(env.DB_TRANSFER_SOURCE_TABLES),
          sourceInclude: readString(env.DB_TRANSFER_SOURCE_INCLUDE),
          sourceExclude: readString(env.DB_TRANSFER_SOURCE_EXCLUDE),
          targetUrl: readString(
            env.DB_TRANSFER_TARGET_URL || env.SUPABASE_DB_URL || env.DATABASE_URL
          ),
          targetSchema: readString(env.DB_TRANSFER_TARGET_SCHEMA, "public"),
          targetRawTable: readString(env.DB_TRANSFER_TARGET_RAW_TABLE),
          targetStateTable: readString(env.DB_TRANSFER_TARGET_STATE_TABLE),
          batchSize: readPositiveInt(env.DB_TRANSFER_BATCH_SIZE, 500),
          full: readBoolean(env.DB_TRANSFER_FULL, false)
        }
      },
      commandQueue: {
        enabled: commandQueueEnabled,
        intervalMs: commandQueueIntervalMs
      },
      reconciliation: {
        enabled: reconciliationEnabled,
        intervalMs: reconciliationIntervalMs
      }
    }
  };
}

export function summarizeWorkerConfig(config) {
  const job = config?.jobs?.vikbookingSync || {};
  const commandQueue = config?.jobs?.commandQueue || {};
  const reconciliation = config?.jobs?.reconciliation || {};
  const transfer = job.dbTransfer || {};

  return {
    mode: config?.mode || "service",
    databaseUrl: redactConnectionString(config?.databaseUrl || ""),
    jobs: {
      vikbookingSync: {
        enabled: Boolean(job.enabled),
        intervalMs: Number(job.intervalMs || 0),
        sourceHost: readString(transfer.sourceHost),
        sourcePort: Number(transfer.sourcePort || 0),
        sourceDatabase: readString(transfer.sourceDatabase),
        sourcePrefix: readString(transfer.sourcePrefix, "wp_"),
        sourceAll: Boolean(transfer.sourceAll),
        targetSchema: readString(transfer.targetSchema, "public"),
        targetRawTable: readString(transfer.targetRawTable),
        targetStateTable: readString(transfer.targetStateTable),
        targetUrl: redactConnectionString(transfer.targetUrl),
        full: Boolean(transfer.full)
      },
      commandQueue: {
        enabled: Boolean(commandQueue.enabled),
        intervalMs: Number(commandQueue.intervalMs || 0)
      },
      reconciliation: {
        enabled: Boolean(reconciliation.enabled),
        intervalMs: Number(reconciliation.intervalMs || 0)
      }
    }
  };
}
