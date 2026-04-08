import process from "node:process";
import { setTimeout as sleep } from "node:timers/promises";
import { runDbTransfer } from "../../../src/lib/dbTransfer.js";
import { ControlPlaneService } from "../../../src/lib/controlPlaneService.js";
import { StaffOperationsService } from "../../../src/lib/staffOperationsService.js";
import { readWorkerConfig, summarizeWorkerConfig } from "../../../packages/config/src/workers.js";

let shuttingDown = false;

function installSignalHandlers() {
  const handleSignal = (signal) => {
    shuttingDown = true;
    console.log(`[workers] received ${signal}, finishing current cycle before exit.`);
  };

  process.on("SIGINT", handleSignal);
  process.on("SIGTERM", handleSignal);
}

async function runVikbookingSyncJob(jobConfig) {
  const startedAt = new Date().toISOString();
  console.log(
    `[workers] starting vikbooking sync at ${startedAt}: ${JSON.stringify(
      summarizeWorkerConfig({ jobs: { vikbookingSync: jobConfig } }).jobs.vikbookingSync
    )}`
  );
  const summary = await runDbTransfer(jobConfig.dbTransfer);
  const finishedAt = new Date().toISOString();
  console.log(
    `[workers] completed vikbooking sync at ${finishedAt}: ${JSON.stringify({
      transferredRows: summary.transferred_rows,
      sourceTables: summary.source_tables.length
    })}`
  );
}

async function runCommandQueueJob(staffService) {
  const startedAt = new Date().toISOString();
  console.log(`[workers] starting command queue drain at ${startedAt}.`);
  const results = await staffService.processCommandQueue({ limit: 10 });
  const succeeded = results.filter((entry) => entry.status === "succeeded").length;
  const failed = results.filter((entry) => entry.status === "failed").length;
  console.log(
    `[workers] completed command queue drain at ${new Date().toISOString()}: ${JSON.stringify({
      processed: results.length,
      succeeded,
      failed
    })}`
  );
}

async function runReconciliationJob(staffService) {
  const startedAt = new Date().toISOString();
  console.log(`[workers] starting reconciliation at ${startedAt}.`);
  const result = await staffService.runReconciliation();
  console.log(
    `[workers] completed reconciliation at ${new Date().toISOString()}: ${JSON.stringify({
      cursor: result?.cursor || null,
      type: result?.type || null
    })}`
  );
}

function buildJobRunners(config, staffService) {
  const runners = [];
  if (config.jobs.vikbookingSync.enabled) {
    runners.push({
      name: "vikbookingSync",
      intervalMs: config.jobs.vikbookingSync.intervalMs,
      run: () => runVikbookingSyncJob(config.jobs.vikbookingSync)
    });
  }
  if (config.jobs.commandQueue.enabled) {
    if (!staffService) {
      throw new Error("Command queue worker requires WORKER_DATABASE_URL or SYNC_DATABASE_URL.");
    }
    runners.push({
      name: "commandQueue",
      intervalMs: config.jobs.commandQueue.intervalMs,
      run: () => runCommandQueueJob(staffService)
    });
  }
  if (config.jobs.reconciliation.enabled) {
    if (!staffService) {
      throw new Error("Reconciliation worker requires WORKER_DATABASE_URL or SYNC_DATABASE_URL.");
    }
    runners.push({
      name: "reconciliation",
      intervalMs: config.jobs.reconciliation.intervalMs,
      run: () => runReconciliationJob(staffService)
    });
  }
  return runners;
}

async function createStaffOperationsService(config) {
  if (!config.databaseUrl) {
    return null;
  }
  const controlPlaneService = new ControlPlaneService({
    databaseUrl: config.databaseUrl,
    schema: process.env.SYNC_SCHEMA || "sync",
    wordpressBaseUrl: process.env.WORDPRESS_BASE_URL || "",
    wordpressPublicBaseUrl:
      process.env.WORDPRESS_PUBLIC_BASE_URL ||
      process.env.NEXT_PUBLIC_WORDPRESS_ASSET_BASE_URL ||
      process.env.WORDPRESS_BASE_URL ||
      "",
    wordpressCommandEndpoint:
      process.env.WORDPRESS_COMMAND_ENDPOINT || "/wp-json/wpshell-sync/v1/commands",
    wordpressCommandSecret:
      process.env.WORDPRESS_COMMAND_SECRET || process.env.SYNC_HMAC_SECRET || ""
  });
  return {
    controlPlaneService,
    staffService: new StaffOperationsService({
      controlPlaneService,
      syncSchema: process.env.SYNC_SCHEMA || "sync",
      opsSchema: "ops"
    })
  };
}

async function runOnce(runners) {
  if (!runners.length) {
    console.log("[workers] no jobs enabled, nothing to run.");
    return;
  }
  for (const runner of runners) {
    await runner.run();
  }
}

async function runService(runners) {
  if (!runners.length) {
    throw new Error("No worker jobs are enabled. Enable at least one WORKER_*_ENABLED flag.");
  }

  const nextRunAt = new Map(runners.map((runner) => [runner.name, 0]));
  while (!shuttingDown) {
    let ranJob = false;
    const now = Date.now();

    for (const runner of runners) {
      const nextAt = nextRunAt.get(runner.name) || 0;
      if (nextAt > now) {
        continue;
      }

      try {
        await runner.run();
      } catch (error) {
        console.error(`[workers] ${runner.name} failed: ${error?.message || String(error)}`);
      }
      nextRunAt.set(runner.name, Date.now() + runner.intervalMs);
      ranJob = true;

      if (shuttingDown) {
        break;
      }
    }

    if (shuttingDown) {
      break;
    }

    if (!ranJob) {
      const nextWakeMs = Math.max(
        500,
        Math.min(...Array.from(nextRunAt.values()).map((value) => Math.max(0, value - Date.now())))
      );
      await sleep(nextWakeMs);
    }
  }

  console.log("[workers] service stopped.");
}

async function main() {
  installSignalHandlers();
  const config = readWorkerConfig(process.env);
  console.log(`[workers] boot config: ${JSON.stringify(summarizeWorkerConfig(config), null, 2)}`);
  const staffServices = await createStaffOperationsService(config);
  const runners = buildJobRunners(config, staffServices?.staffService || null);

  try {
    if (config.mode === "once") {
      await runOnce(runners);
      return;
    }

    await runService(runners);
  } finally {
    if (staffServices?.controlPlaneService?.close) {
      await staffServices.controlPlaneService.close().catch(() => {});
    }
  }
}

main().catch((error) => {
  console.error(`[workers] fatal error: ${error?.message || String(error)}`);
  process.exitCode = 1;
});
