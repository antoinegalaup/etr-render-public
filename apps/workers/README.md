# Workers App

Background job surface for the ETR platform.

The first worker job wired here is the VikBooking to Supabase transfer loop backed by `src/lib/dbTransfer.js`.

Local commands from the repo root:

```bash
npm run workers:once
npm run workers:start
```

Primary environment flags:

1. `WORKER_MODE=once|service`
2. `WORKER_VIKBOOKING_SYNC_ENABLED=true|false`
3. `WORKER_VIKBOOKING_SYNC_INTERVAL_MS=300000`
4. `DB_TRANSFER_*` for source and target database connectivity
