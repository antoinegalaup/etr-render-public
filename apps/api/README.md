# API App

Logical API surface for the ETR platform.

Current implementation:

- runtime entry: `../../src/server.js`
- primary service logic: `../../src/lib/controlPlaneService.js`

Start locally from the repo root:

```bash
npm run api:start
```

This directory exists to establish the long-term `apps/api` boundary before the root server code is fully extracted.
