# WALrus

WALrus ("Write-Ahead Log in object storage") is a horizontally scalable,
multi-tenant SQLite service with one logical database per user. Object
storage (Cloudflare R2 or any S3-compatible store with conditional writes)
is the durable database; workers are disposable compute.