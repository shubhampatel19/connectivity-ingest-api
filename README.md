# connectivity-ingest-api

The cloud's front door for devices. It authenticates them, validates their payloads and writes everything to Kafka. It holds no state of its own, so it scales horizontally and restarts freely.

Part of [Connectivity](https://github.com/spr732/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

## Endpoints

| Route | Auth | Purpose |
|---|---|---|
| `POST /v1/register` | `X-Enrollment-Key` | Register a device; returns its token |
| `POST /v1/heartbeat` | device token | Mark the device alive |
| `POST /v1/telemetry` | device token | Submit a batch of samples |
| `POST /v1/bundles` | device token | Start or resume a log bundle upload; returns chunk size and chunks received |
| `GET /v1/bundles/{id}` | device token | Upload status |
| `PUT /v1/bundles/{id}/chunks/{n}` | device token | One chunk, with `X-Chunk-SHA256` |
| `POST /v1/bundles/{id}/complete` | device token | Assemble, verify, store and publish the bundle |
| `GET /healthz` | none | Liveness: the process is up |
| `GET /readyz` | none | Readiness: Kafka is reachable |
| `GET /metrics` | none | Prometheus metrics (`connectivity_ingest_` prefix) |

Device requests carry `X-Device-ID` and `Authorization: Bearer <token>`. Tokens are HMAC-SHA256 of the device ID, so any replica can verify them without a lookup. A later phase replaces them with mTLS.

## Guarantees

- **202 means durable.** A batch is acknowledged only after Kafka acks it with `acks=all`.
- **Per-device ordering.** Records are keyed by device ID, so each device's data stays on one partition.
- **Back-pressure.** When Kafka is unavailable the API returns 503 with `Retry-After`, and devices buffer locally.

## Log bundles

Upload sessions live in the object store, not in the API process, so any replica can take any chunk and an upload survives an ingest restart:

```
uploads/<bundle_id>/manifest.json        the init request and chunk size
uploads/<bundle_id>/chunk-000000 ...     one object per received chunk
bundles/<device_id>/<bundle_id>.tar.gz   the finished bundle
```

On complete, the API streams the chunks into the final object while hashing them, rejects the bundle if the hash doesn't match what the device declared, publishes a `bundles.uploaded` event, and only then deletes the session. If publishing fails, the stored bundle is kept and the device's retry publishes it without re-assembling.

| Response | Meaning for the device |
|---|---|
| 409 on a chunk | Checksum mismatch, damaged in transit: resend it |
| 409 on complete | Chunks missing: re-init and send them |
| 404 on a chunk or complete | Session gone: re-init |
| 422 on complete | Assembled bundle doesn't match its declared SHA-256: give up |
| 503 | Object store or Kafka unavailable: retry later |

Readiness (`/readyz`) deliberately checks only Kafka. If it also checked the object store, a MinIO outage would pull every replica out of rotation and take telemetry down along with log uploads.

Leave `S3_ENDPOINT` unset and the bundle routes return 404; devices keep their bundles spooled until the cloud supports them.

## Configuration

| Variable | Default |
|---|---|
| `HTTP_ADDR` | `:8080` |
| `KAFKA_BROKERS` | `localhost:29092` |
| `TOKEN_SECRET` | required, at least 16 bytes |
| `ENROLLMENT_KEY` | required |
| `MAX_BODY_BYTES` | `1048576` |
| `PRODUCE_TIMEOUT` | `5s` |
| `S3_ENDPOINT` | unset (bundle uploads off) |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | required when `S3_ENDPOINT` is set |
| `S3_BUCKET` | `log-bundles` |
| `S3_USE_TLS` | `false` |
| `BUNDLE_CHUNK_SIZE` | `1048576` |
| `MAX_BUNDLE_BYTES` | `52428800` |
