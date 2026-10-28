# connectivity-ingest-api

The cloud's front door for devices. It authenticates them, validates their payloads and writes everything to Kafka. It holds no state of its own, so it scales horizontally and restarts freely.

Part of [Connectivity](https://github.com/spr732/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

## Endpoints

| Route | Auth | Purpose |
|---|---|---|
| `POST /v1/register` | `X-Enrollment-Key` | Register a device; returns its token |
| `POST /v1/heartbeat` | device token | Mark the device alive |
| `POST /v1/telemetry` | device token | Submit a batch of samples |
| `GET /healthz` | none | Liveness: the process is up |
| `GET /readyz` | none | Readiness: Kafka is reachable |
| `GET /metrics` | none | Prometheus metrics (`connectivity_ingest_` prefix) |

Device requests carry `X-Device-ID` and `Authorization: Bearer <token>`. Tokens are HMAC-SHA256 of the device ID, so any replica can verify them without a lookup. A later phase replaces them with mTLS.

## Guarantees

- **202 means durable.** A batch is acknowledged only after Kafka acks it with `acks=all`.
- **Per-device ordering.** Records are keyed by device ID, so each device's data stays on one partition.
- **Back-pressure.** When Kafka is unavailable the API returns 503 with `Retry-After`, and devices buffer locally.

## Configuration

| Variable | Default |
|---|---|
| `HTTP_ADDR` | `:8080` |
| `KAFKA_BROKERS` | `localhost:29092` |
| `TOKEN_SECRET` | required, at least 16 bytes |
| `ENROLLMENT_KEY` | required |
| `MAX_BODY_BYTES` | `1048576` |
| `PRODUCE_TIMEOUT` | `5s` |
