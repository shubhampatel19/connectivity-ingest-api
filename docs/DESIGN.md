# connectivity-ingest-api: design

The ingest API is the only thing devices talk to in the cloud. Its job is narrow: decide whether a request is from a real device and well-formed, and if so make it durable in Kafka (or the object store) before answering. It keeps no state between requests.

## Layout

| File | Responsibility |
|---|---|
| `cmd/ingest-api/main.go` | Configuration, wiring, background bucket creation, shutdown |
| `internal/auth/token.go` | Device tokens: issue and verify |
| `internal/ingestion/server.go` | Routes, the JSON handlers, authentication, publishing |
| `internal/ingestion/middleware.go` | Request IDs, panic recovery, metrics and access logs, load shedding |
| `internal/ingestion/producer.go` | The Kafka producer behind a small interface |
| `internal/ingestion/bundles.go` | The resumable log-bundle upload protocol |
| `internal/ingestion/objectstore.go` | S3 behind a small interface |
| `internal/ingestion/metrics.go` | Every metric |

## The path of a request

```
recoverer ─▶ requestID ─▶ mux ─▶ TraceHTTP ─▶ instrument ─▶ shed ─▶ authenticate ─▶ handler
                           │
                           └─ /healthz, /readyz, /metrics   (never traced, shed or authenticated)
```

- `recoverer` turns a panic in a handler into a 500 and a logged stack, so one bad request can't take the process down.
- `requestID` gives every request an `X-Request-ID`, echoed in the response and the access log.
- `TraceHTTP` (from go-kit) opens a server span that continues the device's trace.
- `instrument` records the request metric and writes the access log. Successes log at debug, 4xx at info, 5xx at warn, so steady traffic doesn't bury the lines that matter.
- `shed` refuses the request if `MAX_IN_FLIGHT` are already in progress.
- `authenticate` checks the device token and puts the device ID in the context.

Health checks sit outside the chain on purpose. A saturated server must still answer `/healthz`, or Kubernetes would restart it for being busy.

## Authentication

A device registers once with the fleet's enrollment key and receives `token = HMAC-SHA256(TOKEN_SECRET, device_id)`. Every later request carries the device ID and the token, and any replica verifies it by recomputing the HMAC. There is no token store and no lookup.

Every handler then checks that the `device_id` inside the payload equals the authenticated one. Without that check, any device could write data as any other.

What this scheme can't do is the reason a later phase replaces it with client certificates: tokens never expire, one device's token can't be revoked without changing `TOKEN_SECRET` for the whole fleet, and anyone holding the enrollment key can obtain a token for any device ID. Changing `TOKEN_SECRET` invalidates every token at once; devices notice the 401 and register again.

## Durability: what a 202 means

`publish` marshals the payload and calls `Producer.Produce`, which blocks until Kafka acknowledges with `acks=all` or `PRODUCE_TIMEOUT` passes. The handler answers 202 only after that. On failure it answers 503 with `Retry-After`, the device keeps the batch, and nothing is lost.

Records are keyed by device ID. The telemetry and alert handlers wrap the payload in an envelope with `received_at`, because the device's clock can't be trusted for anything the cloud measures.

## Validation

Validation is the contracts module's `Validate` methods, called here because this is the last point where refusing bad data costs one device one 400. Once a record is in Kafka, a consumer that can't store it has to choose between stalling and dead-lettering.

A validation failure is a 400, which the agent treats as permanent and drops, so one bad batch never wedges a device's queue.

## Load shedding

`shed` is a semaphore. When `MAX_IN_FLIGHT` requests are in progress, the next one gets a 503 with `Retry-After` immediately.

The failure it prevents: Kafka slows down, every request takes seconds, devices hit their timeouts and retry, and the retries arrive while the originals are still running. In-flight requests and memory climb, and the service completes less the busier it gets. With a cap, admitted requests stay fast and everyone else is told to wait.

## Log bundles

A bundle is uploaded as chunks so that a dropped connection costs one chunk, not the whole file. All session state is in the object store:

```
uploads/<bundle_id>/manifest.json       the init request, the chunk size, when it started
uploads/<bundle_id>/chunk-000000 ...    one object per chunk received
bundles/<device_id>/<bundle_id>.tar.gz  the finished bundle
```

Because nothing is held in memory, any replica can serve any request for any bundle, and a restart mid-upload loses nothing.

| Step | Handler | What it guarantees |
|---|---|---|
| Init | `handleBundleInit` | Idempotent: the same init returns the session's state; a different size or hash under the same ID is a 409 |
| Chunk | `handleBundleChunk` | Exact length and SHA-256 are checked before the chunk is stored; a re-sent chunk overwrites itself |
| Complete | `handleBundleComplete` | Missing chunks are a 409 listing what's missing; otherwise assemble, verify, publish, clean up |

Completion is ordered so that every failure is recoverable by the device simply calling complete again:

1. Stream the chunks into the final object through a pipe, hashing as they pass. The bundle is never held in memory.
2. Compare the hash with the one the device declared at init. A mismatch deletes the object and the session and returns 422.
3. Publish `BundleUploadedEvent`. If this fails, the stored bundle stays and the retry skips straight to publishing.
4. Delete the session, manifest last, since the manifest is what makes it resumable.

A consequence of step 3 is that the event can be published twice. The contract says consumers must tolerate that.

**Timeouts.** The server's read timeout is 15 seconds, right for small JSON bodies. A chunk on a thin link takes longer, so the chunk and complete handlers replace the deadline for their own request with `http.ResponseController` (`extendDeadlines`). Chunks are 256 KiB with two minutes each, which works down to about 2 KB/s. The chunk size is in the manifest, so an upload that began under a different size finishes with it.

## Readiness

`/readyz` pings Kafka and nothing else. If it also checked the object store, an object store outage would mark every replica unready, the load balancer would stop sending them traffic, and a log-upload problem would become a telemetry outage. Bundle routes answer 503 by themselves while the object store is down.

`/healthz` always answers 200. A dependency being down is not a reason to restart the process.

## Shutdown

SIGTERM stops the listener and gives requests in flight ten seconds to finish (`platform.Serve`). In Kubernetes a `preStop` sleep runs first, so the pod is out of the load balancer before it stops accepting.

## Known limits

- No per-device rate limit: one misbehaving device can use a large share of `MAX_IN_FLIGHT`.
- Abandoned upload sessions (`uploads/...`) are never cleaned up, and finished bundles are kept forever.
- A bundle ID is global, not per device. A device that inits another device's ID first makes the real owner's init a 403.
- A chunk is read into memory before it is stored: `MAX_IN_FLIGHT` times the chunk size is the worst case.
