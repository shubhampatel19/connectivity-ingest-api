package ingestion

// Log bundle uploads. Session state lives in the object store:
//
//	uploads/<bundle_id>/manifest.json       the init request plus chunk size
//	uploads/<bundle_id>/chunk-000000 ...    one object per received chunk
//	bundles/<device_id>/<bundle_id>.tar.gz  the finished, verified bundle
//
// Any replica can serve any request for any bundle, and an upload survives
// an ingest restart. Completion streams the chunks into the final object
// while hashing them, verifies the declared SHA-256, publishes the event to
// Kafka, and only then deletes the session.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

type BundleConfig struct {
	ChunkSize      int64
	MaxBundleBytes int64
}

type manifest struct {
	Request   wire.BundleInitRequest `json:"request"`
	ChunkSize int64                  `json:"chunk_size"`
	CreatedAt time.Time              `json:"created_at"`
}

func (m manifest) chunkCount() int { return wire.ChunkCount(m.Request.SizeBytes, m.ChunkSize) }

func sessionPrefix(bundleID string) string { return "uploads/" + bundleID + "/" }
func manifestKey(bundleID string) string   { return sessionPrefix(bundleID) + "manifest.json" }
func chunkKey(bundleID string, i int) string {
	return fmt.Sprintf("%schunk-%06d", sessionPrefix(bundleID), i)
}
func bundleKey(deviceID, bundleID string) string {
	return "bundles/" + deviceID + "/" + bundleID + ".tar.gz"
}

func (s *Server) handleBundleInit(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	var req wire.BundleInitRequest
	if !s.decode(w, r, 64<<10, &req) {
		return
	}
	if req.DeviceID != deviceID {
		writeError(w, http.StatusForbidden, "bundle device_id does not match credentials")
		return
	}
	if err := req.Validate(s.cfg.Bundles.MaxBundleBytes); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx := r.Context()

	m, found, err := s.loadManifest(ctx, req.BundleID)
	if err != nil {
		s.storeFailed(w, "get_manifest", err)
		return
	}
	if found {
		if m.Request.DeviceID != deviceID {
			writeError(w, http.StatusForbidden, "bundle belongs to another device")
			return
		}
		if m.Request.SHA256 != req.SHA256 || m.Request.SizeBytes != req.SizeBytes {
			writeError(w, http.StatusConflict, "bundle_id already used for different content")
			return
		}
		s.writeSessionStatus(w, r, m)
		return
	}

	done, err := s.store.Exists(ctx, bundleKey(deviceID, req.BundleID))
	if err != nil {
		s.storeFailed(w, "exists", err)
		return
	}
	if done {
		writeJSON(w, http.StatusOK, completeStatus(req.BundleID, req.SizeBytes, s.cfg.Bundles.ChunkSize))
		return
	}

	m = manifest{Request: req, ChunkSize: s.cfg.Bundles.ChunkSize, CreatedAt: s.now().UTC()}
	body, _ := json.Marshal(m)
	if err := s.store.Put(ctx, manifestKey(req.BundleID), bytes.NewReader(body), int64(len(body))); err != nil {
		s.storeFailed(w, "put_manifest", err)
		return
	}
	s.m.bundlesStarted.Inc()
	writeJSON(w, http.StatusOK, wire.BundleStatus{
		BundleID: req.BundleID, State: wire.BundleUploading, ChunkSize: m.ChunkSize,
		ChunkCount: m.chunkCount(), ReceivedChunks: []int{},
	})
}

func (s *Server) handleBundleStatus(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	bundleID := r.PathValue("bundle_id")
	if !wire.ValidID(bundleID) {
		writeError(w, http.StatusBadRequest, "invalid bundle_id")
		return
	}
	m, found, err := s.loadManifest(r.Context(), bundleID)
	if err != nil {
		s.storeFailed(w, "get_manifest", err)
		return
	}
	if found {
		if m.Request.DeviceID != deviceID {
			writeError(w, http.StatusForbidden, "bundle belongs to another device")
			return
		}
		s.writeSessionStatus(w, r, m)
		return
	}
	writeError(w, http.StatusNotFound, "no upload in progress for this bundle")
}

func (s *Server) handleBundleChunk(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	bundleID := r.PathValue("bundle_id")
	index, err := strconv.Atoi(r.PathValue("index"))
	if !wire.ValidID(bundleID) || err != nil {
		writeError(w, http.StatusBadRequest, "invalid bundle_id or chunk index")
		return
	}
	ctx := r.Context()
	m, found, err := s.loadManifest(ctx, bundleID)
	if err != nil {
		s.storeFailed(w, "get_manifest", err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "no upload in progress for this bundle; init it first")
		return
	}
	if m.Request.DeviceID != deviceID {
		writeError(w, http.StatusForbidden, "bundle belongs to another device")
		return
	}
	if index < 0 || index >= m.chunkCount() {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("chunk index %d out of range [0,%d)", index, m.chunkCount()))
		return
	}

	want := wire.ChunkLen(m.Request.SizeBytes, m.ChunkSize, index)
	r.Body = http.MaxBytesReader(w, r.Body, want)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("chunk %d must be %d bytes", index, want))
			return
		}
		writeError(w, http.StatusBadRequest, "reading chunk: "+err.Error())
		return
	}
	if int64(len(data)) != want {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("chunk %d is %d bytes, must be %d", index, len(data), want))
		return
	}
	// A checksum mismatch means the bytes were damaged in transit. That is
	// worth resending, so it's a 409 rather than a 4xx the device gives up on.
	sum := sha256.Sum256(data)
	if !strings.EqualFold(r.Header.Get(wire.HeaderChunkSHA256), hex.EncodeToString(sum[:])) {
		s.m.chunkChecksumFailures.Inc()
		writeError(w, http.StatusConflict, "chunk checksum mismatch; resend it")
		return
	}

	if err := s.store.Put(ctx, chunkKey(bundleID, index), bytes.NewReader(data), want); err != nil {
		s.storeFailed(w, "put_chunk", err)
		return
	}
	s.m.bundleBytes.Add(float64(want))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleBundleComplete(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	bundleID := r.PathValue("bundle_id")
	if !wire.ValidID(bundleID) {
		writeError(w, http.StatusBadRequest, "invalid bundle_id")
		return
	}
	ctx := r.Context()
	finalKey := bundleKey(deviceID, bundleID)

	m, found, err := s.loadManifest(ctx, bundleID)
	if err != nil {
		s.storeFailed(w, "get_manifest", err)
		return
	}
	if !found {
		// Either never started, or already completed and cleaned up.
		done, err := s.store.Exists(ctx, finalKey)
		if err != nil {
			s.storeFailed(w, "exists", err)
			return
		}
		if !done {
			writeError(w, http.StatusNotFound, "no upload in progress for this bundle")
			return
		}
		writeJSON(w, http.StatusOK, wire.BundleStatus{BundleID: bundleID, State: wire.BundleComplete})
		return
	}
	if m.Request.DeviceID != deviceID {
		writeError(w, http.StatusForbidden, "bundle belongs to another device")
		return
	}

	received, err := s.receivedChunks(ctx, bundleID)
	if err != nil {
		s.storeFailed(w, "list_chunks", err)
		return
	}
	status := wire.BundleStatus{BundleID: bundleID, State: wire.BundleUploading, ChunkSize: m.ChunkSize,
		ChunkCount: m.chunkCount(), ReceivedChunks: received}
	if missing := status.Missing(); len(missing) > 0 {
		writeJSON(w, http.StatusConflict, status)
		return
	}

	// A previous attempt may have stored the bundle but failed to publish.
	// Don't assemble twice; go straight to publishing.
	stored, err := s.store.Exists(ctx, finalKey)
	if err != nil {
		s.storeFailed(w, "exists", err)
		return
	}
	if !stored {
		start := time.Now()
		sum, err := s.assemble(ctx, m, finalKey)
		if err != nil {
			s.m.assemblyFailures.WithLabelValues("store").Inc()
			s.storeFailed(w, "assemble", err)
			return
		}
		if sum != m.Request.SHA256 {
			s.m.assemblyFailures.WithLabelValues("checksum").Inc()
			s.log.Error("assembled bundle does not match its declared checksum",
				"bundle_id", bundleID, "device_id", deviceID, "declared", m.Request.SHA256, "actual", sum)
			_ = s.store.Delete(ctx, finalKey)
			s.cleanupSession(ctx, m)
			writeError(w, http.StatusUnprocessableEntity, "assembled bundle does not match declared sha256")
			return
		}
		s.m.assemblyDuration.Observe(time.Since(start).Seconds())
	}

	ev := wire.BundleUploadedEvent{
		BundleID: bundleID, DeviceID: deviceID, Reason: m.Request.Reason, ObjectKey: finalKey,
		SizeBytes: m.Request.SizeBytes, SHA256: m.Request.SHA256,
		CollectedAt: m.Request.CollectedAt, UploadedAt: s.now().UTC(),
	}
	if err := s.publish(ctx, wire.TopicBundles, deviceID, ev); err != nil {
		writeUnavailable(w) // the stored bundle stays; a retried complete publishes it
		return
	}
	s.cleanupSession(ctx, m)
	s.m.bundlesCompleted.WithLabelValues(string(m.Request.Reason)).Inc()
	writeJSON(w, http.StatusOK, completeStatus(bundleID, m.Request.SizeBytes, m.ChunkSize))
}

// assemble streams the chunks, in order, into the final object and returns
// the SHA-256 of what was written.
func (s *Server) assemble(ctx context.Context, m manifest, finalKey string) (string, error) {
	pr, pw := io.Pipe()
	h := sha256.New()
	done := make(chan error, 1)
	go func() {
		for i := 0; i < m.chunkCount(); i++ {
			rc, err := s.store.Get(ctx, chunkKey(m.Request.BundleID, i))
			if err != nil {
				pw.CloseWithError(err)
				done <- err
				return
			}
			_, err = io.Copy(io.MultiWriter(pw, h), rc)
			rc.Close()
			if err != nil {
				pw.CloseWithError(err)
				done <- err
				return
			}
		}
		pw.Close()
		done <- nil
	}()

	putErr := s.store.Put(ctx, finalKey, pr, m.Request.SizeBytes)
	pr.CloseWithError(errors.New("assembly aborted")) // unblocks the writer if Put gave up early
	if err := <-done; err != nil {
		return "", err
	}
	if putErr != nil {
		return "", putErr
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *Server) cleanupSession(ctx context.Context, m manifest) {
	for i := 0; i < m.chunkCount(); i++ {
		if err := s.store.Delete(ctx, chunkKey(m.Request.BundleID, i)); err != nil {
			s.log.Warn("leaving orphaned chunk", "bundle_id", m.Request.BundleID, "chunk", i, "err", err)
		}
	}
	// The manifest goes last: while it exists, the session can still be resumed.
	if err := s.store.Delete(ctx, manifestKey(m.Request.BundleID)); err != nil {
		s.log.Warn("leaving orphaned manifest", "bundle_id", m.Request.BundleID, "err", err)
	}
}

func (s *Server) loadManifest(ctx context.Context, bundleID string) (manifest, bool, error) {
	var m manifest
	rc, err := s.store.Get(ctx, manifestKey(bundleID))
	if errors.Is(err, ErrObjectNotFound) {
		return m, false, nil
	}
	if err != nil {
		return m, false, err
	}
	defer rc.Close()
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return m, false, fmt.Errorf("corrupt manifest: %w", err)
	}
	return m, true, nil
}

func (s *Server) receivedChunks(ctx context.Context, bundleID string) ([]int, error) {
	keys, err := s.store.List(ctx, sessionPrefix(bundleID)+"chunk-")
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, len(keys))
	for _, k := range keys {
		n, err := strconv.Atoi(strings.TrimPrefix(k, sessionPrefix(bundleID)+"chunk-"))
		if err == nil {
			out = append(out, n)
		}
	}
	sort.Ints(out)
	return out, nil
}

func (s *Server) writeSessionStatus(w http.ResponseWriter, r *http.Request, m manifest) {
	received, err := s.receivedChunks(r.Context(), m.Request.BundleID)
	if err != nil {
		s.storeFailed(w, "list_chunks", err)
		return
	}
	writeJSON(w, http.StatusOK, wire.BundleStatus{
		BundleID: m.Request.BundleID, State: wire.BundleUploading, ChunkSize: m.ChunkSize,
		ChunkCount: m.chunkCount(), ReceivedChunks: received,
	})
}

func completeStatus(bundleID string, size, chunkSize int64) wire.BundleStatus {
	n := wire.ChunkCount(size, chunkSize)
	all := make([]int, n)
	for i := range all {
		all[i] = i
	}
	return wire.BundleStatus{BundleID: bundleID, State: wire.BundleComplete, ChunkSize: chunkSize, ChunkCount: n, ReceivedChunks: all}
}

// storeFailed reports an object store outage as retryable.
func (s *Server) storeFailed(w http.ResponseWriter, op string, err error) {
	s.m.storeErrors.WithLabelValues(op).Inc()
	s.log.Error("object store operation failed", "op", op, "err", err)
	writeUnavailable(w)
}
