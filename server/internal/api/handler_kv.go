package api

import (
	"encoding/base64"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"oxendb/server/internal/bridge"
	"oxendb/server/internal/metrics"
)

// kvResponse is the JSON shape for GET /v1/kv/:key.
type kvResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"` // base64-encoded bytes
}

// KVHandlers groups GET, PUT, DELETE handlers for the /v1/kv/{key} route.
type KVHandlers struct {
	db *bridge.DB
}

// NewKVHandlers constructs a KVHandlers.
func NewKVHandlers(db *bridge.DB) *KVHandlers {
	return &KVHandlers{db: db}
}

// Get handles GET /v1/kv/{key}
// Returns the base64-encoded value for the key, or 404 if not found.
func (h *KVHandlers) Get(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if key == "" {
		ErrBadRequest(w, "key parameter is required")
		return
	}

	value, err := h.db.Get([]byte(key))
	if err != nil {
		if errors.Is(err, bridge.ErrNotFound) {
			metrics.RecordOp("get", "not_found")
			ErrNotFound(w, "key not found")
			return
		}
		metrics.RecordOp("get", "error")
		ErrInternal(w, err.Error())
		return
	}

	metrics.RecordOp("get", "ok")
	JSON(w, kvResponse{
		Key:   key,
		Value: base64.StdEncoding.EncodeToString(value),
	})
}

// Put handles PUT /v1/kv/{key}
// Request body is the raw value bytes. Returns 201 on success.
func (h *KVHandlers) Put(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if key == "" {
		ErrBadRequest(w, "key parameter is required")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024*1024)) // 64 MiB max
	if err != nil {
		ErrBadRequest(w, "could not read request body")
		return
	}
	defer r.Body.Close()

	if err := h.db.Put([]byte(key), body); err != nil {
		metrics.RecordOp("put", "error")
		ErrInternal(w, err.Error())
		return
	}

	metrics.RecordOp("put", "ok")
	JSONCreated(w, kvResponse{
		Key:   key,
		Value: base64.StdEncoding.EncodeToString(body),
	})
}

// Delete handles DELETE /v1/kv/{key}
// Returns 204 on success.
func (h *KVHandlers) Delete(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if key == "" {
		ErrBadRequest(w, "key parameter is required")
		return
	}

	if err := h.db.Delete([]byte(key)); err != nil {
		if errors.Is(err, bridge.ErrNotFound) {
			metrics.RecordOp("delete", "not_found")
			// Treat delete-of-nonexistent as success (idempotent)
			JSONNoContent(w)
			return
		}
		metrics.RecordOp("delete", "error")
		ErrInternal(w, err.Error())
		return
	}

	metrics.RecordOp("delete", "ok")
	JSONNoContent(w)
}
