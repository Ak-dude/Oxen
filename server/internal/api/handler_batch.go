package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"oxendb/server/internal/bridge"
	"oxendb/server/internal/metrics"
)

// batchOpRequest is one operation in the batch request body.
type batchOpRequest struct {
	Op    string `json:"op"`    // "put" or "delete"
	Key   string `json:"key"`
	Value string `json:"value"` // raw string value (for put)
}

// batchRequest is the JSON body for POST /v1/batch.
type batchRequest struct {
	Ops []batchOpRequest `json:"ops"`
}

// batchResponse reports how many operations were applied.
type batchResponse struct {
	Applied int    `json:"applied"`
	Message string `json:"message"`
}

// BatchHandler handles POST /v1/batch.
type BatchHandler struct {
	db *bridge.DB
}

// NewBatchHandler constructs a BatchHandler.
func NewBatchHandler(db *bridge.DB) *BatchHandler {
	return &BatchHandler{db: db}
}

// Handle processes a JSON batch of put/delete operations atomically
// (best-effort: operations are executed in order and errors abort the batch).
func (h *BatchHandler) Handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024*1024)) // 64 MiB
	if err != nil {
		ErrBadRequest(w, "could not read request body")
		return
	}
	defer r.Body.Close()

	var req batchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		ErrBadRequest(w, "invalid JSON: "+err.Error())
		return
	}
	if len(req.Ops) == 0 {
		ErrBadRequest(w, "ops array must not be empty")
		return
	}

	var applied int
	for i, op := range req.Ops {
		if op.Key == "" {
			ErrBadRequest(w, "op["+itoa(i)+"]: key is required")
			return
		}
		key := []byte(op.Key)

		switch op.Op {
		case "put":
			if err := h.db.Put(key, []byte(op.Value)); err != nil {
				metrics.RecordOp("batch_put", "error")
				ErrInternal(w, "op["+itoa(i)+"]: "+err.Error())
				return
			}
			metrics.RecordOp("batch_put", "ok")
		case "delete":
			if err := h.db.Delete(key); err != nil {
				if !errors.Is(err, bridge.ErrNotFound) {
					metrics.RecordOp("batch_delete", "error")
					ErrInternal(w, "op["+itoa(i)+"]: "+err.Error())
					return
				}
				// delete of nonexistent key is idempotent — count it as applied
			}
			metrics.RecordOp("batch_delete", "ok")
		default:
			ErrBadRequest(w, "op["+itoa(i)+"]: unknown op "+op.Op+", expected put or delete")
			return
		}
		applied++
	}

	JSON(w, batchResponse{
		Applied: applied,
		Message: "batch applied",
	})
}

// itoa is a tiny helper — avoids importing strconv just for this.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 20)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
