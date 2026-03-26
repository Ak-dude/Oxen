package api

import (
	"net/http"
	"runtime"

	"oxendb/server/internal/bridge"
	"oxendb/server/internal/metrics"
)

// statsResponse is the JSON body for GET /v1/admin/stats.
type statsResponse struct {
	GoRoutines  int    `json:"goroutines"`
	HeapAllocMB float64 `json:"heap_alloc_mb"`
	SysMB       float64 `json:"sys_mb"`
	GCRuns      uint32 `json:"gc_runs"`
	DBStatus    string `json:"db_status"`
}

// AdminHandlers groups admin endpoint handlers.
type AdminHandlers struct {
	db *bridge.DB
}

// NewAdminHandlers constructs an AdminHandlers.
func NewAdminHandlers(db *bridge.DB) *AdminHandlers {
	return &AdminHandlers{db: db}
}

// Stats handles GET /v1/admin/stats
// Returns runtime memory stats and database status.
func (h *AdminHandlers) Stats(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	dbStatus := "ok"
	if h.db == nil {
		dbStatus = "not_connected"
	}

	JSON(w, statsResponse{
		GoRoutines:  runtime.NumGoroutine(),
		HeapAllocMB: float64(ms.HeapAlloc) / (1024 * 1024),
		SysMB:       float64(ms.Sys) / (1024 * 1024),
		GCRuns:      ms.NumGC,
		DBStatus:    dbStatus,
	})
}

// Compact handles POST /v1/admin/compact
// Triggers a synchronous compaction round in the Rust engine.
func (h *AdminHandlers) Compact(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		ErrInternal(w, "database not connected")
		return
	}
	if err := h.db.Compact(); err != nil {
		metrics.RecordOp("compact", "error")
		ErrInternal(w, err.Error())
		return
	}
	metrics.CompactionRuns.Inc()
	metrics.RecordOp("compact", "ok")
	JSONMessage(w, "compaction triggered")
}
