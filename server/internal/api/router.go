package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"oxendb/server/internal/bridge"
	"oxendb/server/internal/config"
)

// NewRouter builds the chi router with all routes and middleware wired up.
func NewRouter(cfg *config.Config, db *bridge.DB) http.Handler {
	r := chi.NewRouter()

	// ---- global middleware ----
	r.Use(RecoveryMiddleware)
	r.Use(LoggingMiddleware)
	r.Use(MetricsMiddleware)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(AuthMiddleware(cfg.Auth.Token))

	// ---- Prometheus metrics ----
	if cfg.Metrics.Enabled {
		path := cfg.Metrics.Path
		if path == "" {
			path = "/metrics"
		}
		r.Handle(path, promhttp.Handler())
	}

	// ---- health / readiness ----
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		JSONMessage(w, "ok")
	})

	// ---- API v1 ----
	kv := NewKVHandlers(db)
	qh := NewQueryHandler(db)
	bh := NewBatchHandler(db)
	ah := NewAdminHandlers(db)

	r.Route("/v1", func(r chi.Router) {
		// Key-value CRUD
		r.Route("/kv/{key}", func(r chi.Router) {
			r.Get("/", kv.Get)
			r.Put("/", kv.Put)
			r.Delete("/", kv.Delete)
		})

		// OxenQL query
		r.Post("/query", qh.Handle)

		// Batch writes
		r.Post("/batch", bh.Handle)

		// Admin
		r.Route("/admin", func(r chi.Router) {
			r.Get("/stats", ah.Stats)
			r.Post("/compact", ah.Compact)
		})
	})

	return r
}
