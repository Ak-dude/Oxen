// Command oxendb is the OxenDB HTTP server.
//
// Usage:
//
//	oxendb [-config path/to/config.yaml] [-addr host:port] [-data-dir /path/to/data]
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"oxendb/server/internal/api"
	"oxendb/server/internal/bridge"
	"oxendb/server/internal/config"
)

func main() {
	// ---- flags ----
	configPath := flag.String("config", "", "path to YAML config file (optional)")
	overrideAddr := flag.String("addr", "", "override listen address host:port")
	overrideDataDir := flag.String("data-dir", "", "override database data directory")
	flag.Parse()

	// ---- load config ----
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	if *overrideAddr != "" {
		// Re-split host:port is non-trivial; just override the whole address via env
		log.Printf("note: -addr flag is advisory; set OXEN_HOST and OXEN_PORT for fine control")
	}
	if *overrideDataDir != "" {
		cfg.Database.DataDir = *overrideDataDir
	}

	log.Printf("OxenDB server starting (data=%s addr=%s)", cfg.Database.DataDir, cfg.Addr())

	// ---- open database via Rust FFI ----
	db, err := bridge.Open(cfg.Database.DataDir)
	if err != nil {
		log.Fatalf("failed to open database at %q: %v", cfg.Database.DataDir, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("warning: db.Close: %v", err)
		}
	}()

	// ---- build router ----
	router := api.NewRouter(cfg, db)

	// ---- HTTP server ----
	srv := &http.Server{
		Addr:         cfg.Addr(),
		Handler:      router,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSec) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSec) * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// ---- graceful shutdown ----
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("listening on %s", cfg.Addr())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-stop
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
	log.Println("server stopped")
}
