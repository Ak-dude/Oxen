package api

import (
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"oxendb/server/internal/metrics"
)

// responseWriter is a wrapper that captures the status code written
// so middleware can record it for metrics and logs.
type responseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

func wrapResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, status: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.size += n
	return n, err
}

// LoggingMiddleware logs every request with method, path, status, and latency.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := wrapResponseWriter(w)
		next.ServeHTTP(rw, r)
		log.Printf(
			"%s %s %d %s %dB",
			r.Method,
			r.URL.Path,
			rw.status,
			time.Since(start).Round(time.Microsecond),
			rw.size,
		)
	})
}

// MetricsMiddleware records Prometheus request counters and histograms.
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := wrapResponseWriter(w)
		metrics.ActiveConnections.Inc()
		next.ServeHTTP(rw, r)
		metrics.ActiveConnections.Dec()

		elapsed := time.Since(start).Seconds()
		status := strconv.Itoa(rw.status)
		// Normalise path to reduce cardinality (strip URL path parameters)
		path := normalisePath(r.URL.Path)
		metrics.RequestsTotal.WithLabelValues(r.Method, path, status).Inc()
		metrics.RequestDuration.WithLabelValues(r.Method, path).Observe(elapsed)
	})
}

// AuthMiddleware validates the Authorization: Bearer <token> header when
// auth is enabled. Pass an empty string to disable.
func AuthMiddleware(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if token == "" {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hdr := r.Header.Get("Authorization")
			if !strings.HasPrefix(hdr, "Bearer ") {
				ErrUnauthorized(w)
				return
			}
			if strings.TrimPrefix(hdr, "Bearer ") != token {
				ErrUnauthorized(w)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RecoveryMiddleware catches panics, logs the stack trace, and returns HTTP 500.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("PANIC: %v\n%s", rec, debug.Stack())
				ErrInternal(w, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// normalisePath returns a sanitised path string suitable for use as a metric label.
// It replaces UUID-like and numeric path segments with placeholders to reduce cardinality.
func normalisePath(p string) string {
	parts := strings.Split(p, "/")
	for i, seg := range parts {
		if looksLikeID(seg) {
			parts[i] = ":id"
		}
	}
	return strings.Join(parts, "/")
}

// looksLikeID returns true for path segments that look like dynamic IDs
// (all digits, or long hex strings).
func looksLikeID(s string) bool {
	if len(s) == 0 {
		return false
	}
	allDigits := true
	allHex := true
	for _, c := range s {
		if c < '0' || c > '9' {
			allDigits = false
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || c == '-') {
			allHex = false
		}
	}
	// Consider it an ID if it's all digits or a long hex/UUID string
	return allDigits || (allHex && len(s) >= 32)
}
