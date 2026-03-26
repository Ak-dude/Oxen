#!/usr/bin/env bash
# start.sh — build (if needed) and start the OxenDB server, then launch the notes app.

set -e
REPO="$(cd "$(dirname "$0")/.." && pwd)"
SERVER_BIN="$REPO/demo/oxendb-server"
DATA_DIR="/tmp/oxendb_notes_demo"
PORT=8080

# ---- build Rust core if needed ----
RUST_LIB="$REPO/core/target/x86_64-apple-darwin/release/liboxendb_core.dylib"
# Also try arm64 native build path
[ ! -f "$RUST_LIB" ] && RUST_LIB="$REPO/core/target/release/liboxendb_core.dylib"
if [ ! -f "$RUST_LIB" ]; then
  echo ">> Building Rust core (x86_64)..."
  cd "$REPO/core"
  ~/.cargo/bin/cargo build --release --target x86_64-apple-darwin
fi

# ---- build Go server if needed ----
if [ ! -f "$SERVER_BIN" ]; then
  echo ">> Building Go server..."
  cd "$REPO/server"
  CGO_ENABLED=1 \
    CGO_LDFLAGS="-L$REPO/core/target/x86_64-apple-darwin/release -loxendb_core" \
    /usr/local/go/bin/go build -tags cgo -o "$SERVER_BIN" ./cmd/oxendb/
fi

# ---- start server in background ----
mkdir -p "$DATA_DIR"
echo ">> Starting OxenDB on port $PORT (data: $DATA_DIR)"
DYLD_LIBRARY_PATH="$REPO/core/target/x86_64-apple-darwin/release:$DYLD_LIBRARY_PATH" \
  OXEN_PORT=$PORT \
  "$SERVER_BIN" -data-dir "$DATA_DIR" &
SERVER_PID=$!

# ---- wait for server to be ready ----
echo -n ">> Waiting for server..."
for i in $(seq 1 20); do
  if curl -sf "http://localhost:$PORT/metrics" >/dev/null 2>&1 || \
     curl -sf "http://localhost:$PORT/v1/kv/ping" >/dev/null 2>&1; then
    echo " ready."
    break
  fi
  sleep 0.3
  echo -n "."
done
echo ""

# ---- cleanup on exit ----
cleanup() {
  echo ""
  echo ">> Stopping OxenDB server (pid $SERVER_PID)..."
  kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# ---- launch notes app ----
python3 "$REPO/demo/notes.py"
