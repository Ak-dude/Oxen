# OxenDB

A hyperfast, lightweight, PostgreSQL-compatible database built for speed.

OxenDB pairs a Rust LSM-tree storage core with a Go HTTP+SQL server and speaks the native PostgreSQL wire protocol — meaning any standard Postgres client (`psql`, `pgcli`, ORMs, JDBC) connects without modification.

---

## Architecture

```
┌─────────────────────────────────────┐
│         Clients                     │
│  psql / pgcli / any Postgres driver │
│  OxenDB Python SDK  │  REST HTTP    │
└────────┬────────────┴───────────────┘
         │ PostgreSQL wire v3 (port 5432)   HTTP REST (port 8080)
┌────────▼────────────────────────────┐
│          Go Server                  │
│  ┌──────────────┐  ┌─────────────┐  │
│  │  PG Protocol │  │  HTTP API   │  │
│  └──────┬───────┘  └──────┬──────┘  │
│         └────────┬─────────┘        │
│            SQL Engine               │
│       (pg_query_go parser)          │
└────────────────┬────────────────────┘
                 │ FFI (cgo)
┌────────────────▼────────────────────┐
│          Rust Core                  │
│  WAL → MemTable → SSTable (L0-L6)   │
│  Group commit · mmap reads          │
│  Parallel compaction (rayon)        │
└─────────────────────────────────────┘
```

**Storage engine** — Rust LSM-tree with:
- Write-ahead log with CRC32C and group commit (500 µs batching window, single fsync per batch)
- Lock-free MemTable backed by `crossbeam-skiplist`
- Memory-mapped SSTables with bloom filters and LZ4 compression
- Leveled compaction (L0–L6) parallelised with `rayon`

**SQL engine** — Go with:
- Real PostgreSQL parser via `pg_query_go` (wraps the upstream `postgres/postgres` C source)
- Full SQL: `CREATE TABLE`, `INSERT`, `SELECT` (WHERE / GROUP BY / ORDER BY / LIMIT / JOIN), `UPDATE`, `DELETE`, `CREATE INDEX`
- Transactions (`BEGIN` / `COMMIT` / `ROLLBACK`) with write buffering and overlay reads
- Correct SQL three-valued NULL semantics throughout

**PostgreSQL wire protocol** — port 5432:
- Simple query and extended query protocols (Parse / Bind / Execute / Sync)
- System catalog intercepts (`pg_class`, `pg_attribute`, `information_schema`) for tool compatibility
- Trust and cleartext password auth modes

---

## Quick start

### Prerequisites

| Tool | Version |
|------|---------|
| Rust + Cargo | stable (≥ 1.75) |
| Go | ≥ 1.19 |
| Python | ≥ 3.9 |

### Build

```bash
# 1. Build the Rust core
cd core
cargo build --release --target x86_64-apple-darwin   # macOS x86/Rosetta
# or: cargo build --release                          # native arm64

# 2. Build the Go server
cd ../server
CGO_ENABLED=1 \
  CGO_LDFLAGS="-L../core/target/x86_64-apple-darwin/release -loxendb_core" \
  go build -tags cgo -o oxendb ./cmd/oxendb/

# 3. Run
DYLD_LIBRARY_PATH=../core/target/x86_64-apple-darwin/release \
  ./oxendb -data-dir /tmp/oxendb
```

Or use the Makefile:

```bash
make build-all   # builds core + server
make run         # starts the server on :8080 (HTTP) and :5432 (PG)
```

### Demo notes app

```bash
cd demo && ./start.sh
```

Starts the server and launches a tiny terminal notes app backed by OxenDB:

```
OxenDB Notes  (type 'help' for commands)
notes> add todo Buy groceries
  saved 'todo'
notes> get todo
  todo: Buy groceries
notes> list
  todo: Buy groceries
notes> del todo
  deleted 'todo'
```

---

## Connecting with psql

```bash
psql -h localhost -p 5432 -U oxen -d oxendb
```

```sql
CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL, age INT);
INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);
SELECT name, age FROM users WHERE age > 20 ORDER BY age DESC;
```

---

## HTTP REST API

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/v1/kv/:key` | Store raw bytes |
| `GET` | `/v1/kv/:key` | Retrieve value (base64 in JSON) |
| `DELETE` | `/v1/kv/:key` | Delete key |
| `POST` | `/v1/query` | Execute OxenQL query |
| `POST` | `/v1/batch` | Multi-key write (single fsync) |
| `GET` | `/metrics` | Prometheus metrics |

### OxenQL

```bash
# Range scan
curl -s localhost:8080/v1/query \
  -d '{"query": "SCAN FROM \"notes:\" TO \"notes:zz\" LIMIT 100"}'
```

---

## Python SDK

```python
from oxendb import OxenDBClient

db = OxenDBClient("http://localhost:8080")
db.put("hello", b"world")
print(db.get("hello"))        # b'world'

# SQL via query endpoint
result = db.query('SCAN FROM "a" TO "z" LIMIT 10')
```

---

## Configuration

Environment variables (all optional):

| Variable | Default | Description |
|----------|---------|-------------|
| `OXEN_HOST` | `0.0.0.0` | Listen host |
| `OXEN_PORT` | `8080` | HTTP port |
| `OXEN_PG_PORT` | `5432` | PostgreSQL wire port |
| `OXEN_DATA_DIR` | `./data` | Storage directory |
| `OXEN_PG_AUTH` | `trust` | Auth mode: `trust` or `cleartext` |

Or use a YAML config file: `oxendb -config config.yaml`

---

## Docker

```bash
docker compose up
```

Exposes port `8080` (HTTP) and `5432` (PostgreSQL wire).

---

## Project layout

```
core/          Rust LSM-tree storage engine + C FFI
server/
  cmd/oxendb/  Server entrypoint
  internal/
    api/       HTTP REST handlers
    bridge/    cgo bridge to Rust
    pg/        PostgreSQL wire protocol
    sql/       SQL parser, planner, executor, catalog
    query/     OxenQL lexer + parser
client/        Python SDK
demo/          Terminal notes app + pre-built binary
```
