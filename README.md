# OxenDB
A cool little chat app in the terminal

---

## v0.0.1 — Terminal Chat (Python + Rust)

A minimal multi-client chat system.
**Rust** handles the TCP server; **Python** handles the CLI client.
Messages are in-memory only — nothing is persisted to disk.

```
┌─────────────┐   TCP (port 7878)   ┌────────────────────┐
│  client.py  │ ──────────────────► │  oxen-chat-server  │
│  (Python)   │ ◄────────────────── │  (Rust binary)     │
└─────────────┘   broadcast         └────────────────────┘
      ...                                    ▲
┌─────────────┐                             │
│  client.py  │ ────────────────────────────┘
│  (Python)   │
└─────────────┘
```

---

## Prerequisites

| Tool   | Install                              |
|--------|--------------------------------------|
| Rust   | https://rustup.rs                    |
| Python | 3.8+ (stdlib only, no pip packages)  |

---

## Quickstart

### 1. Start the Rust server

```bash
cargo run
```

You should see:
```
Oxen Chat Server v0.0.1
Listening on 127.0.0.1:7878  — press Ctrl+C to stop.
```

### 2. Open a client (repeat in as many terminals as you like)

```bash
python client.py
```

Enter a username when prompted, then start typing.

### 3. Chat

Every message you send is echoed back to **all** connected clients in real time.

---

## Project Layout

```
Oxen/
├── Cargo.toml        # Rust package manifest
├── src/
│   └── main.rs       # Rust TCP server
├── client.py         # Python terminal client
└── README.md
```

---

## How it works

### Rust server (`src/main.rs`)

- Binds a `TcpListener` on `127.0.0.1:7878`.
- Each new connection gets its own OS thread (`std::thread::spawn`).
- All connected clients are stored in a `Arc<Mutex<Vec<TcpStream>>>` so every thread can safely read and write the list.
- When a message arrives it is appended to an in-memory `Vec<String>` (history) and immediately broadcast to every client.
- Clients that fail to receive (disconnected) are silently removed from the list via `retain_mut`.

### Python client (`client.py`)

- Uses only Python's standard library (`socket`, `threading`, `sys`).
- A **background daemon thread** calls `sock.recv()` in a loop and prints incoming messages.
- The **main thread** reads `input()` and sends `username: message\n` to the server.

---

## Note on PyO3

PyO3 produces a Python *extension module* embedded in a single Python process — great for one-process tools, but incompatible with a multi-client server that needs to run as a standalone, always-on process accepting connections from many clients. TCP is the right primitive for v0.0.1. A PyO3 binding that wraps the client-side logic is planned for a future version.

---

## Known limitations (v0.0.1)

- No message persistence — history is lost when the server stops.
- No channels, rooms, or private messages.
- No authentication or encryption.
- All clients must be on the same machine (`127.0.0.1`).
