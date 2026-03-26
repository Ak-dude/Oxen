#!/usr/bin/env python3
"""
OxenDB Notes — a tiny terminal notes app backed by OxenDB.

Commands:
  add <title> <text...>   Save a note
  get <title>             Read a note
  list                    List all note titles
  del <title>             Delete a note
  quit                    Exit
"""
import base64
import json
import sys
import urllib.parse
import urllib.request
import urllib.error

BASE = "http://localhost:8080/v1"


def _req(method, path, body=None, content_type="application/json"):
    if isinstance(body, str):
        data = body.encode()
    elif isinstance(body, bytes):
        data = body
    elif body is not None:
        data = json.dumps(body).encode()
    else:
        data = None

    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": content_type} if data else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return json.loads(raw)
        except Exception:
            return {"status": "error", "message": raw.decode()}


def _key(title):
    return urllib.parse.quote(f"notes:{title}", safe="")


def cmd_add(title, text):
    resp = _req("PUT", f"/kv/{_key(title)}", body=text.encode(), content_type="text/plain")
    data = resp.get("data", {})
    if resp.get("status") == "ok":
        print(f"  saved '{title}'")
    else:
        print(f"  error: {resp.get('message', resp)}")


def cmd_get(title):
    resp = _req("GET", f"/kv/{_key(title)}")
    if resp.get("status") == "error":
        print(f"  not found: '{title}'")
        return
    value_b64 = resp.get("data", {}).get("value", "")
    text = base64.b64decode(value_b64).decode()
    print(f"  {title}: {text}")


def cmd_list():
    resp = _req("POST", "/query", {"query": 'SCAN FROM "notes:" TO "notes:zz" LIMIT 100'})
    if resp.get("status") == "error":
        print(f"  error: {resp.get('message', resp)}")
        return
    pairs = resp.get("data", {}).get("pairs") or []
    if not pairs:
        print("  (no notes)")
        return
    for pair in pairs:
        title = pair["key"].removeprefix("notes:")
        snippet = base64.b64decode(pair["value"]).decode()[:60]
        ellipsis = "..." if len(base64.b64decode(pair["value"]).decode()) > 60 else ""
        print(f"  {title}: {snippet}{ellipsis}")


def cmd_del(title):
    resp = _req("DELETE", f"/kv/{_key(title)}")
    # DELETE returns 204 (empty body) on success
    if resp.get("status") == "error":
        print(f"  error: {resp.get('message', resp)}")
    else:
        print(f"  deleted '{title}'")


def check_server():
    try:
        urllib.request.urlopen(BASE + "/kv/ping", timeout=2)
        return True
    except urllib.error.HTTPError:
        return True  # 404 means server is up
    except Exception:
        return False


def repl():
    print("OxenDB Notes  (type 'help' for commands)")
    while True:
        try:
            line = input("notes> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nbye")
            break

        if not line:
            continue

        parts = line.split(None, 2)
        cmd = parts[0].lower()

        if cmd in ("quit", "exit", "q"):
            print("bye")
            break
        elif cmd == "help":
            print("  add <title> <text...>  save a note")
            print("  get <title>            read a note")
            print("  list                   list all notes")
            print("  del <title>            delete a note")
            print("  quit                   exit")
        elif cmd == "add":
            if len(parts) < 3:
                print("  usage: add <title> <text...>")
            else:
                cmd_add(parts[1], parts[2])
        elif cmd == "get":
            if len(parts) < 2:
                print("  usage: get <title>")
            else:
                cmd_get(parts[1])
        elif cmd == "list":
            cmd_list()
        elif cmd in ("del", "delete", "rm"):
            if len(parts) < 2:
                print("  usage: del <title>")
            else:
                cmd_del(parts[1])
        else:
            print(f"  unknown command: {cmd}  (type 'help')")


if __name__ == "__main__":
    if not check_server():
        print("OxenDB server is not running.")
        print("Start it first:  cd demo && ./start.sh")
        sys.exit(1)
    repl()
