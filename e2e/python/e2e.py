"""walrusd end-to-end check for the C ABI through Python's ctypes against real
object storage (MinIO/S3) and a real Redis/Valkey lease store.

Python has no dedicated binding: it drives the same shared library the
Node/Bun addon loads, through the documented six-function C ABI (JSON
envelopes in, JSON envelopes out).

The full loop for one fresh database: a read before any write (no lease), a
schema write, an insert write, an idempotent retry, and read-after-write.
Around every step the lease key is inspected directly in Redis (plain RESP
over a socket) to prove the runtime alone acquires and releases the lease and
that reads never touch it.

Run:

    python3 e2e/python/e2e.py

Configuration (all optional):

    WALRUSD_E2E_LIB (shared library path, default bindings/node/lib/libwalrusd.so),
    WALRUSD_E2E_ENDPOINT, WALRUSD_E2E_REGION, WALRUSD_E2E_BUCKET,
    WALRUSD_E2E_ACCESS_KEY, WALRUSD_E2E_SECRET_KEY, WALRUSD_E2E_REDIS,
    WALRUSD_E2E_ROOT_PREFIX (object-storage namespace, default "walrusd-e2e")

WALRUSD_E2E_ROOT_PREFIX is the only per-deployment knob: the per-run database
ID lives under it as "users/<library>_<random>" and never repeats the prefix.
Every language's e2e script follows this layout.
"""

import ctypes
import json
import os
import random
import socket
import string
import sys
import time

ENDPOINT = os.environ.get("WALRUSD_E2E_ENDPOINT", "http://127.0.0.1:9000")
REGION = os.environ.get("WALRUSD_E2E_REGION", "us-east-1")
BUCKET = os.environ.get("WALRUSD_E2E_BUCKET", "walrusd-e2e")
ACCESS_KEY = os.environ.get("WALRUSD_E2E_ACCESS_KEY", "walrusd")
SECRET_KEY = os.environ.get("WALRUSD_E2E_SECRET_KEY", "walrusdsecret")
REDIS = os.environ.get("WALRUSD_E2E_REDIS", "127.0.0.1:6379")
ROOT_PREFIX = os.environ.get("WALRUSD_E2E_ROOT_PREFIX", "walrusd-e2e")
LIBRARY = os.environ.get("WALRUSD_E2E_LIB", "bindings/node/lib/libwalrusd.so")

REQUEST_TIMEOUT_MS = 20_000

database_id = "users/python_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
lease_key = f"{ROOT_PREFIX}/{database_id}/lease.json"

descriptor = {
    "database_id": database_id,
    "storage": {
        "provider": "s3",
        "endpoint": ENDPOINT,
        "region": REGION,
        "bucket": BUCKET,
        "root_prefix": ROOT_PREFIX,
    },
    "credentials": {"access_key_id": ACCESS_KEY, "secret_access_key": SECRET_KEY},
}


def load_library():
    lib = ctypes.CDLL(LIBRARY)
    lib.walrusd_runtime_version.restype = ctypes.c_void_p
    lib.walrusd_runtime_init.argtypes = [ctypes.c_char_p, ctypes.c_int]
    lib.walrusd_runtime_init.restype = ctypes.c_uint64
    for name in ("walrusd_runtime_write", "walrusd_runtime_read"):
        fn = getattr(lib, name)
        fn.argtypes = [ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong]
        fn.restype = ctypes.c_void_p
    lib.walrusd_runtime_close.argtypes = [ctypes.c_uint64]
    lib.walrusd_runtime_close.restype = ctypes.c_void_p
    lib.walrusd_free.argtypes = [ctypes.c_void_p]
    return lib


lib = load_library()


def call(function, handle, request):
    """Run one ABI call and return its result payload (spec §11 envelope)."""
    payload = json.dumps(request).encode()
    deadline_ms = int(time.time() * 1000) + REQUEST_TIMEOUT_MS
    raw = function(handle, payload, len(payload), deadline_ms)
    if not raw:
        raise RuntimeError(f"{function.__name__}: null response from core")
    try:
        envelope = json.loads(ctypes.string_at(raw))
    finally:
        lib.walrusd_free(raw)
    if not envelope["ok"]:
        error = envelope["error"]
        raise RuntimeError(f'{error["class"]}: {error["message"]}')
    return envelope["result"]


def redis_command(command):
    """Send one inline RESP command and return its reply."""
    host, port = REDIS.split(":")
    with socket.create_connection((host, int(port)), timeout=5) as sock:
        sock.sendall(command.encode() + b"\r\n")
        buffer = b""
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                raise RuntimeError(f"redis: connection closed before replying to {command}")
            buffer += chunk
            reply = parse_reply(buffer)
            if reply is not None:
                return reply


def parse_reply(buffer):
    end = buffer.find(b"\r\n")
    if end < 0:
        return None
    header = buffer[:end].decode()
    if header.startswith(":"):
        return int(header[1:])
    if header.startswith("-"):
        raise RuntimeError(f"redis: {header[1:]}")
    if header.startswith("$"):
        length = int(header[1:])
        if length < 0:
            return None
        if len(buffer) < end + 2 + length + 2:
            return None
        return buffer[end + 2 : end + 2 + length].decode()
    raise RuntimeError(f"redis: unsupported reply {header}")


def lease_state(step):
    data = redis_command(f"HGET {lease_key} data")
    if not data:
        raise AssertionError(f"{step}: lease record {lease_key} not found")
    return json.loads(data)


def require_lease_absent(step):
    exists = redis_command(f"EXISTS {lease_key}")
    assert exists == 0, f"{step}: lease key {lease_key} exists; the operation took a lease it should not have"


def require_lease_released(step):
    record = lease_state(step)
    assert record["state"] == "released", f'{step}: lease state {record["state"]}, want released'


def require_lease_unchanged(step, before):
    after = lease_state(step)
    assert (after["state"], after["epoch"], after["lease_id"]) == (
        before["state"],
        before["epoch"],
        before["lease_id"],
    ), f"{step}: lease record changed ({before} -> {after}); reads must not touch leases"


def main():
    init_request = {
        "owner": "e2e-python-owner",
        "config": {
            "request_timeout_ms": REQUEST_TIMEOUT_MS,
            "redis_address": REDIS,
        },
    }
    payload = json.dumps(init_request).encode()
    handle = lib.walrusd_runtime_init(payload, len(payload))
    if handle == 0:
        raise RuntimeError("walrusd_runtime_init failed")

    try:
        # 1. A read on a database with no flushed state must not take a lease.
        empty = call(lib.walrusd_runtime_read, handle, {
            "descriptor": descriptor,
            "sql": "SELECT count(*) AS n FROM sqlite_master",
        })
        assert empty["rows"][0]["n"] == 0, f"read before write: expected empty database, got {empty['rows']}"
        require_lease_absent("read before write")
        print("ok read-before-write: empty database served, no lease created")

        # 2. Schema write: the runtime acquires the lease, commits, flushes to
        # object storage, releases the lease, and only then acknowledges.
        schema = call(lib.walrusd_runtime_write, handle, {
            "descriptor": descriptor,
            "idempotency_key": "e2e-schema",
            "statements": [{"sql": "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)"}],
        })
        assert schema["txid"], "schema write: empty txid"
        require_lease_released("schema write")
        print(f'ok schema write: txid={schema["txid"]} lease released')

        # 3. Insert write with a fresh idempotency key.
        value = "value-" + "".join(random.choices(string.ascii_lowercase, k=8))
        insert_key = "e2e-insert-" + "".join(random.choices(string.ascii_lowercase, k=8))
        insert = call(lib.walrusd_runtime_write, handle, {
            "descriptor": descriptor,
            "idempotency_key": insert_key,
            "statements": [{"sql": "INSERT INTO kv (k, v) VALUES (?, ?)", "params": ["greeting", value]}],
        })
        assert insert["txid"] > schema["txid"], (
            f'insert write: txid {insert["txid"]} did not advance past {schema["txid"]}'
        )
        released = lease_state("insert write")
        assert released["state"] == "released", f'insert write: lease state {released["state"]}, want released'
        assert released["epoch"] >= 2, (
            f'insert write: epoch {released["epoch"]}, want >= 2 (one acquisition per write)'
        )
        print(f'ok insert write: txid={insert["txid"]} epoch={released["epoch"]} lease released')

        # 4. Retrying the same idempotency key is deduplicated: same TXID,
        # no second mutation, lease still ends released.
        retry = call(lib.walrusd_runtime_write, handle, {
            "descriptor": descriptor,
            "idempotency_key": insert_key,
            "statements": [{"sql": "INSERT INTO kv (k, v) VALUES (?, ?)", "params": ["greeting", value]}],
        })
        assert retry["txid"] == insert["txid"], f'idempotent retry: txid {retry["txid"]}, want {insert["txid"]}'
        require_lease_released("idempotent retry")
        print(f'ok idempotent retry: txid={retry["txid"]} deduplicated')

        # 5. Reads see the flushed state and leave the lease record untouched.
        before = lease_state("read after write")
        read = call(lib.walrusd_runtime_read, handle, {
            "descriptor": descriptor,
            "sql": "SELECT v FROM kv WHERE k = ?",
            "params": ["greeting"],
        })
        assert read["rows"][0]["v"] == value, f'read after write: got {read["rows"][0]["v"]}, want {value}'
        require_lease_unchanged("read after write", before)
        print(f'ok read after write: value={read["rows"][0]["v"]} lease untouched (epoch={before["epoch"]})')

        # 6. A second read is served from the cached read session; still no lease.
        again = call(lib.walrusd_runtime_read, handle, {
            "descriptor": descriptor,
            "sql": "SELECT count(*) AS n FROM kv",
        })
        assert again["rows"][0]["n"] == 1, f"second read: {again['rows']}, want 1 row"
        require_lease_unchanged("second read", before)
        print("ok second read: cached session, lease untouched")

        print("PASS python")
    finally:
        raw = lib.walrusd_runtime_close(handle)
        if raw:
            lib.walrusd_free(raw)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - the script is the test runner
        print(f"FAIL python: {error}", file=sys.stderr)
        raise SystemExit(1)
