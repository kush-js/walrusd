import ctypes
import json

lib = ctypes.CDLL("./libwalrusd.so")
lib.walrusd_runtime_version.restype = ctypes.c_void_p
lib.walrusd_runtime_init.argtypes = [ctypes.c_char_p, ctypes.c_int]
lib.walrusd_runtime_init.restype = ctypes.c_uint64
lib.walrusd_free.argtypes = [ctypes.c_void_p]

version = lib.walrusd_runtime_version()
assert json.loads(ctypes.string_at(version))["ok"] is True
lib.walrusd_free(version)

init_request = json.dumps({
    "owner": "api-pod-7",
    "config": {
        "request_timeout_ms": 20_000,
        "write_buffer_root_path": "/tmp/walrusd-example/buffers",
    },
}).encode()
handle = lib.walrusd_runtime_init(init_request, len(init_request))
if handle == 0:
    raise RuntimeError("walrusd_runtime_init failed")

descriptor = {
    "database_id": "users/user_1a4b",
    "storage": {"provider": "file", "file_root": "/tmp/walrusd-example"},
    "credentials": {},
}

import time

lib.walrusd_runtime_write.argtypes = [
    ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong,
]
lib.walrusd_runtime_write.restype = ctypes.c_void_p
deadline_ms = int(time.time() * 1000) + 20_000

# The documentation puts Read before Write, so seed the database with the
# same idempotent operation first. The Write snippet below returns that txid.
seed_request = json.dumps({
    "descriptor": descriptor,
    "idempotency_key": "create-event-42",
    "statements": [
        {
            "sql": "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)",
        },
        {
            "sql": "INSERT INTO events (id, body) VALUES (?, ?)",
            "params": [1, "hello from walrusd"],
        },
    ],
}).encode()
seed_response = lib.walrusd_runtime_write(
    handle, seed_request, len(seed_request), deadline_ms)
seed_envelope = json.loads(ctypes.string_at(seed_response))
lib.walrusd_free(seed_response)
if not seed_envelope["ok"]:
    raise RuntimeError(seed_envelope["error"])

lib.walrusd_runtime_read.argtypes = [
    ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong,
]
lib.walrusd_runtime_read.restype = ctypes.c_void_p

read_request = json.dumps({
    "descriptor": descriptor,
    "sql": "SELECT body FROM events WHERE id = ?",
    "params": [1],
}).encode()
response = lib.walrusd_runtime_read(
    handle, read_request, len(read_request), deadline_ms)
envelope = json.loads(ctypes.string_at(response))
lib.walrusd_free(response)
assert envelope["ok"] is True
rows = envelope["result"]["rows"]
body = rows[0]["body"]

lib.walrusd_runtime_write.argtypes = [
    ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong,
]
lib.walrusd_runtime_write.restype = ctypes.c_void_p

write_request = json.dumps({
    "descriptor": descriptor,
    "idempotency_key": "create-event-42",
    "statements": [
        {
            "sql": "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)",
        },
        {
            "sql": "INSERT INTO events (id, body) VALUES (?, ?)",
            "params": [1, "hello from walrusd"],
        },
    ],
}).encode()
response = lib.walrusd_runtime_write(
    handle, write_request, len(write_request), deadline_ms)
envelope = json.loads(ctypes.string_at(response))
lib.walrusd_free(response)
assert envelope["ok"] is True
txid = envelope["result"]["txid"]

error_response = lib.walrusd_runtime_write(
    handle, write_request, len(write_request), deadline_ms)
error_envelope = json.loads(ctypes.string_at(error_response))
lib.walrusd_free(error_response)
if not error_envelope["ok"]:
    error = error_envelope["error"]
    if error["class"] == "DB_FLUSH_FAILED":
        pass  # retry with the same idempotency key
    elif error["class"] == "DB_BUSY":
        print("retry after", error["retry_after_ms"], "ms")

print(f"Python (C ABI) durable at txid {txid}")
print(f"Python (C ABI) read: {body}")

lib.walrusd_runtime_close.argtypes = [ctypes.c_uint64]
lib.walrusd_runtime_close.restype = ctypes.c_void_p
close_response = lib.walrusd_runtime_close(handle)
close_envelope = json.loads(ctypes.string_at(close_response))
lib.walrusd_free(close_response)
if not close_envelope["ok"]:
    raise RuntimeError(close_envelope["error"])
