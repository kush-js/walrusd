import ctypes
import json
import os
import time

lib = ctypes.CDLL(os.environ["WALRUSD_LIBRARY"])
root = os.environ.get("WALRUSD_EXAMPLE_ROOT", "/tmp/walrusd-example")
buffer_root = os.environ.get("WALRUSD_BUFFER_ROOT", "/tmp/walrusd-buffers")

lib.walrusd_runtime_version.restype = ctypes.c_void_p
lib.walrusd_runtime_init.argtypes = [ctypes.c_char_p, ctypes.c_int]
lib.walrusd_runtime_init.restype = ctypes.c_uint64
lib.walrusd_runtime_write.argtypes = [
    ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong
]
lib.walrusd_runtime_write.restype = ctypes.c_void_p
lib.walrusd_runtime_read.argtypes = [
    ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong
]
lib.walrusd_runtime_read.restype = ctypes.c_void_p
lib.walrusd_runtime_close.argtypes = [ctypes.c_uint64]
lib.walrusd_runtime_close.restype = ctypes.c_void_p
lib.walrusd_free.argtypes = [ctypes.c_void_p]


def consume(pointer):
    if not pointer:
        raise RuntimeError("walrusd returned a null response")
    envelope = json.loads(ctypes.string_at(pointer).decode())
    lib.walrusd_free(pointer)
    if not envelope["ok"]:
        raise RuntimeError(
            f'{envelope["error"]["class"]}: {envelope["error"]["message"]}'
        )
    return envelope.get("result")


consume(lib.walrusd_runtime_version())
init_request = json.dumps({
    "owner": "api-pod-7",
    "config": {
        "request_timeout_ms": 20_000,
        "write_buffer_root_path": buffer_root,
    },
}).encode()
handle = lib.walrusd_runtime_init(init_request, len(init_request))
if handle == 0:
    raise RuntimeError("walrusd_runtime_init failed")

descriptor = {
    "database_id": "users/user_1a4b",
    "storage": {"provider": "file", "file_root": root},
    "credentials": {},
}
deadline = int(time.time() * 1000) + 20_000
write_request = json.dumps({
    "descriptor": descriptor,
    "idempotency_key": "create-event-42",
    "statements": [
        {"sql": "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)"},
        {"sql": "INSERT INTO events (id, body) VALUES (?, ?)", "params": [42, "hello from walrusd"]},
    ],
}).encode()
result = consume(lib.walrusd_runtime_write(
    handle, write_request, len(write_request), deadline))
txid = result["txid"]

read_request = json.dumps({
    "descriptor": descriptor,
    "sql": "SELECT body FROM events WHERE id = ?",
    "params": [42],
}).encode()
result = consume(lib.walrusd_runtime_read(
    handle, read_request, len(read_request), deadline))
body = result["rows"][0]["body"]

print(f"Python (C ABI) durable at txid {txid}")
print(f"Python (C ABI) read: {body}")
consume(lib.walrusd_runtime_close(handle))
