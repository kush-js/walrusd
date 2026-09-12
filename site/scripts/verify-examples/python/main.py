import ctypes
import json
import os

lib = ctypes.CDLL(os.environ["WALRUSD_LIBRARY"])
buffer_root = os.environ.get("WALRUSD_BUFFER_ROOT", "/tmp/walrusd-buffers")


class WalrusdError(RuntimeError):
    def __init__(self, payload):
        super().__init__(payload["message"])
        self.code = payload["class"]
        self.retry_after_ms = payload.get("retry_after_ms")


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
        raise WalrusdError(envelope["error"])
    return envelope.get("result")


def create_runtime():
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
    return handle


def close_runtime(handle):
    consume(lib.walrusd_runtime_close(handle))

import time

root = os.environ.get("WALRUSD_EXAMPLE_ROOT", "/tmp/walrusd-example")

descriptor = {
    "database_id": "users/user_1a4b",
    "storage": {"provider": "file", "file_root": root},
    "credentials": {},
}


def read_value(handle, descriptor, deadline):
    request = json.dumps({
        "descriptor": descriptor,
        "sql": "SELECT body FROM events WHERE id = ?",
        "params": [1],
    }).encode()
    result = consume(lib.walrusd_runtime_read(
        handle, request, len(request), deadline))
    return result["rows"][0]["body"]


def write_txid(handle, descriptor, deadline):
    request = json.dumps({
        "descriptor": descriptor,
        "idempotency_key": "create-event-42",
        "statements": [
            {"sql": "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)"},
            {"sql": "INSERT INTO events (id, body) VALUES (?, ?)", "params": [1, "hello from walrusd"]},
        ],
    }).encode()
    result = consume(lib.walrusd_runtime_write(
        handle, request, len(request), deadline))
    return result["txid"]


def should_retry(error):
    if error.code == "DB_FLUSH_FAILED":
        # safe: retry with the same idempotency key
        return True
    if error.code == "DB_BUSY":
        return error.retry_after_ms is not None
    return False


if __name__ == "__main__":
    handle = create_runtime()
    deadline = int(time.time() * 1000) + 20_000
    try:
        txid = write_txid(handle, descriptor, deadline)
        body = read_value(handle, descriptor, deadline)
        print(f"Python (C ABI) durable at txid {txid}")
        print(f"Python (C ABI) read: {body}")
    finally:
        close_runtime(handle)
