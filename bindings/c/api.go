// Package c exposes the narrow, versioned C ABI over the walrusd Go core
// (spec §11). It is data-oriented: serialized request/response envelopes,
// no raw SQLite pointers ever cross this boundary. JavaScript code cannot
// bypass the lease or flush protocol (spec §18).
package c

/*
#include <stdint.h>
#include <stdlib.h>

static void walrusd_c_free(void* p) { free(p); }
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"

	"walrusd/walrusderr"
)

// ProtocolVersion is the envelope protocol version. Additive changes only
// within a major version (spec §11).
const ProtocolVersion = 1

// envelope is the common request/response wrapper.
type envelope struct {
	APIVersion int             `json:"api_version"`
	Op         string          `json:"op,omitempty"`
	OK         bool            `json:"ok"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      *errPayload     `json:"error,omitempty"`
}

type errPayload struct {
	Class        string `json:"class"`
	Message      string `json:"message"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}

// RuntimeHandle is an opaque handle to an initialized runtime, created by
// the host and passed back on every call.
type RuntimeHandle = uint64

var (
	handleMu   sync.Mutex
	nextHandle RuntimeHandle = 1
	runtimes                 = map[RuntimeHandle]runtimeEntry{}
)

type runtimeEntry struct {
	rt interface {
		WithReadBytes(ctx handledCtx, req []byte) ([]byte, error)
		WithWriteBytes(ctx handledCtx, req []byte) ([]byte, error)
		ReadDSNBytes(ctx handledCtx, req []byte) ([]byte, error)
		Close() error
	}
}

// handledCtx carries a deadline in ms since epoch; the Go core maps it to
// context.WithDeadline (spec §11: cancellation/deadline information).
type handledCtx struct {
	DeadlineMs int64 `json:"deadline_ms,omitempty"`
}

// walrusd_runtime_write executes one write batch. request_bytes is a JSON
// WriteRequest; the response is a JSON envelope.
//
//export walrusd_runtime_write
func walrusd_runtime_write(h C.uint64_t, requestBytes *C.char, n C.int, deadlineMs C.longlong) *C.char {
	return dispatch(h, "write", requestBytes, n, deadlineMs)
}

// walrusd_runtime_read executes one read batch.
//
//export walrusd_runtime_read
func walrusd_runtime_read(h C.uint64_t, requestBytes *C.char, n C.int, deadlineMs C.longlong) *C.char {
	return dispatch(h, "read", requestBytes, n, deadlineMs)
}

// walrusd_runtime_read_dsn registers the read VFS for a descriptor and
// returns the DSN as a JSON envelope. The host opens it with its OWN
// SQLite (bun:sqlite) to read through litestream VFS natively.
//
//export walrusd_runtime_read_dsn
func walrusd_runtime_read_dsn(h C.uint64_t, requestBytes *C.char, n C.int, deadlineMs C.longlong) *C.char {
	return dispatch(h, "read_dsn", requestBytes, n, deadlineMs)
}

// walrusd_runtime_version returns {"api_version":N,"core_version":"..."}.
//
//export walrusd_runtime_version
func walrusd_runtime_version() *C.char {
	res, _ := json.Marshal(map[string]any{
		"api_version":  ProtocolVersion,
		"core_version": CoreVersion,
	})
	return cString(mustEnvelopeOK(res))
}

// walrusd_runtime_close releases a runtime handle.
//
//export walrusd_runtime_close
func walrusd_runtime_close(h C.uint64_t) *C.char {
	handleMu.Lock()
	entry, ok := runtimes[RuntimeHandle(h)]
	if ok {
		delete(runtimes, RuntimeHandle(h))
	}
	handleMu.Unlock()
	if !ok {
		return cString(errEnvelope(errPayload{Class: "DB_INVALID_ARGUMENT", Message: "unknown runtime handle"}))
	}
	if err := entry.rt.Close(); err != nil {
		return cString(errEnvelope(errPayload{Class: "DB_REMOTE_UNAVAILABLE", Message: err.Error()}))
	}
	return cString(mustEnvelopeOK(nil))
}

// walrusd_free frees memory returned by this ABI. Exported symbol is
// walrusd_free; it delegates to the C free.
//
//export walrusd_free
func walrusd_free(p *C.char) {
	if p != nil {
		C.walrusd_c_free(unsafe.Pointer(p))
	}
}

func dispatch(h C.uint64_t, op string, requestBytes *C.char, n C.int, deadlineMs C.longlong) *C.char {
	handleMu.Lock()
	entry, ok := runtimes[RuntimeHandle(h)]
	handleMu.Unlock()
	if !ok {
		return cString(errEnvelope(errPayload{Class: "DB_INVALID_ARGUMENT", Message: "unknown runtime handle"}))
	}
	req := C.GoBytes(unsafe.Pointer(requestBytes), n)
	ctx := handledCtx{DeadlineMs: int64(deadlineMs)}

	var (
		res []byte
		err error
	)
	switch op {
	case "write":
		res, err = entry.rt.WithWriteBytes(ctx, req)
	case "read":
		res, err = entry.rt.WithReadBytes(ctx, req)
	case "read_dsn":
		res, err = entry.rt.ReadDSNBytes(ctx, req)
	}
	if err != nil {
		return cString(errEnvelopeFromClassified(err))
	}
	return cString(mustEnvelopeOK(res))
}

func mustEnvelopeOK(result json.RawMessage) []byte {
	b, err := json.Marshal(envelope{APIVersion: ProtocolVersion, OK: true, Result: result})
	if err != nil {
		panic(fmt.Sprintf("walrusd/c: marshal envelope: %v", err))
	}
	return b
}

func errEnvelope(e errPayload) []byte {
	b, _ := json.Marshal(envelope{APIVersion: ProtocolVersion, OK: false, Error: &e})
	return b
}

func errEnvelopeFromClassified(err error) []byte {
	e := errPayload{Class: string(walrusderr.ClassOf(err)), Message: err.Error()}
	if e.Class == "" {
		e.Class = string(walrusderr.ClassRemoteUnavailable)
	}
	if hint, ok := err.(walrusderr.RetryAfterHint); ok {
		if ms, ok := hint.RetryAfter(); ok {
			e.RetryAfterMs = ms
		}
	}
	return errEnvelope(e)
}

func cString(b []byte) *C.char {
	p := C.malloc(C.size_t(len(b) + 1))
	if p == nil {
		return nil
	}
	buf := (*[1 << 30]byte)(unsafe.Pointer(p))[:len(b)+1]
	copy(buf, b)
	buf[len(b)] = 0
	return (*C.char)(p)
}
