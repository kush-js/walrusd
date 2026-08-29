// Package c exposes the narrow, versioned C ABI over the WALrus Go core
// (spec §11). It is data-oriented: serialized request/response envelopes,
// no raw SQLite pointers ever cross this boundary. JavaScript code cannot
// bypass the lease or flush protocol (spec §18).
package c

/*
#include <stdlib.h>

static void walrus_c_free(void* p) { free(p); }
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"

	"walrus/walruserr"
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
		Close() error
	}
}

// handledCtx carries a deadline in ms since epoch; the Go core maps it to
// context.WithDeadline (spec §11: cancellation/deadline information).
type handledCtx struct {
	DeadlineMs int64 `json:"deadline_ms,omitempty"`
}

// walrus_runtime_write executes one write batch. request_bytes is a JSON
// WriteRequest; the response is a JSON envelope.
//
//export walrus_runtime_write
func walrus_runtime_write(h C.uint64_t, requestBytes *C.char, n C.int, deadlineMs C.longlong) *C.char {
	return dispatch(h, "write", requestBytes, n, deadlineMs)
}

// walrus_runtime_read executes one read batch.
//
//export walrus_runtime_read
func walrus_runtime_read(h C.uint64_t, requestBytes *C.char, n C.int, deadlineMs C.longlong) *C.char {
	return dispatch(h, "read", requestBytes, n, deadlineMs)
}

// walrus_runtime_version returns {"api_version":N,"core_version":"..."}.
//
//export walrus_runtime_version
func walrus_runtime_version() *C.char {
	res, _ := json.Marshal(map[string]any{
		"api_version":  ProtocolVersion,
		"core_version": CoreVersion,
	})
	return cString(mustEnvelopeOK(res))
}

// walrus_runtime_close releases a runtime handle.
//
//export walrus_runtime_close
func walrus_runtime_close(h C.uint64_t) *C.char {
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

// walrus_free frees memory returned by this ABI. Exported symbol is
// walrus_free; it delegates to the C free.
//
//export walrus_free
func walrus_free(p *C.char) {
	if p != nil {
		C.walrus_c_free(unsafe.Pointer(p))
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
	}
	if err != nil {
		return cString(errEnvelopeFromClassified(err))
	}
	return cString(mustEnvelopeOK(res))
}

func mustEnvelopeOK(result json.RawMessage) []byte {
	b, err := json.Marshal(envelope{APIVersion: ProtocolVersion, OK: true, Result: result})
	if err != nil {
		panic(fmt.Sprintf("walrus/c: marshal envelope: %v", err))
	}
	return b
}

func errEnvelope(e errPayload) []byte {
	b, _ := json.Marshal(envelope{APIVersion: ProtocolVersion, OK: false, Error: &e})
	return b
}

func errEnvelopeFromClassified(err error) []byte {
	e := errPayload{Class: string(walruserr.ClassOf(err)), Message: err.Error()}
	if e.Class == "" {
		e.Class = string(walruserr.ClassRemoteUnavailable)
	}
	if hint, ok := err.(walruserr.RetryAfterHint); ok {
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
