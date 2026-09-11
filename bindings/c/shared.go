package c

// Shared-library build entry. Build with:
//
//	go build -buildmode=c-shared -tags vfs -o libwalrusd.dylib ./bindings/c
//
// The host (Node-API addon) loads the library and resolves the exported
// symbols: walrusd_runtime_write, walrusd_runtime_read,
// walrusd_runtime_version, walrusd_runtime_close, walrusd_free.
