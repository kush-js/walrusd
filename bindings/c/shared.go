package c

// Shared-library build entry. Build with:
//
//	go build -buildmode=c-shared -tags vfs -o libwalrus.dylib ./bindings/c
//
// The host (Node-API addon) loads the library and resolves the exported
// symbols: walrus_runtime_write, walrus_runtime_read,
// walrus_runtime_version, walrus_runtime_close, walrus_free.
