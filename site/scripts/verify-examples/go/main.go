//go:build verify_examples

package main

import (
    "context"

    "walrusd/lease"
    "walrusd/runtime"
)

func main() {
    ctx := context.Background()

    // Leases live in Redis/Valkey shared by every API instance (one small
    // instance with persistence/AOF is enough). Memory store is dev-only:
    // it serializes within this process but not across instances.
    store, err := lease.NewRedisStore(ctx, lease.RedisOptions{
        Addr: "127.0.0.1:6379",
    })
    if err != nil {
        panic(err)
    }
    defer store.Close()

    rt, err := runtime.New(store, "api-pod-7", runtime.DefaultConfig())
    if err != nil {
        panic(err)
    }
    _ = rt
}
