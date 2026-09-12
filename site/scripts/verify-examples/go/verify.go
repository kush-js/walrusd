//go:build verify_examples

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
)

func init() {
	root := os.Getenv("WALRUSD_EXAMPLE_ROOT")
	bufferRoot := os.Getenv("WALRUSD_BUFFER_ROOT")
	if root == "" || bufferRoot == "" {
		panic("WALRUSD_EXAMPLE_ROOT and WALRUSD_BUFFER_ROOT are required")
	}

	ctx := context.Background()
	cfg := runtime.DefaultConfig()
	cfg.Litestream.WriteBufferRootPath = bufferRoot
	rt, err := runtime.New(lease.NewMemoryStore(), "api-pod-7", cfg)
	if err != nil {
		panic(err)
	}
	defer rt.Close()

	descriptor := runtime.DatabaseDescriptor{
		DatabaseID: "users/user_1a4b",
		Storage: litestream.Profile{
			Provider: "file",
			FileRoot: root,
		},
		Credentials: runtime.StaticCredentials{},
	}

	result, err := rt.WithWrite(ctx, descriptor, "create-event-42", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx,
			`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, body TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx,
			`INSERT INTO events (id, body) VALUES (?, ?)`, 42, "hello from walrusd")
		return err
	})
	if err != nil {
		panic(err)
	}

	var body string
	if err := rt.WithRead(ctx, descriptor, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx,
			`SELECT body FROM events WHERE id = ?`, 42).Scan(&body)
	}); err != nil {
		panic(err)
	}

	fmt.Printf("Go durable at txid %s\n", result.TXID)
	fmt.Printf("Go read: %s\n", body)
}
