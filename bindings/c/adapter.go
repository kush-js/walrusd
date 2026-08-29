package c

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"walrus/litestream"
	"walrus/runtime"
	"walrus/storage"
	"walrus/walruserr"
)

// Adapter adapts the Go runtime.Runtime to the byte-envelope ABI.
type Adapter struct {
	rt *runtime.Runtime
}

// NewAdapter builds an Adapter over a ConditionalStore-backed runtime.
func NewAdapter(store storage.ConditionalStore, owner string, cfg runtime.Config) (*Adapter, error) {
	rt, err := runtime.New(store, owner, cfg)
	if err != nil {
		return nil, err
	}
	return &Adapter{rt: rt}, nil
}

// WriteRequest is one batched mutation (spec §11: batch-oriented so one
// SQLite connection and one lease cover the whole operation).
type WriteRequest struct {
	Descriptor     descriptorJSON `json:"descriptor"`
	IdempotencyKey string         `json:"idempotency_key"`
	Statements     []statement    `json:"statements"`
}

// ReadRequest is one batched read.
type ReadRequest struct {
	Descriptor  descriptorJSON `json:"descriptor"`
	SQL         string         `json:"sql"`
	Params      []any          `json:"params,omitempty"`
	Consistency string         `json:"consistency,omitempty"` // remote_committed (default)
}

type descriptorJSON struct {
	OrganizationID string      `json:"organization_id"`
	UserID         string      `json:"user_id"`
	Storage        profileJSON `json:"storage"`
	Credentials    credsJSON   `json:"credentials"`
}

type profileJSON struct {
	Provider        string `json:"provider"`
	Endpoint        string `json:"endpoint,omitempty"`
	Region          string `json:"region,omitempty"`
	Bucket          string `json:"bucket,omitempty"`
	RootPrefix      string `json:"root_prefix,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	FileRoot        string `json:"file_root,omitempty"`
}

type credsJSON struct {
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
}

type statement struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params,omitempty"`
}

// WriteResult returns the flushed remote TXID for read-after-write (spec §9).
type WriteResult struct {
	TXID string `json:"txid,omitempty"`
}

// ReadResult returns rows as columnar JSON.
type ReadResult struct {
	Columns []string          `json:"columns"`
	Rows    []json.RawMessage `json:"rows"`
}

func (a *Adapter) WithWriteBytes(ctx handledCtx, req []byte) ([]byte, error) {
	var r WriteRequest
	if err := json.Unmarshal(req, &r); err != nil {
		return nil, walruserr.Wrap(walruserr.ClassInvalidArgument, "decode write request", err)
	}
	cctx, cancel := ctx.context()
	defer cancel()

	d := ddescriptor(r.Descriptor)
	res, err := a.rt.WithWrite(cctx, d, r.IdempotencyKey, func(conn *sql.Conn) error {
		for _, st := range r.Statements {
			if _, err := conn.ExecContext(cctx, st.SQL, st.Params...); err != nil {
				return fmt.Errorf("statement %q: %w", st.SQL, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(WriteResult{TXID: res.TXID})
}

func (a *Adapter) WithReadBytes(ctx handledCtx, req []byte) ([]byte, error) {
	var r ReadRequest
	if err := json.Unmarshal(req, &r); err != nil {
		return nil, walruserr.Wrap(walruserr.ClassInvalidArgument, "decode read request", err)
	}
	cctx, cancel := ctx.context()
	defer cancel()

	d := ddescriptor(r.Descriptor)
	var res ReadResult
	err := a.rt.WithRead(cctx, d, func(conn *sql.Conn) error {
		rows, err := conn.QueryContext(cctx, r.SQL, r.Params...)
		if err != nil {
			return err
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		res.Columns = cols
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			row := make(map[string]any, len(cols))
			for i, c := range cols {
				row[c] = normalize(vals[i])
			}
			b, err := json.Marshal(row)
			if err != nil {
				return err
			}
			res.Rows = append(res.Rows, b)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(res)
}

// Close releases runtime resources.
func (a *Adapter) Close() error { return nil }

func ddescriptor(d descriptorJSON) runtime.DatabaseDescriptor {
	return runtime.DatabaseDescriptor{
		OrganizationID: d.OrganizationID,
		UserID:         d.UserID,
		Storage:        runtimeProfile(d.Storage),
		Credentials: runtime.StaticCredentials{
			AccessKeyID:     d.Credentials.AccessKeyID,
			SecretAccessKey: d.Credentials.SecretAccessKey,
		},
	}
}

func runtimeProfile(p profileJSON) litestream.Profile {
	return litestream.Profile{
		Provider:        p.Provider,
		Endpoint:        p.Endpoint,
		Region:          p.Region,
		Bucket:          p.Bucket,
		RootPrefix:      p.RootPrefix,
		AccessKeyID:     p.AccessKeyID,
		SecretAccessKey: p.SecretAccessKey,
		FileRoot:        p.FileRoot,
	}
}

func (c handledCtx) context() (context.Context, context.CancelFunc) {
	if c.DeadlineMs > 0 {
		return context.WithDeadline(context.Background(), time.UnixMilli(c.DeadlineMs))
	}
	return context.WithCancel(context.Background())
}

// normalize converts driver values into JSON-safe equivalents.
func normalize(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return v
	}
}
