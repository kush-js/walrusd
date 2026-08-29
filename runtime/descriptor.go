// Package runtime implements the WALrus Runtime surface (spec §11): WithRead
// and WithWrite. WithWrite owns the full write lifecycle — conditional lease
// acquisition, VFS write-mode enable, SQLite transaction, the mandatory
// flush-on-disable barrier, and conditional lease release. Callers must not
// acquire a lease or toggle litestream_write_enabled directly (spec §8).
package runtime

import (
	"walrus/identity"
	"walrus/litestream"
)

// CredentialSource resolves short-lived, org-scoped credentials at call time
// (spec §11: never persisted in JS objects, disk, logs, or addon caches).
type CredentialSource interface {
	// AccessKey returns a usable credential pair for the organization bucket.
	AccessKey() (keyID, secret string, err error)
}

// StaticCredentials is a CredentialSource holding one short-lived pair.
type StaticCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

func (c StaticCredentials) AccessKey() (string, string, error) {
	return c.AccessKeyID, c.SecretAccessKey, nil
}

// DatabaseDescriptor is issued by the control plane (spec §11). Bucket
// names, replica URLs, object prefixes, credentials, and database IDs must
// NEVER come from end users.
type DatabaseDescriptor struct {
	// DatabaseID is the caller's canonical database identifier — a
	// path-safe relative key such as "user_1a4b" or "acme/agents/a7".
	// Objects live at "<root_prefix>/<database_id>/". Use whatever
	// partitioning fits your product; WALrus imposes no structure.
	DatabaseID  string
	Storage     litestream.Profile
	Credentials CredentialSource
}

// Identity validates and returns the canonical database ID.
func (d DatabaseDescriptor) Identity() (identity.DatabaseID, error) {
	return identity.NewDatabaseID(d.DatabaseID)
}
