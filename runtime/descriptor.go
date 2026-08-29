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

// DatabaseDescriptor is issued by the Basemnt control plane (spec §11).
// Bucket names, replica URLs, object prefixes, credentials, and database
// IDs must NEVER come from end users.
type DatabaseDescriptor struct {
	OrganizationID string
	UserID         string
	Storage        litestream.Profile
	Credentials    CredentialSource
}

// DatabaseID returns the canonical database ID for this descriptor.
func (d DatabaseDescriptor) DatabaseID() (identity.DatabaseID, error) {
	return identity.NewDatabaseID(d.OrganizationID, d.UserID)
}
