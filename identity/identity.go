// Package identity derives canonical, tenant-safe database IDs and object
// paths from trusted database IDs. Nothing else may construct storage keys
// (spec §4).
package identity

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

// DatabaseID is the caller's canonical database identifier: a relative,
// path-safe object-store key such as "user_1a4b", "users/u1", or
// "acme/agents/a7". WALrus imposes no structure on it; the object layout is
// simply "<root_prefix>/<database_id>/". Databases with different IDs are
// fully independent (separate lease and replica prefixes), so use distinct
// ID paths to partition databases however your product needs.
type DatabaseID struct {
	ID string
}

// NewDatabaseID validates a trusted database ID. It must be non-empty, use
// only path-safe segments, contain no "." or ".." segments, and have no
// leading/trailing/duplicate slashes — a malformed ID must never escape the
// configured root prefix.
func NewDatabaseID(id string) (DatabaseID, error) {
	if id == "" {
		return DatabaseID{}, fmt.Errorf("identity: database_id is required")
	}
	if strings.HasPrefix(id, "/") || strings.HasSuffix(id, "/") || strings.Contains(id, "//") {
		return DatabaseID{}, fmt.Errorf("identity: database_id %q is not a clean relative path", id)
	}
	for _, seg := range strings.Split(id, "/") {
		if seg == "." || seg == ".." {
			return DatabaseID{}, fmt.Errorf("identity: database_id %q must not contain %q segments", id, seg)
		}
		if strings.ContainsAny(seg, "/?#") || seg != url.PathEscape(seg) {
			return DatabaseID{}, fmt.Errorf("identity: database_id segment %q is not path-safe", seg)
		}
	}
	clean := path.Clean(id)
	if clean != id {
		return DatabaseID{}, fmt.Errorf("identity: database_id %q is not a clean relative path", id)
	}
	return DatabaseID{ID: id}, nil
}

// String returns the database ID verbatim.
func (d DatabaseID) String() string { return d.ID }

// ParseDatabaseID validates a database ID string. Use for defensive
// re-validation of descriptors received over the wire.
func ParseDatabaseID(s string) (DatabaseID, error) { return NewDatabaseID(s) }

// StoragePrefix returns the object-storage prefix for a database under a
// root: "<root_prefix>/<database_id>/" (spec §4).
func (d DatabaseID) StoragePrefix(rootPrefix string) string {
	prefix := strings.TrimSuffix(rootPrefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return prefix + d.ID + "/"
}

// LeaseKey returns the lease object key for this database.
func (d DatabaseID) LeaseKey(rootPrefix string) string {
	return d.StoragePrefix(rootPrefix) + "lease.json"
}

// ReplicaPrefix returns the Litestream replica prefix for this database.
func (d DatabaseID) ReplicaPrefix(rootPrefix string) string {
	return d.StoragePrefix(rootPrefix) + "replica"
}
