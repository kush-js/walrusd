// Package identity derives canonical, tenant-safe database IDs and object
// paths from trusted (organization_id, user_id) pairs. Nothing else may
// construct storage keys (spec §4).
package identity

import (
	"fmt"
	"net/url"
	"strings"
)

// DatabaseID is the canonical identifier "org/<org>/user/<user>".
type DatabaseID struct {
	OrganizationID string
	UserID         string
}

// NewDatabaseID builds a DatabaseID from trusted control-plane values.
// It rejects empty or path-unsafe components so a malformed tenant ID can
// never escape the object-storage prefix.
func NewDatabaseID(organizationID, userID string) (DatabaseID, error) {
	if organizationID == "" {
		return DatabaseID{}, fmt.Errorf("identity: organization_id is required")
	}
	if userID == "" {
		return DatabaseID{}, fmt.Errorf("identity: user_id is required")
	}
	for _, id := range []string{organizationID, userID} {
		if strings.ContainsAny(id, "/?#") || id != url.PathEscape(id) {
			return DatabaseID{}, fmt.Errorf("identity: id %q is not path-safe", id)
		}
	}
	return DatabaseID{OrganizationID: organizationID, UserID: userID}, nil
}

// String returns the canonical database ID: "org/<org>/user/<user>".
func (d DatabaseID) String() string {
	return "org/" + d.OrganizationID + "/user/" + d.UserID
}

// ParseDatabaseID validates a canonical database ID string. Use for
// defensive re-validation of descriptors received over the wire.
func ParseDatabaseID(s string) (DatabaseID, error) {
	org, rest, ok := strings.Cut(s, "/user/")
	if !ok {
		return DatabaseID{}, fmt.Errorf("identity: invalid database id %q", s)
	}
	orgID, ok := strings.CutPrefix(org, "org/")
	if !ok {
		return DatabaseID{}, fmt.Errorf("identity: invalid database id %q", s)
	}
	return NewDatabaseID(orgID, rest)
}

// StoragePrefix returns the object-storage prefix for a database under an
// organization root: "<root>/walrus/v1/users/<encoded-user-id>/" (spec §4).
func (d DatabaseID) StoragePrefix(rootPrefix string) string {
	prefix := strings.TrimSuffix(rootPrefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return prefix + "walrus/v1/users/" + url.PathEscape(d.UserID) + "/"
}

// LeaseKey returns the lease object key for this database.
func (d DatabaseID) LeaseKey(rootPrefix string) string {
	return d.StoragePrefix(rootPrefix) + "lease.json"
}

// ReplicaPrefix returns the Litestream replica prefix for this database.
func (d DatabaseID) ReplicaPrefix(rootPrefix string) string {
	return d.StoragePrefix(rootPrefix) + "replica"
}
