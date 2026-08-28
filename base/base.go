// Package base provides canonical database-ID and object-key construction
// (spec §5). All keys derive from trusted tenant context only.
package base

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// DatabaseID is the canonical stable identifier: org/<org>/user/<user>.
type DatabaseID string

// NewDatabaseID builds the canonical database ID exactly once.
func NewDatabaseID(orgID, userID string) (DatabaseID, error) {
	if orgID == "" || userID == "" {
		return "", fmt.Errorf("base: organization and user IDs are required")
	}
	if strings.ContainsAny(orgID+userID, "/ \t\n") {
		return "", fmt.Errorf("base: IDs must not contain slashes or whitespace")
	}
	return DatabaseID("org/" + orgID + "/user/" + userID), nil
}

// ParseDatabaseID validates an already-constructed canonical database ID.
func ParseDatabaseID(id string) (DatabaseID, error) {
	org, user, ok := strings.Cut(string(id), "/user/")
	if !ok || !strings.HasPrefix(org, "org/") || org == "org/" || user == "" ||
		strings.ContainsAny(org[4:]+user, "/ \t\n") {
		return "", fmt.Errorf("base: invalid canonical database ID %q", id)
	}
	return DatabaseID(id), nil
}

// Prefix returns the object prefix for the database inside an org bucket:
// basemnt/v1/databases/<encoded-user-id>/ where the encoded user ID is the
// SHA-256 of the full database ID — canonical and path-safe by construction.
func (d DatabaseID) Prefix() string {
	sum := sha256.Sum256([]byte(d))
	return "basemnt/v1/databases/" + hex.EncodeToString(sum[:])
}

// Keys are the well-known objects inside a database prefix.
func (d DatabaseID) OwnershipKey() string { return d.Prefix() + "/ownership/current.json" }
func (d DatabaseID) CommitKey() string    { return d.Prefix() + "/commits/current.json" }

// LTXKey returns the immutable LTX object key for a level/TXID range.
func (d DatabaseID) LTXKey(level int, minTXID, maxTXID string) string {
	return fmt.Sprintf("%s/ltx/%04x/%s-%s.ltx", d.Prefix(), level, minTXID, maxTXID)
}
