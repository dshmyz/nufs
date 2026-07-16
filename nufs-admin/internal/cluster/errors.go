// Package cluster provides cluster registry and client for proxying requests.
package cluster

import "errors"

// ErrStoreNotConfigured is returned when DB store is not available.
var ErrStoreNotConfigured = errors.New("cluster store (DB) is not configured")

// ErrClusterExists is returned when adding a cluster that already exists.
var ErrClusterExists = errors.New("cluster already exists")

// ErrClusterNotFound is returned when cluster is not found.
var ErrClusterNotFound = errors.New("cluster not found")