// Package replication implements multi-master replication between Tidepool
// nodes: the peer feed endpoint that streams a node's committed ops, and the
// puller that applies a peer's feed locally.
package replication
