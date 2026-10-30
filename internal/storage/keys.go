package storage

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/itsramhere/tidepool/internal/crdt"
)

// Key layout (PLAN.md §4.8): '/'-separated parts; validated collection, doc
// and actor IDs never contain '/', so prefix ranges are unambiguous.
//
//	o/{coll}/{doc}/{docSeq:%020d}  op JSON, per-doc commit order
//	d/{coll}/{doc}                 DocMeta JSON
//	s/{coll}/{doc}                 snapshot JSON
//	f/{feedSeq:%020d}              op JSON, node-wide feed order
//	a/{actor}                      uid that owns this actor
//	p/{peerID}                     last peer feed seq applied
//	m/{key}                        raw metadata ("m/node" = node ID)
//
// Zero-padded seqs keep lexicographic key order equal to numeric order.

func opKey(k crdt.DocKey, docSeq uint64) []byte {
	return []byte("o/" + k.Coll + "/" + k.Doc + "/" + fmt.Sprintf("%020d", docSeq))
}

func docOpsPrefix(k crdt.DocKey) []byte {
	return []byte("o/" + k.Coll + "/" + k.Doc + "/")
}

func metaKey(k crdt.DocKey) []byte {
	return []byte("d/" + k.Coll + "/" + k.Doc)
}

func docsPrefix(coll string) []byte {
	return []byte("d/" + coll + "/")
}

func snapshotKey(k crdt.DocKey) []byte {
	return []byte("s/" + k.Coll + "/" + k.Doc)
}

func feedKey(feedSeq uint64) []byte {
	return []byte("f/" + fmt.Sprintf("%020d", feedSeq))
}

func feedPrefix() []byte { return []byte("f/") }

// parseDocSeq extracts the trailing %020d docSeq from an o/ key.
func parseDocSeq(key []byte) (uint64, error) {
	if len(key) < 20 {
		return 0, fmt.Errorf("op key %q too short", key)
	}
	seq, err := strconv.ParseUint(string(key[len(key)-20:]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse docSeq from %q: %w", key, err)
	}
	return seq, nil
}

// parseFeedSeq extracts the %020d feedSeq from an f/ key.
func parseFeedSeq(key []byte) (uint64, error) {
	rest := bytes.TrimPrefix(key, feedPrefix())
	if len(rest) != 20 {
		return 0, fmt.Errorf("feed key %q malformed", key)
	}
	seq, err := strconv.ParseUint(string(rest), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse feedSeq from %q: %w", key, err)
	}
	return seq, nil
}

// prefixUpperBound returns the smallest key greater than every key with the
// given prefix (the prefix with its last byte incremented), for use as an
// exclusive iterator upper bound.
func prefixUpperBound(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0x00 {
			return end
		}
	}
	return nil // prefix is all 0xFF: no exclusive upper bound exists
}
