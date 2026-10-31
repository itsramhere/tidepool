package server

import (
	"fmt"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/storage"
	"io"
	"log/slog"
	"testing"
)

// BenchmarkCommit measures complete committer groups (validation, Pebble commit,
// snapshots, metrics, ack and echo serialization), with one counter op/request.
func BenchmarkCommit(b *testing.B) {
	for _, sync := range []bool{false, true} {
		for _, size := range []int{1, 16, 256} {
			b.Run(fmt.Sprintf("sync=%t/batch=%d", sync, size), func(b *testing.B) {
				store, err := storage.Open(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := store.Close(); err != nil {
						b.Error(err)
					}
				})
				cfg := &Config{Fsync: "never"}
				if sync {
					cfg.Fsync = "always"
				}
				c, err := newCommitter(cfg, testSchemas, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
				if err != nil {
					b.Fatal(err)
				}
				c.reqs = make(chan request, 256)
				srv := &Server{comm: c, cfg: cfg, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
				sess := &session{srv: srv, writeCh: make(chan []byte, size*3), done: make(chan struct{}), auth: map[string]any{"uid": "bench"}}
				keys := make([]crdt.DocKey, size)
				for i := range keys {
					keys[i] = crdt.DocKey{Coll: "cards", Doc: fmt.Sprintf("d%d", i)}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					seq := uint64(i + 1)
					var first request
					for j := range size {
						op := crdt.Op{ID: crdt.OpID{Actor: "aaaaaaaaaaaaaaaa", Seq: seq}, Key: keys[j], Field: "votes", Kind: crdt.KindInc, Lamport: seq, Delta: 1}
						r := &pushReq{sess: sess, uid: "bench", actor: op.ID.Actor, req: uint64(j + 1), ops: []crdt.Op{op}}
						if j == 0 {
							first = r
						} else {
							c.reqs <- r
						}
					}
					c.handleGroup(first)
					if len(sess.writeCh) != size*2 {
						b.Fatal("missing acknowledgments/echoes")
					}
					for range size * 2 {
						<-sess.writeCh
					}
				}
				b.ReportMetric(float64(b.N*size)/b.Elapsed().Seconds(), "ops/s")
			})
		}
	}
}
