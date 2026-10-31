package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/storage"
)

// FeedNotify is one committed op handed to live feed subscribers by the
// committer, in feed order. FeedSeq is the node-wide feed position assigned
// at commit.
type FeedNotify struct {
	Op      crdt.Op
	FeedSeq uint64
}

// FeedHub is the committer's live-feed subscription surface. Subscribe
// BEFORE replaying history, then drop notifications whose FeedSeq the
// replay already covered — no gap, no duplicate.
type FeedHub interface {
	AddFeedSub() chan FeedNotify
	RemoveFeedSub(ch chan FeedNotify)
}

// feedFrame is one server → peer WebSocket frame: a stored op in the node's
// feed. DocSeq is meaningful only on the originating node; receivers assign
// their own per-doc seqs on commit.
type feedFrame struct {
	T  string           `json:"t"`
	Op storage.StoredOp `json:"op"`
}

const feedFrameType = "feed"

// Handler serves GET /v1/replicate?after=N: an authenticated peer connects,
// the node replays every feed op with FeedSeq > after, then streams live
// from the committer's notifications. A slow peer (its notify buffer fills)
// is disconnected and resumes from its own cursor on reconnect.
func Handler(hub FeedHub, store storage.Store, peerSecrets []string, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r, peerSecrets) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		after, err := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		if err != nil {
			http.Error(w, "bad after", http.StatusBadRequest)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			log.Debug("replicate accept failed", slog.Any("err", err))
			return
		}
		serveFeed(conn, hub, store, after, log)
	})
}

// authorized checks the Bearer peer secret. When the node configures no
// non-empty peer secrets (single-node or test setups), any caller is
// allowed.
func authorized(r *http.Request, peerSecrets []string) bool {
	var configured []string
	for _, s := range peerSecrets {
		if s != "" {
			configured = append(configured, s)
		}
	}
	if len(configured) == 0 {
		return true
	}
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, prefix) {
		return false
	}
	token := strings.TrimPrefix(auth, prefix)
	for _, s := range configured {
		if token == s {
			return true
		}
	}
	return false
}

// serveFeed runs one peer connection: register for live notifications,
// replay history from the store, then stream notifications newer than the
// replayed history.
func serveFeed(conn *websocket.Conn, hub FeedHub, store storage.Store, after uint64, log *slog.Logger) {
	defer conn.CloseNow()
	ch := hub.AddFeedSub()
	defer hub.RemoveFeedSub(ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Detect a closed or dead peer: any read error ends the stream.
	go func() {
		conn.SetReadLimit(1 << 10) // peers send nothing; the close frame suffices
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				cancel()
				return
			}
		}
	}()

	// History replay.
	historyLast := after
	for {
		ops, err := store.Feed(historyLast, 500)
		if err != nil {
			log.Error("feed replay", slog.Any("err", err))
			return
		}
		for _, so := range ops {
			if err := writeFeedFrame(ctx, conn, so); err != nil {
				log.Debug("feed write failed", slog.Any("err", err))
				return
			}
			historyLast = so.FeedSeq
		}
		if len(ops) < 500 {
			break
		}
	}

	// Live tail: drop notifications the replay already covered.
	for {
		select {
		case n, ok := <-ch:
			if !ok {
				return // buffer overflow: disconnect; the peer resumes from its cursor
			}
			if n.FeedSeq <= historyLast {
				continue
			}
			so := storage.StoredOp{Op: n.Op, FeedSeq: n.FeedSeq}
			if err := writeFeedFrame(ctx, conn, so); err != nil {
				log.Debug("feed write failed", slog.Any("err", err))
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func writeFeedFrame(ctx context.Context, conn *websocket.Conn, so storage.StoredOp) error {
	data, err := json.Marshal(feedFrame{T: feedFrameType, Op: so})
	if err != nil {
		return fmt.Errorf("marshal feed frame: %w", err)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, data)
}
