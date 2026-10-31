package replication

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/storage"
)

const (
	// pullBatchSize bounds one replicatedOpsReq sent to the committer.
	pullBatchSize = 500
	// readIdle is the per-read timeout: a quiet feed is reconnected so the
	// cursor (committed with each batch) stays fresh.
	readIdle = 2 * time.Second
	// dialRetry is the delay between dial attempts to an unreachable peer.
	dialRetry = 500 * time.Millisecond
)

// CommitterSink is the puller's view of the committer: replicated ops are
// applied there (no rule checks — the origin node decided), the batch also
// persists the peer cursor, and local subscribers are broadcast to.
type CommitterSink interface {
	Replicate(peer string, ops []storage.StoredOp, lastFeedSeq uint64) error
}

// Puller keeps one node in sync with its configured peers: a goroutine per
// peer streams the peer's feed over WebSocket and hands batches to the
// committer. Cursors live in the store (p/<peerID>), committed atomically
// with the ops they cover.
type Puller struct {
	peers  []PeerConfig
	store  storage.Store
	comm   CommitterSink
	client *http.Client
	log    *slog.Logger
}

// PeerConfig describes one replication peer: its ID (cursor key) and origin
// URL (scheme://host[:port]); the puller appends /v1/replicate itself.
type PeerConfig struct {
	ID     string
	URL    string
	Secret string
}

// NewPuller builds a puller for the given peers. Run starts the goroutines.
func NewPuller(peers []PeerConfig, store storage.Store, comm CommitterSink, log *slog.Logger) *Puller {
	return &Puller{
		peers:  peers,
		store:  store,
		comm:   comm,
		client: &http.Client{Timeout: 10 * time.Second},
		log:    log,
	}
}

// Run blocks until ctx is done, pulling from every peer.
func (p *Puller) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, peer := range p.peers {
		wg.Add(1)
		go func(peer PeerConfig) {
			defer wg.Done()
			p.pullLoop(ctx, peer)
		}(peer)
	}
	wg.Wait()
}

func (p *Puller) pullLoop(ctx context.Context, peer PeerConfig) {
	cursor := p.loadCursor(peer.ID)
	for ctx.Err() == nil {
		if _, err := p.pullOnce(ctx, peer, &cursor); err != nil && ctx.Err() == nil {
			p.log.Debug("peer pull failed", slog.String("peer", peer.ID), slog.Any("err", err))
			sleepCtx(ctx, dialRetry)
		}
	}
}

// pullOnce connects to the peer's feed from the cursor and streams batches
// to the committer until the connection dies or ctx is done. It reports how
// many ops were applied; the cursor advances only through committed batches.
func (p *Puller) pullOnce(ctx context.Context, peer PeerConfig, cursor *uint64) (int, error) {
	wsURL := wsFeedURL(peer.URL, *cursor)
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + peer.Secret}},
		HTTPClient: p.client,
	})
	if err != nil {
		return 0, fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20) // feed frames mirror op sizes

	applied := 0
	var batch []storage.StoredOp
	last := *cursor
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := p.comm.Replicate(peer.ID, batch, last); err != nil {
			// The batch was not committed: the cursor stays put and the
			// ops are re-pulled on reconnect.
			return fmt.Errorf("replicate %d ops to committer: %w", len(batch), err)
		}
		*cursor = last
		applied += len(batch)
		batch = batch[:0]
		return nil
	}

	for ctx.Err() == nil {
		rctx, cancelRead := context.WithTimeout(ctx, readIdle)
		_, data, err := conn.Read(rctx)
		cancelRead()
		if err != nil {
			// Idle timeout or closed stream: flush what we have, then let
			// the loop reconnect from the committed cursor.
			if ctx.Err() == nil && !isNormalClose(err) {
				p.log.Debug("peer feed ended", slog.String("peer", peer.ID), slog.Any("err", err))
			}
			if ferr := flush(); ferr != nil {
				return applied, ferr
			}
			return applied, nil
		}
		var frame feedFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			return applied, fmt.Errorf("decode feed frame: %w", err)
		}
		if frame.T != feedFrameType {
			return applied, fmt.Errorf("unexpected feed frame type %q", frame.T)
		}
		if frame.Op.FeedSeq <= *cursor {
			continue // already applied (cursor raced the stream restart)
		}
		batch = append(batch, frame.Op)
		last = frame.Op.FeedSeq
		if len(batch) >= pullBatchSize {
			if err := flush(); err != nil {
				return applied, err
			}
		}
	}
	if err := flush(); err != nil {
		return applied, err
	}
	return applied, nil
}

func (p *Puller) loadCursor(peerID string) uint64 {
	raw, ok, err := p.store.GetMeta("p/" + peerID)
	if err != nil || !ok || len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

// CursorBytes encodes a feed cursor for the p/<peerID> meta key.
func CursorBytes(seq uint64) []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, seq)
	return raw
}

func wsFeedURL(origin string, after uint64) string {
	ws := "ws" + strings.TrimPrefix(strings.TrimSuffix(origin, "/"), "http")
	return ws + "/v1/replicate?after=" + fmt.Sprint(after)
}

func isNormalClose(err error) bool {
	return websocket.CloseStatus(err) == websocket.StatusNormalClosure
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
