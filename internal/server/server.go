package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/itsramhere/tidepool/internal/auth"

	"github.com/coder/websocket"
	"github.com/itsramhere/tidepool/internal/replication"
	"github.com/itsramhere/tidepool/internal/schema"
	"github.com/itsramhere/tidepool/internal/storage"
)

// Server wires the config, schema config, store and committer together and
// serves the WebSocket sync endpoint.
type Server struct {
	cfg     *Config
	schemas *schema.Config
	store   storage.Store
	comm    *committer
	log     *slog.Logger

	ctx       context.Context
	cancel    context.CancelFunc
	pullers   *sync.WaitGroup
	replicate http.Handler
	stopOnce  sync.Once
}

// New builds a server and starts its committer goroutine. When peers are
// configured it also mounts the replication feed endpoint and starts one
// puller per peer. Call Stop to shut it down.
func New(cfg *Config, schemas *schema.Config, store storage.Store, log *slog.Logger) (*Server, error) {
	comm, err := newCommitter(cfg, schemas, store, log)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg: cfg, schemas: schemas, store: store, comm: comm, log: log,
		ctx: ctx, cancel: cancel, pullers: &sync.WaitGroup{},
	}
	go comm.run()

	if len(cfg.Peers) > 0 {
		secrets := make([]string, 0, len(cfg.Peers))
		peers := make([]replication.PeerConfig, 0, len(cfg.Peers))
		for _, p := range cfg.Peers {
			secrets = append(secrets, p.Secret)
			peers = append(peers, replication.PeerConfig{ID: p.ID, URL: p.URL, Secret: p.Secret})
		}
		s.replicate = replication.Handler(comm, store, secrets, log)
		puller := replication.NewPuller(peers, store, comm, log)
		puller.Metrics = comm.metrics
		for _, peer := range peers {
			comm.metrics.ReplicationLag.WithLabelValues(peer.ID).Set(0)
		}
		s.pullers.Add(1)
		go func() {
			defer s.pullers.Done()
			puller.Run(ctx)
		}()
	}
	return s, nil
}

// Handler returns the HTTP handler serving /v1/sync, /v1/replicate (when
// peers are configured) and /healthz.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sync", s.handleSync)
	mux.Handle("GET /metrics", s.comm.metrics.Handler())
	if s.replicate != nil {
		mux.Handle("GET /v1/replicate", s.replicate)
	}
	if s.cfg.DevMode {
		mux.HandleFunc("GET /dev/token", s.handleDevToken)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// handleDevToken mints a token for ?uid= without authentication. Only
// mounted when dev_mode is set in the config — demo tooling, never for
// networks others can reach.
func (s *Server) handleDevToken(w http.ResponseWriter, r *http.Request) {
	// Demo pages run on a different origin (vite), so this dev-only endpoint
	// allows any cross-origin GET. Never enable dev_mode in production.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	uid := r.URL.Query().Get("uid")
	if uid == "" {
		http.Error(w, "uid is required", http.StatusBadRequest)
		return
	}
	token, err := auth.Issue(s.cfg.JWTSecret, uid, 24*time.Hour)
	if err != nil {
		http.Error(w, "issue token", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"token": token, "uid": uid}); err != nil {
		s.log.Debug("encode dev token", slog.Any("err", err))
	}
}

// handleSync upgrades the connection and serves the session. The reader runs
// on this handler goroutine; the writer runs on its own goroutine.
func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	// Browsers from other origins connect to a Tidepool node; the token in
	// the hello frame is the authentication, so the origin check is relaxed
	// deliberately.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
	if err != nil {
		s.log.Debug("websocket accept failed", slog.Any("err", err))
		return
	}
	s.comm.metrics.Sessions.Inc()
	defer s.comm.metrics.Sessions.Dec()
	sess := newSession(s, conn)
	go sess.writeLoop()
	sess.readLoop()
}

// Stop shuts the pullers down, then the committer gracefully, then closes
// the store. Requests still queued when Stop is called are processed first;
// the request channel itself is never closed, so dying sessions can always
// deliver their cleanup.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.cancel()
		s.pullers.Wait()
		s.comm.closed.Store(true)
		close(s.comm.stopped)
		<-s.comm.done
		if err := s.store.Close(); err != nil {
			s.log.Error("close store", slog.Any("err", err))
		}
	})
}
