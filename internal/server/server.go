package server

import (
	"log/slog"
	"net/http"

	"github.com/coder/websocket"
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
}

// New builds a server and starts its committer goroutine. Call Stop to shut
// it down.
func New(cfg *Config, schemas *schema.Config, store storage.Store, log *slog.Logger) (*Server, error) {
	comm, err := newCommitter(cfg, schemas, store, log)
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, schemas: schemas, store: store, comm: comm, log: log}
	go comm.run()
	return s, nil
}

// Handler returns the HTTP handler serving /v1/sync and /healthz.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/sync", s.handleSync)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
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
	sess := newSession(s, conn)
	go sess.writeLoop()
	sess.readLoop()
}

// Stop shuts the committer down gracefully and closes the store. Requests
// still queued when Stop is called are processed first; the request channel
// itself is never closed, so dying sessions can always deliver their
// cleanup.
func (s *Server) Stop() {
	s.comm.closed.Store(true)
	close(s.comm.stopped)
	<-s.comm.done
	if err := s.store.Close(); err != nil {
		s.log.Error("close store", slog.Any("err", err))
	}
}
