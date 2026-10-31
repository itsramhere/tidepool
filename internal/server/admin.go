package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/storage"
	"net/http"
	"net/http/pprof"
	"sort"
)

// AdminHandler must be served on the separate admin listener. An empty token
// disables access even when this handler is mounted by an embedding application.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/docs/{coll}", s.handleInspect)
	mux.HandleFunc("GET /admin/docs/{coll}/{doc}", s.handleInspect)
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.AdminToken == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.cfg.AdminToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

var errDocNotFound = errors.New("document or collection not found")

type inspectReq struct {
	coll, doc string
	done      chan inspectResult
}
type inspectResult struct {
	value any
	err   error
}

func (s *Server) inspect(ctx context.Context, coll, doc string) (any, error) {
	req := &inspectReq{coll: coll, doc: doc, done: make(chan inspectResult, 1)}
	select {
	case s.comm.reqs <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.comm.stopped:
		return nil, fmt.Errorf("server stopping")
	}
	select {
	case result := <-req.done:
		return result.value, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.comm.stopped:
		return nil, fmt.Errorf("server stopping")
	}
}

func (s *Server) handleInspect(w http.ResponseWriter, r *http.Request) {
	coll, doc := r.PathValue("coll"), r.PathValue("doc")
	id := doc
	if id == "" {
		id = "list"
	}
	if err := crdt.ValidateKey(crdt.DocKey{Coll: coll, Doc: id}); err != nil {
		http.Error(w, "invalid document key", 400)
		return
	}
	result, err := s.inspect(r.Context(), coll, doc)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errDocNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		s.log.Debug("encode inspection", "err", err)
	}
}

// inspectCommitted runs only on the committer after storage commits, copying
// maps into JSON before HTTP handlers can observe later mutations.
func (c *committer) inspectCommitted(r *inspectReq) inspectResult {
	sch, ok := c.schemas.Schema(r.coll)
	if !ok {
		return inspectResult{err: errDocNotFound}
	}
	if r.doc == "" {
		ids := []string{}
		if err := c.store.ListDocs(r.coll, func(id string) error { ids = append(ids, id); return nil }); err != nil {
			return inspectResult{err: err}
		}
		sort.Strings(ids)
		return inspectResult{value: struct {
			Coll string   `json:"coll"`
			Docs []string `json:"docs"`
		}{r.coll, ids}}
	}
	key := crdt.DocKey{Coll: r.coll, Doc: r.doc}
	if _, ok, err := c.store.DocMeta(key); err != nil {
		return inspectResult{err: err}
	} else if !ok {
		return inspectResult{err: errDocNotFound}
	}
	doc, _, err := storage.LoadDocument(c.store, key, sch)
	if err != nil {
		return inspectResult{err: err}
	}
	snap, err := doc.MarshalSnapshot()
	if err != nil {
		return inspectResult{err: err}
	}
	return inspectResult{value: struct {
		Key      crdt.DocKey             `json:"key"`
		Values   map[string]any          `json:"values"`
		VV       map[crdt.ActorID]uint64 `json:"vv"`
		Snapshot json.RawMessage         `json:"snapshot"`
	}{key, doc.Values(), doc.VV, snap}}
}
