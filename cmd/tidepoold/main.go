// Command tidepoold runs a Tidepool sync node: it loads a YAML config, opens
// the durable store, and serves the WebSocket sync protocol, the admin API,
// and peer replication.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itsramhere/tidepool/internal/schema"
	"github.com/itsramhere/tidepool/internal/server"
	"github.com/itsramhere/tidepool/internal/storage"
)

func main() {
	configPath := flag.String("config", "", "path to the server YAML config (required)")
	flag.Parse()
	if *configPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := server.LoadConfig(*configPath)
	if err != nil {
		log.Error("load config", slog.Any("err", err))
		os.Exit(1)
	}
	schemas, err := schema.Load(cfg.RulesFile)
	if err != nil {
		log.Error("load rules", slog.Any("err", err))
		os.Exit(1)
	}
	// The store's durability per batch is decided by the committer from
	// cfg.Fsync; pebble v2 has no global no-sync option.
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		log.Error("open store", slog.Any("err", err))
		os.Exit(1)
	}

	srv, err := server.New(cfg, schemas, store, log)
	if err != nil {
		log.Error("start server", slog.Any("err", err))
		os.Exit(1)
	}

	ctx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()

	httpSrv := &http.Server{Addr: cfg.Listen, Handler: srv.Handler()}
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			log.Error("http shutdown", slog.Any("err", err))
		}
	}()

	log.Info("tidepoold listening",
		slog.String("addr", cfg.Listen),
		slog.String("node", cfg.NodeID),
		slog.String("fsync", cfg.Fsync),
	)
	err = httpSrv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server", slog.Any("err", err))
		os.Exit(1)
	}

	srv.Stop()
	log.Info("shutdown complete")
}
