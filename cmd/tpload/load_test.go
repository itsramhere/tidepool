package main

import (
	"context"
	"github.com/itsramhere/tidepool/internal/auth"
	"github.com/itsramhere/tidepool/internal/schema"
	"github.com/itsramhere/tidepool/internal/server"
	"github.com/itsramhere/tidepool/internal/storage"
	"io"
	"log/slog"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHistogramBounds(t *testing.T) {
	var h histogram
	if h.quantile(.5) != 0 {
		t.Fatal("empty histogram")
	}
	for i := 1; i <= 100; i++ {
		h.add(time.Duration(i) * time.Millisecond)
	}
	for _, q := range []float64{.5, .95, .99} {
		got := h.quantile(q)
		want := math.Ceil(q * 100)
		if got < want || got > want*1.011 {
			t.Fatalf("q=%f got %f want within 1%% of %f", q, got, want)
		}
	}
}

func TestLoadReportsRealAcksAndRemoteFanout(t *testing.T) {
	schemas, err := schema.Parse([]byte("collections:\n  load:\n    fields:\n      run: register\n      sentAt: register\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &server.Config{NodeID: "load-test", JWTSecret: "secret", Fsync: "never", MaxOpsPerPush: 1000, SessionSendBuffer: 4096}
	srv, err := server.New(cfg, schemas, storage.NewMemoryStore(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer srv.Stop()
	defer ts.Close()
	token, err := auth.Issue("secret", "load", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := run(ctx, options{URL: "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/sync", Token: token, Clients: 3, Docs: 2, Rate: 20, Duration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if result.Errors != 0 || result.Sent != 20 || result.Acked != 20 || result.Ack.Samples != 20 || result.Fanout.Samples != 40 {
		t.Fatalf("bad load report: %+v", result)
	}
	if result.OpsPerSecond <= 0 || result.Ack.P99 <= 0 || result.Fanout.P99 <= 0 {
		t.Fatal("missing latency samples")
	}
}

func TestLoadInvalidOptions(t *testing.T) {
	if _, err := run(context.Background(), options{}); err == nil {
		t.Fatal("accepted invalid options")
	}
}
