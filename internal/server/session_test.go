package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/itsramhere/tidepool/internal/auth"
	"github.com/itsramhere/tidepool/internal/crdt"
	"github.com/itsramhere/tidepool/internal/protocol"
)

func TestHandshakeErrors(t *testing.T) {
	t.Run("bad token", func(t *testing.T) {
		ts, _ := newTestServer(t, nil)
		conn := dial(t, ts)
		send(t, conn, protocol.NewHello(1, "not-a-token", aliceActor))
		var e protocol.Error
		if err := json.Unmarshal(recvType(t, conn, "err"), &e); err != nil {
			t.Fatal(err)
		}
		if e.Code != "unauthorized" {
			t.Errorf("err code = %q, want unauthorized", e.Code)
		}
		recvClosed(t, conn)
	})

	t.Run("wrong version", func(t *testing.T) {
		ts, _ := newTestServer(t, nil)
		conn := dial(t, ts)
		send(t, conn, protocol.NewHello(2, mustToken(t, "alice"), aliceActor))
		var e protocol.Error
		if err := json.Unmarshal(recvType(t, conn, "err"), &e); err != nil {
			t.Fatal(err)
		}
		if e.Code != "unsupported_version" {
			t.Errorf("err code = %q, want unsupported_version", e.Code)
		}
		recvClosed(t, conn)
	})

	t.Run("bad actor", func(t *testing.T) {
		ts, _ := newTestServer(t, nil)
		conn := dial(t, ts)
		send(t, conn, protocol.NewHello(1, mustToken(t, "alice"), "NOT-HEX"))
		var e protocol.Error
		if err := json.Unmarshal(recvType(t, conn, "err"), &e); err != nil {
			t.Fatal(err)
		}
		if e.Code != "invalid_actor" {
			t.Errorf("err code = %q, want invalid_actor", e.Code)
		}
		recvClosed(t, conn)
	})

	t.Run("first frame not hello", func(t *testing.T) {
		ts, _ := newTestServer(t, nil)
		conn := dial(t, ts)
		send(t, conn, protocol.NewPing())
		var e protocol.Error
		if err := json.Unmarshal(recvType(t, conn, "err"), &e); err != nil {
			t.Fatal(err)
		}
		if e.Code != "protocol_error" {
			t.Errorf("err code = %q, want protocol_error", e.Code)
		}
		recvClosed(t, conn)
	})

	t.Run("duplicate hello", func(t *testing.T) {
		ts, _ := newTestServer(t, nil)
		conn := dial(t, ts)
		hello(t, conn, aliceActor)
		send(t, conn, protocol.NewHello(1, mustToken(t, "alice"), aliceActor))
		var e protocol.Error
		if err := json.Unmarshal(recvType(t, conn, "err"), &e); err != nil {
			t.Fatal(err)
		}
		if e.Code != "protocol_error" {
			t.Errorf("err code = %q, want protocol_error", e.Code)
		}
		recvClosed(t, conn)
	})

	t.Run("garbage frame", func(t *testing.T) {
		ts, _ := newTestServer(t, nil)
		conn := dial(t, ts)
		hello(t, conn, aliceActor)
		sendRaw(t, conn, "total nonsense")
		var e protocol.Error
		if err := json.Unmarshal(recvType(t, conn, "err"), &e); err != nil {
			t.Fatal(err)
		}
		if e.Code != "bad_frame" {
			t.Errorf("err code = %q, want bad_frame", e.Code)
		}
		recvClosed(t, conn)
	})

	t.Run("unsigned garbage token", func(t *testing.T) {
		ts, _ := newTestServer(t, nil)
		conn := dial(t, ts)
		// Well-formed JWT-shaped string with no valid signature.
		send(t, conn, protocol.NewHello(1, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.x", aliceActor))
		var e protocol.Error
		if err := json.Unmarshal(recvType(t, conn, "err"), &e); err != nil {
			t.Fatal(err)
		}
		if e.Code != "unauthorized" {
			t.Errorf("err code = %q, want unauthorized", e.Code)
		}
		recvClosed(t, conn)
	})
}

func TestPingPong(t *testing.T) {
	ts, _ := newTestServer(t, nil)
	conn := dial(t, ts)
	hello(t, conn, aliceActor)
	send(t, conn, protocol.NewPing())
	recvType(t, conn, "pong")
}

func TestPushTooManyOps(t *testing.T) {
	ts, _ := newTestServer(t, func(cfg *Config) { cfg.MaxOpsPerPush = 3 })
	conn := dial(t, ts)
	hello(t, conn, aliceActor)
	ops := []crdt.Op{
		validSetOp(t, aliceActor, 1, "c1", "title", "a"),
		validSetOp(t, aliceActor, 2, "c1", "title", "b"),
		validSetOp(t, aliceActor, 3, "c1", "title", "c"),
		validSetOp(t, aliceActor, 4, "c1", "title", "d"),
	}
	send(t, conn, protocol.NewPush(1, ops))
	var e protocol.Error
	if err := json.Unmarshal(recvType(t, conn, "err"), &e); err != nil {
		t.Fatal(err)
	}
	if e.Code != "too_many_ops" {
		t.Errorf("err code = %q, want too_many_ops", e.Code)
	}
	recvClosed(t, conn)
}

// TestDevTokenEndpoint: /dev/token issues a working token only when
// dev_mode is enabled (PLAN.md M11 demo tooling).
func TestDevTokenEndpoint(t *testing.T) {
	ts, _ := newTestServer(t, func(cfg *Config) { cfg.DevMode = true })
	res, err := http.Get(ts.URL + "/dev/token?uid=demo-user")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dev token status = %d, want 200", res.StatusCode)
	}
	var body struct {
		Token string `json:"token"`
		UID   string `json:"uid"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.UID != "demo-user" || body.Token == "" {
		t.Fatalf("dev token body = %+v", body)
	}
	if _, _, err := auth.Verify(testJWTSecret, body.Token); err != nil {
		t.Fatalf("issued token does not verify: %v", err)
	}

	// The endpoint must work end-to-end: a session with the token handshakes.
	conn := dial(t, ts)
	send(t, conn, protocol.NewHello(1, body.Token, crdt.ActorID(bobActor)))
	recvType(t, conn, "welcome")

	// Missing uid is rejected.
	res2, err := http.Get(ts.URL + "/dev/token")
	if err != nil {
		t.Fatal(err)
	}
	res2.Body.Close()
	if res2.StatusCode != http.StatusBadRequest {
		t.Fatalf("dev token without uid = %d, want 400", res2.StatusCode)
	}
}

// TestDevTokenDisabledByDefault: without dev_mode the endpoint must not exist.
func TestDevTokenDisabledByDefault(t *testing.T) {
	ts, _ := newTestServer(t, nil)
	res, err := http.Get(ts.URL + "/dev/token?uid=demo-user")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("dev token without dev_mode = %d, want 404", res.StatusCode)
	}
}
