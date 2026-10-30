package server

import (
	"encoding/json"
	"testing"

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
