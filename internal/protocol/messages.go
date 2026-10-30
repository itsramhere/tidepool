// Package protocol defines the JSON messages exchanged between Tidepool
// clients and servers over WebSocket, one object per text frame.
package protocol

import (
	"encoding/json"
	"fmt"

	"github.com/itsramhere/tidepool/internal/crdt"
)

// Protocol version: hello must carry v = ProtocolVersion.
const ProtocolVersion = 1

// Frame types (PLAN.md §4.7), field "t".
const (
	TypeHello    = "hello"
	TypeSub      = "sub"
	TypeUnsub    = "unsub"
	TypePush     = "push"
	TypePing     = "ping"
	TypeWelcome  = "welcome"
	TypeSnapshot = "snap"
	TypeOp       = "op"
	TypeEvict    = "evict"
	TypeReady    = "ready"
	TypeAck      = "ack"
	TypeErr      = "err"
	TypePong     = "pong"
)

// Ack status values (PLAN.md §4.7).
const (
	StatusOK       = "ok"
	StatusDup      = "dup"
	StatusRetry    = "retry"
	StatusRejected = "rejected"
	StatusInvalid  = "invalid"
)

// Envelope peeks at a frame's type before decoding the whole message.
type Envelope struct {
	T string `json:"t"`
}

// VersionVector rides the wire as map[actor]lastSeq.
type VersionVector map[crdt.ActorID]uint64

// Client → server messages.

type Hello struct {
	T     string       `json:"t"`
	V     int          `json:"v"`
	Token string       `json:"token"`
	Actor crdt.ActorID `json:"actor"`
}

type Sub struct {
	T      string                   `json:"t"`
	ID     string                   `json:"id"`
	Coll   string                   `json:"coll"`
	Filter string                   `json:"filter"`
	Known  map[string]VersionVector `json:"known"`
}

type Unsub struct {
	T  string `json:"t"`
	ID string `json:"id"`
}

type Push struct {
	T   string    `json:"t"`
	Req uint64    `json:"req"`
	Ops []crdt.Op `json:"ops"`
}

type Ping struct {
	T string `json:"t"`
}

// Server → client messages.

type Welcome struct {
	T     string       `json:"t"`
	Node  string       `json:"node"`
	Actor crdt.ActorID `json:"actor"`
}

type SnapshotFrame struct {
	T        string          `json:"t"`
	Sub      string          `json:"sub"`
	Snapshot json.RawMessage `json:"snapshot"`
}

type OpFrame struct {
	T   string  `json:"t"`
	Sub string  `json:"sub"`
	Op  crdt.Op `json:"op"`
}

type Evict struct {
	T   string      `json:"t"`
	Sub string      `json:"sub"`
	Key crdt.DocKey `json:"key"`
}

type Ready struct {
	T   string `json:"t"`
	Sub string `json:"sub"`
}

// AckResult reports one pushed op's fate, in push order.
type AckResult struct {
	ID     crdt.OpID `json:"id"`
	Status string    `json:"status"`
	Reason string    `json:"reason,omitempty"`
}

type Ack struct {
	T       string      `json:"t"`
	Req     uint64      `json:"req"`
	Results []AckResult `json:"results"`
}

type Error struct {
	T    string `json:"t"`
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

type Pong struct {
	T string `json:"t"`
}

// Constructors stamp the frame type so call sites can't forget it.

func NewHello(v int, token string, actor crdt.ActorID) Hello {
	return Hello{T: TypeHello, V: v, Token: token, Actor: actor}
}

func NewSub(id, coll, filter string, known map[string]VersionVector) Sub {
	return Sub{T: TypeSub, ID: id, Coll: coll, Filter: filter, Known: known}
}

func NewUnsub(id string) Unsub { return Unsub{T: TypeUnsub, ID: id} }

func NewPush(req uint64, ops []crdt.Op) Push { return Push{T: TypePush, Req: req, Ops: ops} }

func NewPing() Ping { return Ping{T: TypePing} }

func NewWelcome(node string, actor crdt.ActorID) Welcome {
	return Welcome{T: TypeWelcome, Node: node, Actor: actor}
}

func NewSnapshot(sub string, snapshot json.RawMessage) SnapshotFrame {
	return SnapshotFrame{T: TypeSnapshot, Sub: sub, Snapshot: snapshot}
}

func NewOp(sub string, op crdt.Op) OpFrame { return OpFrame{T: TypeOp, Sub: sub, Op: op} }

func NewEvict(sub string, key crdt.DocKey) Evict {
	return Evict{T: TypeEvict, Sub: sub, Key: key}
}

func NewReady(sub string) Ready { return Ready{T: TypeReady, Sub: sub} }

func NewAck(req uint64, results []AckResult) Ack {
	return Ack{T: TypeAck, Req: req, Results: results}
}

func NewError(code, msg string) Error { return Error{T: TypeErr, Code: code, Msg: msg} }

func NewPong() Pong { return Pong{T: TypePong} }

// Encode marshals one frame.
func Encode(m any) ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode frame: %w", err)
	}
	return data, nil
}

// Decode peeks at the frame's type and unmarshals the whole message into the
// matching client → server struct. Server → client frames decode as
// "unknown type" here; only one direction is ever read per connection.
func Decode(data []byte) (any, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode frame: %w", err)
	}
	switch env.T {
	case TypeHello:
		var m Hello
		return m, json.Unmarshal(data, &m)
	case TypeSub:
		var m Sub
		return m, json.Unmarshal(data, &m)
	case TypeUnsub:
		var m Unsub
		return m, json.Unmarshal(data, &m)
	case TypePush:
		var m Push
		return m, json.Unmarshal(data, &m)
	case TypePing:
		var m Ping
		return m, json.Unmarshal(data, &m)
	default:
		return nil, fmt.Errorf("decode frame: unknown type %q", env.T)
	}
}
