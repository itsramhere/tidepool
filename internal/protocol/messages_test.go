package protocol

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/itsramhere/tidepool/internal/crdt"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	actor := crdt.ActorID("a1b2c3d4e5f60708")
	op := crdt.Op{
		ID:      crdt.OpID{Actor: actor, Seq: 1},
		Key:     crdt.DocKey{Coll: "cards", Doc: "c1"},
		Lamport: 5,
		Field:   "title",
		Kind:    crdt.KindSet,
		Value:   json.RawMessage(`"hi"`),
	}
	tests := []struct {
		name string
		m    any
		want any
	}{
		{"hello", NewHello(1, "tok", actor), Hello{V: 1, Token: "tok", Actor: actor}},
		{"sub", NewSub("s1", "cards", "", map[string]VersionVector{
			"c1": {actor: 3},
		}), Sub{ID: "s1", Coll: "cards", Filter: "", Known: map[string]VersionVector{
			"c1": {actor: 3},
		}}},
		{"unsub", NewUnsub("s1"), Unsub{ID: "s1"}},
		{"push", NewPush(7, []crdt.Op{op}), Push{Req: 7, Ops: []crdt.Op{op}}},
		{"ping", NewPing(), Ping{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.m)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			// The decoded struct must carry the type tag and equal the
			// constructor's fields.
			env := reflect.ValueOf(got).FieldByName("T").String()
			if env != tt.name {
				t.Errorf("decoded T = %q, want %q", env, tt.name)
			}
			want := tt.want
			// Compare field-by-field via JSON to skip the T field ordering
			// concerns; re-marshal both.
			wantData, _ := json.Marshal(want)
			gotData, _ := json.Marshal(got)
			var wantMap, gotMap map[string]any
			_ = json.Unmarshal(wantData, &wantMap)
			_ = json.Unmarshal(gotData, &gotMap)
			delete(gotMap, "t")
			delete(wantMap, "t")
			if !reflect.DeepEqual(gotMap, wantMap) {
				t.Errorf("decoded = %v, want %v", gotMap, wantMap)
			}
		})
	}
}

func TestDecodeRejects(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"not json", `nope`},
		{"no type", `{"v":1}`},
		{"unknown type", `{"t":"nonsense"}`},
		{"server frame on client side", `{"t":"welcome"}`},
		{"bad sub shape", `{"t":"sub","known":"nope"}`},
		{"bad push ops", `{"t":"push","ops":"nope"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Decode([]byte(tt.data)); err == nil {
				t.Errorf("Decode(%s) = nil error, want error", tt.data)
			}
		})
	}
}

func TestEnvelopePeek(t *testing.T) {
	data, err := Encode(NewPush(3, nil))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.T != TypePush {
		t.Errorf("envelope T = %q, want push", env.T)
	}
}

func TestOpFrameJSONShape(t *testing.T) {
	op := crdt.Op{ID: crdt.OpID{Actor: "a1b2c3d4e5f60708", Seq: 1}, Kind: crdt.KindSet}
	data, err := Encode(NewOp("sub-1", op))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"t", "sub", "op"} {
		if _, ok := m[key]; !ok {
			t.Errorf("op frame missing %q (PLAN.md §4.7)", key)
		}
	}
	if m["t"] != "op" {
		t.Errorf("op frame t = %v, want op", m["t"])
	}
}
