package crdt

import (
	"encoding/json"
	"reflect"
	"testing"
)

func buildSnapshotDoc(t *testing.T) *Document {
	t.Helper()
	d := NewDocument(DocKey{Coll: "cards", Doc: "snap1"})
	ed := &Editor{Actor: actorA, Doc: d, Schema: testSchema}
	ed2 := &Editor{Actor: actorB, Doc: d, Schema: testSchema}
	_, err := ed.Set("title", "hello")
	must(t, err)
	_, err = ed.Inc("votes", 5)
	must(t, err)
	_, err = ed2.Inc("votes", -5) // counter exists with N = 0
	must(t, err)
	_, err = ed.Add("labels", "x")
	must(t, err)
	_, err = ed2.Add("labels", "x") // second tag on the same element
	must(t, err)
	_, err = ed2.Add("labels", "gone")
	must(t, err)
	_, err = ed2.Remove("labels", "gone") // element fully removed: elems keeps an empty entry path
	must(t, err)
	_, err = ed.Insert("checklist", 0, "a")
	must(t, err)
	_, err = ed.Insert("checklist", 1, "b")
	must(t, err)
	_, err = ed.Delete("checklist", 1) // tombstone retained
	must(t, err)
	_, err = ed.InsertText("body", 0, "héllo")
	must(t, err)
	_, err = ed.Set("title", nil) // register holding JSON null
	must(t, err)
	return d
}

func TestSnapshotRoundTrip(t *testing.T) {
	d := buildSnapshotDoc(t)
	raw, err := d.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	back, err := UnmarshalSnapshot(raw)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}
	if !reflect.DeepEqual(back, d) {
		t.Fatalf("round-trip document differs:\nwant %+v\ngot  %+v", d, back)
	}
	if got := back.Values(); !reflect.DeepEqual(got, d.Values()) {
		t.Errorf("round-trip Values() = %v, want %v", got, d.Values())
	}
}

func TestSnapshotJSONShape(t *testing.T) {
	d := buildSnapshotDoc(t)
	raw, err := d.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatalf("top-level unmarshal: %v", err)
	}
	for _, key := range []string{"key", "vv", "clock", "maxLamport", "fields"} {
		if _, ok := top[key]; !ok {
			t.Errorf("snapshot missing %q key (PLAN.md §4.6)", key)
		}
	}

	var fields map[string]map[string]any
	if err := json.Unmarshal(top["fields"], &fields); err != nil {
		t.Fatalf("fields unmarshal: %v", err)
	}

	title := fields["title"]
	if title["type"] != string(FieldRegister) || title["actor"] != string(actorA) {
		t.Errorf("register shape: %v", title)
	}
	if _, ok := title["value"]; !ok {
		t.Error("register missing value (null must be present, not omitted)")
	}
	if title["lamport"] != float64(d.Fields["title"].Reg.Lamport) {
		t.Errorf("register lamport: %v", title)
	}

	if votes := fields["votes"]; votes["type"] != string(FieldCounter) || votes["n"] != float64(0) {
		t.Errorf("counter shape: %v (n must be present even at 0)", votes)
	}

	labels := fields["labels"]
	if labels["type"] != string(FieldSet) {
		t.Errorf("set shape: %v", labels)
	}
	elemsJSON, err := json.Marshal(labels["elems"])
	if err != nil {
		t.Fatalf("marshal elems: %v", err)
	}
	var elems map[string][]map[string]any
	if err := json.Unmarshal(elemsJSON, &elems); err != nil {
		t.Fatalf("elems unmarshal: %v", err)
	}
	tags := elems["x"]
	if len(tags) != 2 || tags[0]["actor"].(string) > tags[1]["actor"].(string) {
		t.Errorf("set tags not sorted by (actor, seq): %v", tags)
	}
	if _, ok := elems["gone"]; ok {
		t.Error("fully removed element still in elems")
	}

	body := fields["body"]
	if body["type"] != string(FieldText) {
		t.Errorf("text shape: %v", body)
	}
	itemsJSON, err := json.Marshal(body["items"])
	if err != nil {
		t.Fatalf("marshal items: %v", err)
	}
	var items []map[string]any
	if err := json.Unmarshal(itemsJSON, &items); err != nil {
		t.Fatalf("items unmarshal: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("text has %d items, want 5 (one per code point of héllo)", len(items))
	}
	first := items[0]
	for _, key := range []string{"id", "lamport", "value", "deleted"} {
		if _, ok := first[key]; !ok {
			t.Errorf("text item missing %q (PLAN.md §4.6): %v", key, first)
		}
	}
}

func TestSnapshotRejectsGarbage(t *testing.T) {
	if _, err := UnmarshalSnapshot([]byte(`not json`)); err == nil {
		t.Fatal("UnmarshalSnapshot(garbage) = nil error, want error")
	}
	if _, err := UnmarshalSnapshot([]byte(`{"key":{"coll":"Cards","doc":"x"}}`)); err == nil {
		t.Fatal("UnmarshalSnapshot(invalid key) = nil error, want error")
	}
	if _, err := UnmarshalSnapshot([]byte(`{"key":{"coll":"cards","doc":"x"},"fields":{"f":{"type":"blob"}}}`)); err == nil {
		t.Fatal("UnmarshalSnapshot(unknown field type) = nil error, want error")
	}
	if _, err := UnmarshalSnapshot([]byte(`{"key":{"coll":"cards","doc":"x"},"fields":{"f":{"type":"register"}}}`)); err == nil {
		t.Fatal("UnmarshalSnapshot(register without value) = nil error, want error")
	}
}
