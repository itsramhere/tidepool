package crdt

import (
	"errors"
	"reflect"
	"testing"
)

func newEditor(t *testing.T, actor ActorID) *Editor {
	t.Helper()
	d := NewDocument(DocKey{Coll: "cards", Doc: "c1"})
	return &Editor{Actor: actor, Doc: d, Schema: testSchema}
}

// must fails the test when an editor method returned an error.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
}

func TestEditorSetStampsSeqAndLamport(t *testing.T) {
	ed := newEditor(t, actorA)
	ops1, err := ed.Set("title", "v1")
	must(t, err)
	ops2, err := ed.Set("title", "v2")
	must(t, err)
	if len(ops1) != 1 || len(ops2) != 1 {
		t.Fatalf("Set returned %d and %d ops, want 1 each", len(ops1), len(ops2))
	}
	if ops1[0].ID != (OpID{Actor: actorA, Seq: 1}) || ops1[0].Lamport != 1 {
		t.Errorf("first op = %+v, want seq 1 lamport 1", ops1[0])
	}
	if ops2[0].ID != (OpID{Actor: actorA, Seq: 2}) || ops2[0].Lamport != 2 {
		t.Errorf("second op = %+v, want seq 2 lamport 2", ops2[0])
	}
	if got := ed.Doc.Values()["title"]; got != "v2" {
		t.Errorf("title = %v, want v2", got)
	}
	if ops1[0].Key != ed.Doc.Key {
		t.Errorf("op key %+v does not match document", ops1[0].Key)
	}
}

func TestEditorInc(t *testing.T) {
	ed := newEditor(t, actorA)
	_, err := ed.Inc("votes", 5)
	must(t, err)
	_, err = ed.Inc("votes", -3)
	must(t, err)
	if got := ed.Doc.Values()["votes"]; got != int64(2) {
		t.Errorf("votes = %v, want 2", got)
	}
	if _, err := ed.Inc("title", 1); !errors.Is(err, ErrInvalid) {
		t.Errorf("Inc on register = %v, want ErrInvalid", err)
	}
}

func TestEditorAddRemove(t *testing.T) {
	ed := newEditor(t, actorA)
	addX, err := ed.Add("labels", "x")
	must(t, err)
	_, err = ed.Add("labels", "y")
	must(t, err)
	if got := ed.Doc.Values()["labels"]; !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Errorf("labels = %v, want [x y]", got)
	}

	rem, err := ed.Remove("labels", "x")
	must(t, err)
	if !reflect.DeepEqual(rem[0].Observed, []OpID{addX[0].ID}) {
		t.Errorf("rem.Observed = %v, want [%v]", rem[0].Observed, addX[0].ID)
	}
	if got := ed.Doc.Values()["labels"]; !reflect.DeepEqual(got, []string{"y"}) {
		t.Errorf("labels after remove = %v, want [y]", got)
	}
	if _, err := ed.Remove("labels", "x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Remove of absent element = %v, want ErrNotFound", err)
	}
	if _, err := ed.Remove("title", "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("Remove on register = %v, want ErrInvalid", err)
	}
	if _, err := ed.Remove("votes", "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("Remove on counter = %v, want ErrInvalid", err)
	}
}

func TestEditorInsertPositions(t *testing.T) {
	ed := newEditor(t, actorA)
	a, err := ed.Insert("checklist", 0, "a")
	must(t, err)
	if a[0].Ref != nil {
		t.Errorf("insert at 0 has Ref %+v, want nil", a[0].Ref)
	}
	b, err := ed.Insert("checklist", 1, "b")
	must(t, err)
	if b[0].Ref == nil || *b[0].Ref != a[0].ID {
		t.Errorf("insert at 1 refs %v, want %v", b[0].Ref, a[0].ID)
	}
	_, err = ed.Insert("checklist", 1, "c") // refs a again, higher lamport first
	must(t, err)
	if got := ed.Doc.Values()["checklist"]; !reflect.DeepEqual(got, []any{"a", "c", "b"}) {
		t.Errorf("checklist = %v, want [a c b]", got)
	}
	d, err := ed.Insert("checklist", 3, "d") // append: refs the last element
	must(t, err)
	vis := ed.Doc.Fields["checklist"].Seq.VisibleIDs()
	if d[0].Ref == nil || *d[0].Ref != vis[2] {
		t.Errorf("append refs %v, want %v", d[0].Ref, vis[2])
	}
	if _, err := ed.Insert("checklist", 5, "x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("insert past end = %v, want ErrNotFound", err)
	}
	if _, err := ed.Insert("checklist", -1, "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("insert at negative index = %v, want ErrInvalid", err)
	}
}

func TestEditorInsertOnFreshField(t *testing.T) {
	ed := newEditor(t, actorA)
	if _, ok := ed.Doc.Fields["checklist"]; ok {
		t.Fatal("field should not exist yet")
	}
	_, err := ed.Insert("checklist", 0, "first")
	must(t, err)
	if got := ed.Doc.Values()["checklist"]; !reflect.DeepEqual(got, []any{"first"}) {
		t.Errorf("checklist = %v, want [first]", got)
	}
}

func TestEditorInsertTextChained(t *testing.T) {
	ed := newEditor(t, actorA)
	ops, err := ed.InsertText("body", 0, "abé")
	must(t, err)
	if len(ops) != 3 {
		t.Fatalf("InsertText returned %d ops, want 3 (one per code point)", len(ops))
	}
	for i, op := range ops {
		if op.ID.Seq != uint64(i+1) || op.Lamport != uint64(i+1) {
			t.Errorf("op %d stamped %+v lamport %d", i, op.ID, op.Lamport)
		}
		if i > 0 && (op.Ref == nil || *op.Ref != ops[i-1].ID) {
			t.Errorf("op %d refs %v, want %v (previous new element)", i, op.Ref, ops[i-1].ID)
		}
	}
	if ops[0].Ref != nil {
		t.Errorf("first op refs %v, want nil (head)", ops[0].Ref)
	}
	if got := ed.Doc.Values()["body"]; got != "abé" {
		t.Errorf("body = %q, want %q", got, "abé")
	}

	_, err = ed.InsertText("body", 1, "XY")
	must(t, err)
	if got := ed.Doc.Values()["body"]; got != "aXYbé" {
		t.Errorf("body = %q, want %q", got, "aXYbé")
	}
	if _, err := ed.InsertText("body", 0, "\xff\xfe"); !errors.Is(err, ErrInvalid) {
		t.Errorf("InsertText with invalid UTF-8 = %v, want ErrInvalid", err)
	}
	if _, err := ed.Insert("body", 0, "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("Insert on text field = %v, want ErrInvalid", err)
	}
	if _, err := ed.InsertText("checklist", 0, "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("InsertText on list field = %v, want ErrInvalid", err)
	}
}

func TestEditorDelete(t *testing.T) {
	ed := newEditor(t, actorA)
	_, err := ed.Insert("checklist", 0, "a")
	must(t, err)
	b, err := ed.Insert("checklist", 1, "b")
	must(t, err)
	_, err = ed.Insert("checklist", 2, "c")
	must(t, err)

	del, err := ed.Delete("checklist", 1) // deletes b
	must(t, err)
	if del[0].Ref == nil || *del[0].Ref != b[0].ID {
		t.Errorf("delete refs %v, want %v", del[0].Ref, b[0].ID)
	}
	if got := ed.Doc.Values()["checklist"]; !reflect.DeepEqual(got, []any{"a", "c"}) {
		t.Errorf("checklist = %v, want [a c]", got)
	}
	if _, err := ed.Delete("checklist", 2); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete past end = %v, want ErrNotFound", err)
	}

	// Delete works on text too.
	_, err = ed.InsertText("body", 0, "hey")
	must(t, err)
	_, err = ed.Delete("body", 1)
	must(t, err)
	if got := ed.Doc.Values()["body"]; got != "hy" {
		t.Errorf("body = %q, want %q", got, "hy")
	}
}

func TestEditorUnknownSchemaField(t *testing.T) {
	ed := newEditor(t, actorA)
	if _, err := ed.Set("ghost", "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("Set on unknown field = %v, want ErrInvalid", err)
	}
	if _, err := ed.Insert("ghost", 0, "x"); !errors.Is(err, ErrInvalid) {
		t.Errorf("Insert on unknown field = %v, want ErrInvalid", err)
	}
}
