package crdt

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateActor(t *testing.T) {
	tests := []struct {
		name    string
		actor   ActorID
		wantErr bool
	}{
		{"16 lowercase hex", "0123456789abcdef", false},
		{"all zeros", "0000000000000000", false},
		{"uppercase hex", "0123456789ABCDEF", true},
		{"too short", "0123456789abcde", true},
		{"too long", "0123456789abcdef0", true},
		{"not hex", "0123456789abcdeg", true},
		{"empty", "", true},
		{"contains slash", "0123456789abcde/", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateActor(tt.actor)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateActor(%q) = %v, wantErr %v", tt.actor, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Errorf("error does not wrap ErrInvalid: %v", err)
			}
		})
	}
}

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name    string
		key     DocKey
		wantErr bool
	}{
		{"plain", DocKey{Coll: "cards", Doc: "c1"}, false},
		{"underscored coll", DocKey{Coll: "card_lists", Doc: "_-x"}, false},
		{"single-char coll", DocKey{Coll: "a", Doc: "1"}, false},
		{"max coll", DocKey{Coll: strings.Repeat("a", 64), Doc: "d"}, false},
		{"coll too long", DocKey{Coll: strings.Repeat("a", 65), Doc: "d"}, true},
		{"coll uppercase", DocKey{Coll: "Cards", Doc: "d"}, true},
		{"coll digit start", DocKey{Coll: "1cards", Doc: "d"}, true},
		{"coll dash", DocKey{Coll: "my-cards", Doc: "d"}, true},
		{"empty coll", DocKey{Coll: "", Doc: "d"}, true},
		{"empty doc", DocKey{Coll: "cards", Doc: ""}, true},
		{"doc slash", DocKey{Coll: "cards", Doc: "a/b"}, true},
		{"doc 128 chars", DocKey{Coll: "cards", Doc: strings.Repeat("x", 128)}, false},
		{"doc 129 chars", DocKey{Coll: "cards", Doc: strings.Repeat("x", 129)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateKey(%+v) = %v, wantErr %v", tt.key, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Errorf("error does not wrap ErrInvalid: %v", err)
			}
		})
	}
}

func TestValidateField(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		wantErr bool
	}{
		{"plain", "title", false},
		{"mixed case tail", "titleText_2", false},
		{"uppercase start", "Title", true},
		{"digit start", "2title", true},
		{"dash", "my-field", true},
		{"empty", "", true},
		{"max 64", strings.Repeat("a", 64), false},
		{"65 chars", strings.Repeat("a", 65), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateField(tt.field)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateField(%q) = %v, wantErr %v", tt.field, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Errorf("error does not wrap ErrInvalid: %v", err)
			}
		})
	}
}

func TestNewActorID(t *testing.T) {
	seen := make(map[ActorID]bool)
	for i := 0; i < 100; i++ {
		a := NewActorID()
		if err := ValidateActor(a); err != nil {
			t.Fatalf("NewActorID() = %q: %v", a, err)
		}
		if seen[a] {
			t.Fatalf("NewActorID returned %q twice in %d calls", a, i+1)
		}
		seen[a] = true
	}
}

func TestOpIDLess(t *testing.T) {
	const a, b = ActorID("0123456789abcdef"), ActorID("fedcba9876543210")
	tests := []struct {
		name string
		x, y OpID
		want bool
	}{
		{"same actor, lower seq first", OpID{a, 1}, OpID{a, 2}, true},
		{"same actor, higher seq not first", OpID{a, 2}, OpID{a, 1}, false},
		{"same actor, equal seq", OpID{a, 1}, OpID{a, 1}, false},
		{"actor dominates seq", OpID{a, 9}, OpID{b, 1}, a < b},
		{"reverse actor order", OpID{b, 9}, OpID{a, 1}, b < a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.x.Less(tt.y); got != tt.want {
				t.Errorf("Less(%+v, %+v) = %v, want %v", tt.x, tt.y, got, tt.want)
			}
		})
	}
}

func TestOpLess(t *testing.T) {
	const a, b = ActorID("0123456789abcdef"), ActorID("fedcba9876543210")
	mk := func(lamport uint64, actor ActorID) Op {
		return Op{ID: OpID{Actor: actor, Seq: 1}, Key: DocKey{Coll: "cards", Doc: "c1"}, Lamport: lamport, Kind: KindSet}
	}
	tests := []struct {
		name string
		x, y Op
		want bool
	}{
		{"higher lamport sorts later", mk(1, a), mk(2, a), true},
		{"tie: lower actor first", mk(5, a), mk(5, b), a < b},
		{"tie: reverse", mk(5, b), mk(5, a), b < a},
		{"equal ops", mk(5, a), mk(5, a), false},
		{"lamport dominates actor", mk(1, b), mk(2, a), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := OpLess(tt.x, tt.y); got != tt.want {
				t.Errorf("OpLess = %v, want %v", got, tt.want)
			}
		})
	}
}
