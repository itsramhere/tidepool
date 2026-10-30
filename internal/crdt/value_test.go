package crdt

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestParseScalar(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    any
		wantErr bool
	}{
		{"string", `"hello"`, "hello", false},
		{"empty string", `""`, "", false},
		{"number", `3.5`, 3.5, false},
		{"negative number", `-12`, -12.0, false},
		{"bool true", `true`, true, false},
		{"bool false", `false`, false, false},
		{"null", `null`, nil, false},
		{"leading zero not JSON", `01`, nil, true},
		{"garbage", `nope`, nil, true},
		{"empty input", ``, nil, true},
		{"object", `{"a":1}`, nil, true},
		{"array", `[1,2]`, nil, true},
		{"trailing garbage", `1 2`, nil, true},
		{"max size string", `"` + strings.Repeat("a", MaxScalarBytes-2) + `"`, strings.Repeat("a", MaxScalarBytes-2), false},
		{"over max size", `"` + strings.Repeat("a", MaxScalarBytes-1) + `"`, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseScalar(json.RawMessage(tt.raw))
			if tt.wantErr {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("ParseScalar(%s) = %v, want ErrInvalid", tt.raw, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseScalar(%s): %v", tt.raw, err)
			}
			if got != tt.want {
				t.Errorf("ParseScalar(%s) = %#v, want %#v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseScalarErrorType(t *testing.T) {
	// The nil result on the error path must be a nil interface, so callers
	// checking `got != nil` against a zero register value behave sanely.
	got, err := ParseScalar(json.RawMessage(`{}`))
	if err == nil || got != nil {
		t.Fatalf("ParseScalar(object) = (%#v, %v), want (nil, error)", got, err)
	}
}

func TestIsSingleCodePoint(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{"ascii", "a", true},
		{"latin1", "é", true},
		{"emoji", "😀", true},
		{"space", " ", true},
		{"empty", "", false},
		{"two ascii", "ab", false},
		{"two code points", "aé", false},
		{"flag grapheme cluster", "🇺🇳", false},
		{"precomposed é is one code point", "é", true},
		{"decomposed cluster (e + combining acute)", "e\u0301", false},
		{"invalid utf-8", "\xff\xfe", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSingleCodePoint(tt.s); got != tt.want {
				t.Errorf("IsSingleCodePoint(%q) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}
