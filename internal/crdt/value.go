package crdt

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// MaxScalarBytes is the largest raw JSON size accepted for one scalar value
// (PLAN.md §4.2: 64 KiB).
const MaxScalarBytes = 64 << 10

// ParseScalar parses a JSON scalar: string, number (as float64), bool or
// null. Objects, arrays, invalid JSON and anything larger than MaxScalarBytes
// are rejected. The returned error wraps ErrInvalid.
func ParseScalar(raw json.RawMessage) (any, error) {
	if len(raw) > MaxScalarBytes {
		return nil, fmt.Errorf("%w: scalar value is %d bytes, max is %d", ErrInvalid, len(raw), MaxScalarBytes)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("%w: value is not valid JSON: %v", ErrInvalid, err)
	}
	switch v.(type) {
	case nil, string, bool, float64:
		return v, nil
	default:
		return nil, fmt.Errorf("%w: scalar must be string, number, bool or null, got %T", ErrInvalid, v)
	}
}

// IsSingleCodePoint reports whether s is exactly one valid Unicode code point,
// as text ins ops require. Invalid UTF-8 and multi-code-point strings
// (including grapheme clusters like flag emoji) are rejected.
func IsSingleCodePoint(s string) bool {
	return utf8.ValidString(s) && utf8.RuneCountInString(s) == 1
}
