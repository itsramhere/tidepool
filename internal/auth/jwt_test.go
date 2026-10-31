package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "test-secret"

func TestIssueVerifyRoundTrip(t *testing.T) {
	token, err := Issue(testSecret, "alice", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	uid, claims, err := Verify(testSecret, token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if uid != "alice" {
		t.Errorf("uid = %q, want alice", uid)
	}
	if claims["sub"] != "alice" {
		t.Errorf("claims[sub] = %v, want alice", claims["sub"])
	}
}

func TestIssueWithClaimsRoundTrip(t *testing.T) {
	token, err := IssueWithClaims(testSecret, "alice", map[string]any{
		"boards": []string{"b1", "b2"},
		"role":   "editor",
	}, time.Hour)
	if err != nil {
		t.Fatalf("IssueWithClaims: %v", err)
	}
	uid, claims, err := Verify(testSecret, token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if uid != "alice" {
		t.Errorf("uid = %q, want alice", uid)
	}
	boards, ok := claims["boards"].([]any)
	if !ok || len(boards) != 2 || boards[0] != "b1" {
		t.Errorf("claims[boards] = %#v, want [b1 b2]", claims["boards"])
	}
	if claims["role"] != "editor" {
		t.Errorf("claims[role] = %v, want editor", claims["role"])
	}
}

func TestVerifyRejects(t *testing.T) {
	good, err := Issue(testSecret, "alice", time.Hour)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	expired, err := Issue(testSecret, "alice", -time.Minute)
	if err != nil {
		t.Fatalf("Issue expired: %v", err)
	}
	noExp := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{Subject: "alice"})
	noExpStr, err := noExp.SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign no-exp token: %v", err)
	}
	wrongAlg := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.RegisteredClaims{
		Subject:   "alice",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})
	wrongAlgStr, err := wrongAlg.SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign HS512 token: %v", err)
	}

	tests := []struct {
		name  string
		token string
	}{
		{"wrong secret", func() string { t, _ := Issue("other-secret", "alice", time.Hour); return t }()},
		{"expired", expired},
		{"missing exp", noExpStr},
		{"wrong algorithm", wrongAlgStr},
		{"garbage", "not-a-token"},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if uid, _, err := Verify(testSecret, tt.token); err == nil {
				t.Errorf("Verify(%s token) = (%q, nil), want error", tt.name, uid)
			}
		})
	}
	_ = good // round-trip covered above
}

func TestIssueRejectsEmptyUID(t *testing.T) {
	if _, err := Issue(testSecret, "", time.Hour); err == nil {
		t.Error("Issue(empty uid) = nil error, want error")
	}
}

func TestTokenHasThreeSegments(t *testing.T) {
	token, err := Issue(testSecret, "bob", time.Minute)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if got := strings.Count(token, "."); got != 2 {
		t.Errorf("token has %d dots, want 2 (header.payload.signature)", got)
	}
}
