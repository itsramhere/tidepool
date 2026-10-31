// Package auth issues and verifies the JSON Web Tokens that authenticate
// Tidepool sessions.
package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Issue signs an HS256 token for uid, valid for ttl.
func Issue(secret string, uid string, ttl time.Duration) (string, error) {
	return IssueWithClaims(secret, uid, nil, ttl)
}

// IssueWithClaims signs an HS256 token for uid with additional custom
// claims (e.g. board memberships for authorization rules), valid for ttl.
func IssueWithClaims(secret string, uid string, extra map[string]any, ttl time.Duration) (string, error) {
	if uid == "" {
		return "", errors.New("auth: uid must not be empty")
	}
	now := time.Now()
	claims := jwt.MapClaims{
		"sub": uid,
		"iat": now.Unix(),
		"exp": now.Add(ttl).Unix(),
	}
	for k, v := range extra {
		if k == "sub" || k == "iat" || k == "exp" {
			continue // registered claims win
		}
		claims[k] = v
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("auth: sign token: %w", err)
	}
	return signed, nil
}

// Verify parses and validates an HS256 token and returns its uid plus every
// claim the token carries (values are JSON types: string, float64, bool,
// []any, map[string]any). The token must carry an exp claim (checked
// automatically by the parser option) and must not be expired.
func Verify(secret string, token string) (string, map[string]any, error) {
	parsed, err := jwt.ParseWithClaims(token, jwt.MapClaims{},
		func(t *jwt.Token) (any, error) { return []byte(secret), nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return "", nil, fmt.Errorf("auth: invalid token: %w", err)
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok || !parsed.Valid {
		return "", nil, errors.New("auth: invalid token claims")
	}
	uid, ok := claims["sub"].(string)
	if !ok || uid == "" {
		return "", nil, errors.New("auth: token has empty sub claim")
	}
	return uid, map[string]any(claims), nil
}
