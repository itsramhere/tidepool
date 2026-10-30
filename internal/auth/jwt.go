// Package auth issues and verifies the JSON Web Tokens that authenticate
// Tidepool sessions.
package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims is the session identity carried by a token: the uid rides in the
// standard sub claim.
type Claims struct {
	UID string
	jwt.RegisteredClaims
}

// Issue signs an HS256 token for uid, valid for ttl.
func Issue(secret string, uid string, ttl time.Duration) (string, error) {
	if uid == "" {
		return "", errors.New("auth: uid must not be empty")
	}
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Subject:   uid,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("auth: sign token: %w", err)
	}
	return signed, nil
}

// Verify parses and validates an HS256 token and returns its uid. The token
// must carry an exp claim (checked automatically by the parser option) and
// must not be expired.
func Verify(secret string, token string) (string, error) {
	parsed, err := jwt.ParseWithClaims(token, &jwt.RegisteredClaims{},
		func(t *jwt.Token) (any, error) { return []byte(secret), nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return "", fmt.Errorf("auth: invalid token: %w", err)
	}
	claims, ok := parsed.Claims.(*jwt.RegisteredClaims)
	if !ok || !parsed.Valid {
		return "", errors.New("auth: invalid token claims")
	}
	if claims.Subject == "" {
		return "", errors.New("auth: token has empty sub claim")
	}
	return claims.Subject, nil
}
