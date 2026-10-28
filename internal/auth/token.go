// Package auth issues and checks device credentials.
//
// Phase 1 uses stateless HMAC tokens: token = HMAC-SHA256(secret, deviceID).
// Any ingestion replica can verify a token without a database lookup. A later
// phase replaces this with mTLS client certificates, which is closer to how
// real field devices prove identity.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
)

type Signer struct{ key []byte }

func NewSigner(secret string) (*Signer, error) {
	if len(secret) < 16 {
		return nil, errors.New("token secret must be at least 16 bytes")
	}
	return &Signer{key: []byte(secret)}, nil
}

func (s *Signer) Issue(deviceID string) string {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(deviceID))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func (s *Signer) Verify(deviceID, token string) bool {
	if deviceID == "" || token == "" {
		return false
	}
	return hmac.Equal([]byte(s.Issue(deviceID)), []byte(token))
}
