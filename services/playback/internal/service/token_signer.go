package playback

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// TokenSigner mints verifiable playback JWTs (EdDSA/Ed25519) and exposes the
// public keys so the CDN edge can verify them. Keys are persisted in ScyllaDB
// and reloaded on startup so tokens survive a service restart.
type TokenSigner struct {
	session    *gocql.Session
	privateKey ed25519.PrivateKey
	keyID      string
}

const playbackKeysTable = `CREATE TABLE IF NOT EXISTS playback_signing_keys (
	kid         text PRIMARY KEY,
	private_key blob,
	public_key  blob,
	algorithm   text,
	created_at  timestamp
)`

func NewTokenSigner(ctx context.Context, session *gocql.Session) (*TokenSigner, error) {
	if err := session.Query(playbackKeysTable).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("ensure playback signing keys table: %w", err)
	}
	signer := &TokenSigner{session: session}

	// Reuse the most recent persisted key when present so previously issued
	// tokens remain verifiable across restarts.
	var kid string
	var seedBytes, publicKeyBytes []byte
	err := session.Query(
		`SELECT kid, private_key, public_key FROM playback_signing_keys LIMIT 1`,
	).WithContext(ctx).Scan(&kid, &seedBytes, &publicKeyBytes)
	switch {
	case err == nil:
		if privateKey := ed25519.NewKeyFromSeed(seedBytes); len(seedBytes) == ed25519.SeedSize {
			signer.keyID = kid
			signer.privateKey = privateKey
			return signer, nil
		}
		// Corrupt seed: fall through to key generation.
	case err != gocql.ErrNotFound:
		return nil, fmt.Errorf("load playback signing key: %w", err)
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate playback signing key: %w", err)
	}
	keyID := "pbk_" + hex.EncodeToString(publicKey[:8])
	if err := session.Query(
		`INSERT INTO playback_signing_keys (kid, private_key, public_key, algorithm, created_at) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`,
		keyID, privateKey.Seed(), []byte(publicKey), "EdDSA", time.Now().UTC(),
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("persist playback signing key: %w", err)
	}
	signer.keyID = keyID
	signer.privateKey = privateKey
	return signer, nil
}

// KeyID returns the identifier of the active signing key.
func (s *TokenSigner) KeyID() string { return s.keyID }

// PublicKeyJWKs returns the public keys as a JWKS document for the CDN edge.
func (s *TokenSigner) PublicKeyJWKs(ctx context.Context) ([]byte, error) {
	iter := s.session.Query(`SELECT public_key FROM playback_signing_keys`).WithContext(ctx).Iter()
	set := jwk.NewSet()
	defer iter.Close()
	var publicKey []byte
	for iter.Scan(&publicKey) {
		key, err := jwk.FromRaw(ed25519.PublicKey(publicKey))
		if err != nil {
			return nil, fmt.Errorf("build jwk: %w", err)
		}
		if err := key.Set(jwk.KeyIDKey, "pbk_"+hex.EncodeToString(publicKey[:8])); err != nil {
			return nil, err
		}
		if err := key.Set(jwk.KeyUsageKey, jwk.ForSignature); err != nil {
			return nil, err
		}
		if err := key.Set(jwk.KeyTypeKey, "OKP"); err != nil {
			return nil, err
		}
		if err := key.Set(jwk.AlgorithmKey, "EdDSA"); err != nil {
			return nil, err
		}
		if err := set.AddKey(key); err != nil {
			return nil, err
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("list playback signing keys: %w", err)
	}
	return json.Marshal(set)
}

// IssueToken mints a signed playback JWT scoped to a playback ID and purpose.
// purpose is one of "playback", "thumbnail", "storyboard" (mirroring Mux's
// token split so each URL class can be revoked/verified independently).
func (s *TokenSigner) IssueToken(ctx context.Context, playbackID, purpose string, ttl time.Duration) (string, error) {
	if s.privateKey == nil {
		return "", fmt.Errorf("playback signer has no private key")
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	now := time.Now()
	token, err := jwt.NewBuilder().
		Subject(playbackID).
		IssuedAt(now).
		Expiration(now.Add(ttl)).
		Claim("kid", s.keyID).
		Claim("purpose", purpose).
		Build()
	if err != nil {
		return "", fmt.Errorf("build playback token: %w", err)
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.EdDSA, s.privateKey))
	if err != nil {
		return "", fmt.Errorf("sign playback token: %w", err)
	}
	return string(signed), nil
}
