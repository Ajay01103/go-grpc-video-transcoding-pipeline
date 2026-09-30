package storage

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// Regression: the presigner must URI-encode query params exactly once.
// A previous implementation encoded via url.Values.Encode() and then again in
// canonicalizeQuery, producing %252F (double-encoded %2F) which S3 servers reject.
func TestPresignPutEncodesCredentialOnce(t *testing.T) {
	client := New(Config{
		Endpoint:  "http://localhost:9000",
		AccessKey: "CzMPuN6RETJUju6t70GF",
		SecretKey: "test-secret",
		Bucket:    "uploads",
	})

	raw, err := client.PresignPut("raw/abc/source.mp4", 30*time.Minute)
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}

	if strings.Contains(raw, "%25") {
		t.Fatalf("presigned url double-encodes characters: %s", raw)
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse presigned url: %v", err)
	}
	credential := parsed.Query().Get("X-Amz-Credential")
	wantScope := time.Now().UTC().Format("/20060102/") + "us-east-1/s3/aws4_request"
	if !strings.Contains(credential, wantScope) {
		t.Fatalf("credential %q does not contain scope %q", credential, wantScope)
	}
	if strings.Count(credential, "/") != 4 {
		t.Fatalf("credential %q should contain 4 slashes after decoding", credential)
	}

	if got := parsed.Query().Get("X-Amz-Date"); !strings.HasSuffix(got, "Z") || len(got) != 16 {
		t.Fatalf("X-Amz-Date %q is not a valid ISO8601 basic timestamp", got)
	}
}
