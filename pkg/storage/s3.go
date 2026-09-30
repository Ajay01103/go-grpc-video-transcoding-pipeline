// Package storage implements the small S3-compatible surface the video
// pipeline needs against RustFS: presigned upload URLs, fetch-to-file staging,
// and recursive directory upload. It is intentionally dependency-free so every
// service module can import it without pulling in the AWS SDK.
package storage

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Config carries the RustFS/S3 endpoint credentials shared by all services.
type Config struct {
	Endpoint   string // e.g. http://localhost:9000
	Region     string // e.g. us-east-1
	AccessKey  string
	SecretKey  string
	Bucket     string
	UsePathStyle bool // RustFS always uses path-style: http://endpoint/bucket/key
}

// Client is a minimal S3-compatible client.
type Client struct {
	cfg    Config
	http   *http.Client
}

func New(cfg Config) *Client {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	cfg.UsePathStyle = true
	return &Client{cfg: cfg, http: &http.Client{Timeout: 10 * time.Minute}}
}

// PresignPut returns a presigned URL that allows an HTTP PUT of an object for
// a limited time, using AWS SigV4 query-string signing.
func (c *Client) PresignPut(key string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	return c.presign(http.MethodPut, c.cfg.Bucket, key, ttl, "")
}

// PresignGet returns a presigned read URL for an object.
func (c *Client) PresignGet(bucket, key string, ttl time.Duration) (string, error) {
	return c.presign(http.MethodGet, bucket, key, ttl, "")
}

// ObjectURL returns the non-presigned public URL of an object.
func (c *Client) ObjectURL(bucket, key string) string {
	return fmt.Sprintf("%s/%s/%s", strings.TrimRight(c.cfg.Endpoint, "/"), bucket, key)
}

func (c *Client) presign(method, bucket, key string, ttl time.Duration, sessionToken string) (string, error) {
	endpoint := strings.TrimRight(c.cfg.Endpoint, "/")
	if endpoint == "" || bucket == "" || key == "" {
		return "", fmt.Errorf("storage: endpoint, bucket and key are required")
	}
	u, err := url.Parse(fmt.Sprintf("%s/%s/%s", endpoint, bucket, key))
	if err != nil {
		return "", fmt.Errorf("storage: parse endpoint url: %w", err)
	}

	amzDate := time.Now().UTC().Format("20060102T150405Z")
	dateStamp := amzDate[:8]
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, c.cfg.Region)

	// SigV4 presigned params held in raw (unencoded) form; they are URI-encoded
	// exactly once when the canonical query is built below.
	params := [][2]string{
		{"X-Amz-Algorithm", "AWS4-HMAC-SHA256"},
		{"X-Amz-Credential", c.cfg.AccessKey + "/" + credentialScope},
		{"X-Amz-Date", amzDate},
		{"X-Amz-Expires", fmt.Sprintf("%d", int64(ttl.Seconds()))},
		{"X-Amz-SignedHeaders", "host"},
	}
	if sessionToken != "" {
		params = append(params, [2]string{"X-Amz-Security-Token", sessionToken})
	}

	// Canonical query: keys and values awsURIEncode'd once, then sorted.
	type queryPair struct{ key, value string }
	pairs := make([]queryPair, 0, len(params))
	for _, param := range params {
		pairs = append(pairs, queryPair{awsURIEncode(param[0], true), awsURIEncode(param[1], true)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key != pairs[j].key {
			return pairs[i].key < pairs[j].key
		}
		return pairs[i].value < pairs[j].value
	})
	parts := make([]string, len(pairs))
	for index, pair := range pairs {
		parts[index] = pair.key + "=" + pair.value
	}
	canonicalQuery := strings.Join(parts, "&")

	canonicalURI := u.EscapedPath()
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
		"host:" + u.Host + "\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")

	scope := credentialScope
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := hmacSHA256(
		hmacSHA256(
			hmacSHA256(
				hmacSHA256([]byte("AWS4"+c.cfg.SecretKey), []byte(dateStamp)),
				[]byte(c.cfg.Region),
			),
			[]byte("s3"),
		),
		[]byte("aws4_request"),
	)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	u.RawQuery = canonicalQuery + "&X-Amz-Signature=" + signature
	return u.String(), nil
}

// Get downloads an object into memory.
func (c *Client) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.ObjectURL(bucket, key), nil)
	if err != nil {
		return nil, err
	}
	c.signRequest(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("storage: get %s/%s: %w", bucket, key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("storage: get %s/%s: status %d", bucket, key, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// GetToFile downloads an object to a local file (worker staging).
func (c *Client) GetToFile(ctx context.Context, bucket, key, destPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.ObjectURL(bucket, key), nil)
	if err != nil {
		return err
	}
	c.signRequest(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("storage: stage %s/%s: %w", bucket, key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("storage: stage %s/%s: status %d", bucket, key, resp.StatusCode)
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("storage: write staged file: %w", err)
	}
	return f.Sync()
}

// PutFile uploads a local file to the bucket at key.
func (c *Client) PutFile(ctx context.Context, bucket, key, srcPath string) error {
	f, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("storage: open %s: %w", srcPath, err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	// Compute real SHA256 so RustFS X-Amz-Content-Sha256 validation passes.
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("storage: hash %s: %w", srcPath, err)
	}
	payloadHash := hex.EncodeToString(h.Sum(nil))
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("storage: seek %s: %w", srcPath, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.ObjectURL(bucket, key), f)
	if err != nil {
		return err
	}
	req.ContentLength = stat.Size()
	c.signRequestWithHash(req, payloadHash)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("storage: put %s/%s: %w", bucket, key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("storage: put %s/%s: status %d: %s", bucket, key, resp.StatusCode, string(body))
	}
	return nil
}

// UploadDir recursively uploads every regular file under dir to bucket/key prefix.
func (c *Client) UploadDir(ctx context.Context, bucket, keyPrefix, dir string) error {
	return filepath.Walk(dir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(dir, filePath)
		if err != nil {
			return err
		}
		objectKey := path.Join(keyPrefix, filepath.ToSlash(rel))
		return c.PutFile(ctx, bucket, objectKey, filePath)
	})
}

// signRequest adds SigV4 headers using the hash of an empty body (for GET/HEAD/DELETE).
func (c *Client) signRequest(req *http.Request) {
	c.signRequestWithHash(req, sha256Hex([]byte{}))
}

// signRequestWithHash adds SigV4 headers using a caller-supplied payload hash.
// Use this for PUT/POST where the body is streamed and the hash is pre-computed.
func (c *Client) signRequestWithHash(req *http.Request, payloadHash string) {
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	dateStamp := amzDate[:8]
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signedHeadersList := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	sort.Strings(signedHeadersList)
	signedHeaders := strings.Join(signedHeadersList, ";")

	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		req.URL.Host, payloadHash, amzDate)

	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		canonicalizeQuery(req.URL.RawQuery),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, c.cfg.Region)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+c.cfg.SecretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(c.cfg.Region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.cfg.AccessKey, credentialScope, signedHeaders, signature,
	))
}

func canonicalizeQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	pairs := strings.Split(rawQuery, "&")
	encoded := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		if pair == "" {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		encoded = append(encoded, awsURIEncode(key, true)+"="+awsURIEncode(value, true))
	}
	sort.Strings(encoded)
	return strings.Join(encoded, "&")
}

func awsURIEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '.' || ch == '_' || ch == '~':
			b.WriteByte(ch)
		case ch == '/':
			if encodeSlash {
				b.WriteString("%2F")
			} else {
				b.WriteByte(ch)
			}
		default:
			b.WriteString(fmt.Sprintf("%%%02X", ch))
		}
	}
	return b.String()
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// ParseSourceURI extracts bucket and key from a storage URI. Supported schemes:
//
//	s3://bucket/key      (production contract)
//	rustfs://bucket/key  (alias)
//	file:// or plain /path (development only)
func ParseSourceURI(sourceURI string) (bucket, key string, err error) {
	u, err := url.Parse(sourceURI)
	if err != nil {
		return "", "", fmt.Errorf("parse source uri: %w", err)
	}
	switch u.Scheme {
	case "s3", "rustfs":
		return u.Host, strings.TrimPrefix(u.Path, "/"), nil
	case "", "file":
		return "", "", fmt.Errorf("local source %q is not accepted; upload to RustFS and pass s3://bucket/key", sourceURI)
	default:
		return "", "", fmt.Errorf("unsupported source scheme %q", u.Scheme)
	}
}

// HeadBucket verifies the bucket exists (used by readiness checks).
func (c *Client) HeadBucket(ctx context.Context, bucket string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fmt.Sprintf("%s/%s", strings.TrimRight(c.cfg.Endpoint, "/"), bucket), nil)
	if err != nil {
		return err
	}
	c.signRequest(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("storage: bucket %q not found", bucket)
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("storage: head bucket %q: status %d", bucket, resp.StatusCode)
	}
	return nil
}
