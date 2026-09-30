package storage

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// ConfigFromEnv loads RustFS/S3 settings from standard environment variables,
// matching the defaults used by docker-compose.
func ConfigFromEnv() Config {
	return Config{
		Endpoint:     envString("RUSTFS_URL", "http://localhost:9000"),
		Region:       envString("RUSTFS_REGION", "us-east-1"),
		AccessKey:    envString("RUSTFS_ACCESS_KEY", "CzMPuN6RETJUju6t70GF"),
		SecretKey:    envString("RUSTFS_SECRET_KEY", "DH7ovwEKN7yj5Z7YRPRDapZZFKgjiU2ha5Ql07C0"),
		Bucket:       envString("RUSTFS_BUCKET", "uploads"),
		UsePathStyle: true,
	}
}

// ParseTTLSeconds reads a duration from an env var, falling back to def.
func ParseTTLSeconds(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return def
}

func envString(name, def string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return def
}
