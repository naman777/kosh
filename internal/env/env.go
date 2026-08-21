// Package env reads configuration from environment variables.
package env

import (
	"os"
	"time"
)

// String returns the variable or def.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Duration parses the variable (e.g. "30s") or returns def.
func Duration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
