package s3fs

import (
	"math"
	"math/rand"
	"time"
)

const (
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 10 * time.Second
	maxRetries     = 5
)

func retryWithBackoff(fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if attempt < maxRetries {
			backoff := initialBackoff * time.Duration(math.Pow(2, float64(attempt)))
			jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
			time.Sleep(backoff + jitter)
			metricsIncS3Retry()
		}
	}
	return lastErr
}
