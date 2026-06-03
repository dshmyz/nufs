package s3

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Middleware chain for the S3 gateway.

// RequestIDMiddleware adds a unique x-amz-request-id to each request.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := generateRequestID()
		w.Header().Set("x-amz-request-id", requestID)
		w.Header().Set("x-amz-id-2", requestID)
		next.ServeHTTP(w, r)
	})
}

// LoggingMiddleware logs each request with method, path, status, and duration.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		duration := time.Since(start)
		log.Printf("s3gw: %s %s %d %v %s",
			r.Method, r.URL.Path, sw.status, duration, r.RemoteAddr)
	})
}

// CORSMiddleware adds CORS headers for browser-based S3 clients.
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Amz-Date, X-Amz-Content-Sha256, x-amz-request-id")
		w.Header().Set("Access-Control-Expose-Headers", "ETag, x-amz-request-id, x-amz-id-2")
		w.Header().Set("Access-Control-Max-Age", "3600")

		// Handle preflight
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// AuthMiddleware verifies AWS Signature V4 credentials.
func AuthMiddleware(creds *CredentialStore, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := creds.VerifySignatureV4(r)
		if err != nil {
			requestID := w.Header().Get("x-amz-request-id")
			WriteXMLError(w, http.StatusForbidden, ErrCodeAccessDenied,
				err.Error(), r.URL.Path, requestID)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RecoveryMiddleware catches panics and returns 500.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("s3gw: panic recovered: %v", rec)
				requestID := w.Header().Get("x-amz-request-id")
				WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
					"Internal server error", r.URL.Path, requestID)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// RateLimitMiddleware limits requests per second per client IP using a
// per-IP token bucket. rps <= 0 means unlimited.
func RateLimitMiddleware(rps float64, burst int) func(http.Handler) http.Handler {
	if rps <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	if burst <= 0 {
		burst = int(rps)
	}
	var mu sync.Mutex
	clients := make(map[string]*rate.Limiter)
	// Background cleanup of stale limiters every minute.
	go func() {
		for {
			time.Sleep(time.Minute)
			mu.Lock()
			for ip, lim := range clients {
				if lim.Tokens() >= float64(burst) {
					delete(clients, ip)
				}
			}
			mu.Unlock()
		}
	}()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			mu.Lock()
			lim, ok := clients[ip]
			if !ok {
				lim = rate.NewLimiter(rate.Limit(rps), burst)
				clients[ip] = lim
			}
			mu.Unlock()
			if !lim.Allow() {
				requestID := w.Header().Get("x-amz-request-id")
				WriteXMLError(w, http.StatusTooManyRequests, ErrCodeSlowDown,
					"Rate limit exceeded", r.URL.Path, requestID)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Chain applies middlewares in order (outermost first).
func Chain(handler http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// statusWriter wraps ResponseWriter to capture the status code.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func generateRequestID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
