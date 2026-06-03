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

// rateLimiterPool manages per-IP token bucket limiters that can be updated
// at runtime. This enables hot-reload of rate limits without restart.
type rateLimiterPool struct {
	mu      sync.Mutex
	clients map[string]*rate.Limiter
	rps     float64
	burst   int
}

func newRateLimiterPool(rps float64, burst int) *rateLimiterPool {
	if rps <= 0 {
		return &rateLimiterPool{} // unlimited
	}
	if burst <= 0 {
		burst = int(rps)
	}
	p := &rateLimiterPool{
		clients: make(map[string]*rate.Limiter),
		rps:     rps,
		burst:   burst,
	}
	go p.cleanupLoop()
	return p
}

// Update changes the rate limit for all existing and future IPs.
// rps <= 0 disables rate limiting.
func (p *rateLimiterPool) Update(rps float64, burst int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if rps <= 0 {
		p.clients = nil
		p.rps = 0
		p.burst = 0
		return
	}
	if burst <= 0 {
		burst = int(rps)
	}
	p.rps = rps
	p.burst = burst
	if p.clients == nil {
		p.clients = make(map[string]*rate.Limiter)
	}
	for _, lim := range p.clients {
		lim.SetLimit(rate.Limit(rps))
		lim.SetBurst(burst)
	}
}

// Middleware returns an HTTP middleware that enforces the per-IP rate limit.
func (p *rateLimiterPool) Middleware() func(http.Handler) http.Handler {
	if p == nil || p.rps <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			p.mu.Lock()
			lim, ok := p.clients[ip]
			if !ok {
				lim = rate.NewLimiter(rate.Limit(p.rps), p.burst)
				p.clients[ip] = lim
			}
			p.mu.Unlock()
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

func (p *rateLimiterPool) cleanupLoop() {
	for {
		time.Sleep(time.Minute)
		p.mu.Lock()
		if p.clients == nil {
			p.mu.Unlock()
			return
		}
		for ip, lim := range p.clients {
			if lim.Tokens() >= float64(p.burst) {
				delete(p.clients, ip)
			}
		}
		p.mu.Unlock()
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
