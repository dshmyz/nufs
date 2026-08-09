// Package tracing provides OpenTelemetry tracing initialization and helpers.
// It supports OTLP gRPC export and a no-op fallback when tracing is disabled.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds tracing configuration.
type Config struct {
	Enabled  bool   // Enable distributed tracing
	Endpoint string // OTLP gRPC endpoint (e.g. "localhost:4317")
	Service  string // Service name for resource attributes
	Insecure bool   // Use insecure gRPC connection (dev only)
}

// ShutdownFunc shuts down the tracing provider gracefully.
type ShutdownFunc func(ctx context.Context) error

// Init initializes the OpenTelemetry tracer provider.
// Returns a shutdown function that must be called on application exit.
// If Config.Enabled is false, returns a no-op tracer and shutdown.
func Init(cfg Config) (trace.Tracer, ShutdownFunc, error) {
	if !cfg.Enabled {
		return trace.NewNoopTracerProvider().Tracer("nufs"),
			func(_ context.Context) error { return nil },
			nil
	}

	if cfg.Endpoint == "" {
		return nil, nil, fmt.Errorf("tracing: endpoint is required when tracing is enabled")
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("tracing: create OTLP exporter: %w", err)
	}

	res, err := newResource(cfg.Service)
	if err != nil {
		return nil, nil, fmt.Errorf("tracing: create resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := provider.Tracer("github.com/dshmyz/nufs/nufs-core",
		trace.WithInstrumentationVersion("1.0.0"),
	)

	shutdown := func(ctx context.Context) error {
		slog.Info("tracing: shutting down provider")
		return provider.Shutdown(ctx)
	}

	slog.Info("tracing: initialized", "service", cfg.Service, "endpoint", cfg.Endpoint)
	return tracer, shutdown, nil
}

func newResource(serviceName string) (*sdkresource.Resource, error) {
	return sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		),
	)
}

// StartSpan starts a new span with the given name and options.
// Returns the context with the span and a function to end the span.
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracer := otel.Tracer("github.com/dshmyz/nufs/nufs-core")
	return tracer.Start(ctx, name, opts...)
}

// HTTPMiddleware returns an HTTP middleware that extracts trace context
// from incoming requests and creates a span for each request.
func HTTPMiddleware(tracer trace.Tracer) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			ctx, span := tracer.Start(ctx, r.Method+" "+r.URL.Path,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPRequestMethodKey.String(r.Method),
					semconv.URLFull(r.URL.String()),
					semconv.ServerAddress(r.Host),
				),
			)
			defer span.End()

			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}

// InjectTraceHeaders injects trace context into outgoing HTTP request headers.
func InjectTraceHeaders(ctx context.Context, req *http.Request) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
}
