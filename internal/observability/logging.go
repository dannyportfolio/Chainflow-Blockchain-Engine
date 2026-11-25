package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// NewLogger creates a structured JSON logger with the configured level and service name.
func NewLogger(level string, service string, out io.Writer) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(out, &slog.HandlerOptions{Level: lvl})
	logger := slog.New(handler)
	if service != "" {
		logger = logger.With("service", service)
	}
	return logger
}

// WithTrace enriches a logger with trace_id and span_id when an active span exists in ctx.
func WithTrace(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return logger
	}
	return logger.With(
		"trace_id", sc.TraceID().String(),
		"span_id", sc.SpanID().String(),
	)
}
