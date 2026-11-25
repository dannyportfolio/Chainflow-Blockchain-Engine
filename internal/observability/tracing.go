package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// InitTracing configures W3C trace context propagation and returns a named Tracer.
func InitTracing(serviceName string) trace.Tracer {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return otel.Tracer(serviceName)
}

// StartSpan starts a child span from ctx using the provided tracer.
func StartSpan(ctx context.Context, tracer trace.Tracer, name string) (context.Context, trace.Span) {
	if tracer == nil {
		tracer = otel.Tracer("chainflow")
	}
	return tracer.Start(ctx, name)
}
