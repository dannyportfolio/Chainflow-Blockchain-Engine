package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chainflow/chainflow/internal/observability"
)

func TestNewLogger_JSONFormatAndLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := observability.NewLogger("warn", "api", &buf)
	logger.Info("ignored info")
	logger.Warn("visible warning", "intent_id", "123")

	var decoded map[string]any
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid json log line, got %q: %v", buf.String(), err)
	}
	if decoded["service"] != "api" || decoded["msg"] != "visible warning" {
		t.Fatalf("unexpected log payload: %v", decoded)
	}
}

func TestMetrics_HandlerAndMiddleware(t *testing.T) {
	t.Parallel()

	m := observability.NewMetrics("api")
	m.IntentsSubmittedTotal.WithLabelValues("accepted").Inc()
	m.ReorgsDetectedTotal.Inc()

	wrapped := m.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	m.Handler().ServeHTTP(metricsRec, metricsReq)
	if metricsRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /metrics, got %d", metricsRec.Code)
	}
	body := metricsRec.Body.String()
	if !strings.Contains(body, "chainflow_http_requests_total") || !strings.Contains(body, "chainflow_intents_submitted_total") {
		t.Fatalf("missing expected metrics in output:\n%s", body)
	}
}

func TestInitTracing_StartsSpan(t *testing.T) {
	t.Parallel()

	tracer := observability.InitTracing("chainflow-test")
	_, span := observability.StartSpan(context.Background(), tracer, "test-op")
	defer span.End()
}
