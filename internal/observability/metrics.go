package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequestsTotal           *prometheus.CounterVec
	HTTPRequestDurationSeconds  *prometheus.HistogramVec
	IntentsSubmittedTotal       *prometheus.CounterVec
	IntentStateTransitionsTotal *prometheus.CounterVec
	RPCRequestsTotal            *prometheus.CounterVec
	RPCLatencySeconds           *prometheus.HistogramVec
	WorkerInflightJobs          prometheus.Gauge
	ConfirmationLagBlocks       prometheus.Gauge
	ReorgsDetectedTotal         prometheus.Counter
}

func NewMetrics(service string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	constLabels := prometheus.Labels{}
	if service != "" {
		constLabels["service"] = service
	}

	m := &Metrics{
		Registry: reg,
		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "chainflow_http_requests_total",
			Help:        "Total HTTP requests handled.",
			ConstLabels: constLabels,
		}, []string{"method", "path", "status"}),
		HTTPRequestDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "chainflow_http_request_duration_seconds",
			Help:        "HTTP request latency in seconds.",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: constLabels,
		}, []string{"method", "path"}),
		IntentsSubmittedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "chainflow_intents_submitted_total",
			Help:        "Total transaction intents submitted by outcome.",
			ConstLabels: constLabels,
		}, []string{"outcome"}),
		IntentStateTransitionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "chainflow_intent_state_transitions_total",
			Help:        "Total intent state transitions.",
			ConstLabels: constLabels,
		}, []string{"from", "to"}),
		RPCRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "chainflow_rpc_requests_total",
			Help:        "Total EVM RPC requests by provider, method, and outcome.",
			ConstLabels: constLabels,
		}, []string{"provider", "method", "outcome"}),
		RPCLatencySeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "chainflow_rpc_latency_seconds",
			Help:        "EVM RPC call latency in seconds.",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: constLabels,
		}, []string{"provider", "method"}),
		WorkerInflightJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "chainflow_worker_inflight_jobs",
			Help:        "Current number of in-flight worker jobs.",
			ConstLabels: constLabels,
		}),
		ConfirmationLagBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "chainflow_confirmation_lag_blocks",
			Help:        "Latest observed confirmation lag in blocks.",
			ConstLabels: constLabels,
		}),
		ReorgsDetectedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "chainflow_reorgs_detected_total",
			Help:        "Total chain reorganizations detected.",
			ConstLabels: constLabels,
		}),
	}

	reg.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPRequestDurationSeconds,
		m.IntentsSubmittedTotal,
		m.IntentStateTransitionsTotal,
		m.RPCRequestsTotal,
		m.RPCLatencySeconds,
		m.WorkerInflightJobs,
		m.ConfirmationLagBlocks,
		m.ReorgsDetectedTotal,
	)

	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (m *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		dur := time.Since(start).Seconds()
		path := r.Pattern
		if path == "" {
			path = r.URL.Path
		}
		m.HTTPRequestsTotal.WithLabelValues(r.Method, path, strconv.Itoa(rec.status)).Inc()
		m.HTTPRequestDurationSeconds.WithLabelValues(r.Method, path).Observe(dur)
	})
}
