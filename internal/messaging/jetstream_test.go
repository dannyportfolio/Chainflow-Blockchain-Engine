package messaging

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/chainflow/chainflow/internal/application"
)

func startEmbeddedNATS(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()

	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("natsserver.NewServer: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("embedded NATS server did not become ready")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return nc, js
}

func TestJetStream_EnsureStreamsDedupAndConsume(t *testing.T) {
	t.Parallel()

	_, js := startEmbeddedNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := EnsureStreams(ctx, js, StreamConfig{Storage: jetstream.MemoryStorage}); err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}
	// Idempotent second call must succeed.
	if err := EnsureStreams(ctx, js, StreamConfig{Storage: jetstream.MemoryStorage}); err != nil {
		t.Fatalf("EnsureStreams second call: %v", err)
	}

	pub := NewJetStreamPublisher(js)
	ev := application.OutboxEvent{
		EventID:      "01934a1b-0000-7000-8000-000000000001",
		Subject:      SubjectDispatch,
		EventVersion: 1,
		TraceParent:  "req-123",
		Payload:      []byte(`{"intent_id":"intent-1"}`),
	}
	if err := pub.Publish(ctx, ev); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	// Duplicate publish with identical EventID should be deduplicated by JetStream.
	if err := pub.Publish(ctx, ev); err != nil {
		t.Fatalf("duplicate Publish: %v", err)
	}

	cons, err := EnsureWorkerConsumer(
		ctx,
		js,
		DefaultTXStream,
		"tx-worker-test",
		[]string{SubjectDispatch, SubjectRetry},
		10*time.Second,
		5,
	)
	if err != nil {
		t.Fatalf("EnsureWorkerConsumer: %v", err)
	}

	batch, err := cons.Fetch(5, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatalf("cons.Fetch: %v", err)
	}

	var count int
	for msg := range batch.Messages() {
		count++
		if got := msg.Headers().Get(HeaderTraceParent); got != "req-123" {
			t.Fatalf("traceparent header = %q, want req-123", got)
		}
		if err := msg.Ack(); err != nil {
			t.Fatalf("msg.Ack: %v", err)
		}
	}
	if batch.Error() != nil {
		t.Fatalf("batch.Error: %v", batch.Error())
	}
	if count != 1 {
		t.Fatalf("expected 1 deduplicated message, got %d", count)
	}

	if err := pub.PublishDLQ(ctx, DLQEnvelope{
		EventID:             ev.EventID,
		EventType:           "tx.dlq.v1",
		OriginalSubject:     SubjectDispatch,
		FailureReason:       "permanent_error",
		DeliveriesAttempted: 1,
		LastWorkerID:        "worker-1",
		FailedAt:            time.Now().UTC().Format(time.RFC3339),
		Payload:             json.RawMessage(ev.Payload),
	}); err != nil {
		t.Fatalf("PublishDLQ: %v", err)
	}
}
