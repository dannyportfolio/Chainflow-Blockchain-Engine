package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/chainflow/chainflow/internal/application"
)

const (
	DefaultTXStream  = "CHAINFLOW_TX"
	DefaultDLQStream = "CHAINFLOW_DLQ"

	SubjectDispatch      = "chainflow.v1.tx.dispatch"
	SubjectRetry         = "chainflow.v1.tx.retry"
	SubjectStatusChanged = "chainflow.v1.tx.status_changed"
	SubjectDLQTX         = "chainflow.v1.dlq.tx"

	HeaderMsgID         = "Nats-Msg-Id"
	HeaderTraceParent   = "traceparent"
	HeaderSchemaVersion = "schema_version"
)

type EventPublisher interface {
	Publish(ctx context.Context, ev application.OutboxEvent) error
}

type DLQPublisher interface {
	PublishDLQ(ctx context.Context, env DLQEnvelope) error
}

type DispatchEnvelope struct {
	EventID     string `json:"event_id"`
	EventType   string `json:"event_type"`
	OccurredAt  string `json:"occurred_at"`
	IntentID    string `json:"intent_id"`
	TenantID    string `json:"tenant_id"`
	ChainID     int64  `json:"chain_id"`
	FromAddress string `json:"from_address"`
	Priority    int16  `json:"priority"`
}

type RetryEnvelope struct {
	EventID             string `json:"event_id"`
	EventType           string `json:"event_type"`
	OccurredAt          string `json:"occurred_at"`
	IntentID            string `json:"intent_id"`
	AttemptNumber       int    `json:"attempt_number"`
	RetryReason         string `json:"retry_reason"`
	PreviousExecutionID string `json:"previous_execution_id,omitempty"`
	ChainID             int64  `json:"chain_id"`
}

type DLQEnvelope struct {
	EventID             string          `json:"event_id"`
	EventType           string          `json:"event_type"`
	OriginalSubject     string          `json:"original_subject"`
	FailureReason       string          `json:"failure_reason"`
	DeliveriesAttempted uint64          `json:"deliveries_attempted"`
	LastWorkerID        string          `json:"last_worker_id"`
	FailedAt            string          `json:"failed_at"`
	Payload             json.RawMessage `json:"payload"`
}

type StreamConfig struct {
	TXStreamName  string
	DLQStreamName string
	Storage       jetstream.StorageType
	Replicas      int
	DedupWindow   time.Duration
	TXMaxAge      time.Duration
	DLQMaxAge     time.Duration
}

func (c StreamConfig) withDefaults() StreamConfig {
	if c.TXStreamName == "" {
		c.TXStreamName = DefaultTXStream
	}
	if c.DLQStreamName == "" {
		c.DLQStreamName = DefaultDLQStream
	}
	if c.Replicas <= 0 {
		c.Replicas = 1
	}
	if c.DedupWindow <= 0 {
		c.DedupWindow = 24 * time.Hour
	}
	if c.TXMaxAge <= 0 {
		c.TXMaxAge = 7 * 24 * time.Hour
	}
	if c.DLQMaxAge <= 0 {
		c.DLQMaxAge = 30 * 24 * time.Hour
	}
	return c
}

func EnsureStreams(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) error {
	if js == nil {
		return errors.New("jetstream instance must not be nil")
	}
	cfg = cfg.withDefaults()

	txCfg := jetstream.StreamConfig{
		Name:       cfg.TXStreamName,
		Subjects:   []string{"chainflow.v1.tx.>"},
		Retention:  jetstream.LimitsPolicy,
		Storage:    cfg.Storage,
		Replicas:   cfg.Replicas,
		Duplicates: cfg.DedupWindow,
		MaxAge:     cfg.TXMaxAge,
	}
	if _, err := js.CreateOrUpdateStream(ctx, txCfg); err != nil {
		return fmt.Errorf("ensure tx stream %s: %w", cfg.TXStreamName, err)
	}

	dlqCfg := jetstream.StreamConfig{
		Name:      cfg.DLQStreamName,
		Subjects:  []string{"chainflow.v1.dlq.>"},
		Retention: jetstream.LimitsPolicy,
		Storage:   cfg.Storage,
		Replicas:  cfg.Replicas,
		MaxAge:    cfg.DLQMaxAge,
	}
	if _, err := js.CreateOrUpdateStream(ctx, dlqCfg); err != nil {
		return fmt.Errorf("ensure dlq stream %s: %w", cfg.DLQStreamName, err)
	}
	return nil
}

func EnsureWorkerConsumer(
	ctx context.Context,
	js jetstream.JetStream,
	streamName string,
	consumerName string,
	filterSubjects []string,
	ackWait time.Duration,
	maxDeliver int,
) (jetstream.Consumer, error) {
	if js == nil {
		return nil, errors.New("jetstream instance must not be nil")
	}
	if streamName == "" {
		streamName = DefaultTXStream
	}
	if ackWait <= 0 {
		ackWait = 45 * time.Second
	}
	if maxDeliver <= 0 {
		maxDeliver = 5
	}
	cfg := jetstream.ConsumerConfig{
		Durable:        consumerName,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        ackWait,
		MaxDeliver:     maxDeliver,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		FilterSubjects: filterSubjects,
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, cfg)
	if err != nil {
		return nil, fmt.Errorf("ensure consumer %s on %s: %w", consumerName, streamName, err)
	}
	return cons, nil
}

type JetStreamPublisher struct {
	js jetstream.JetStream
}

func NewJetStreamPublisher(js jetstream.JetStream) *JetStreamPublisher {
	return &JetStreamPublisher{js: js}
}

func (p *JetStreamPublisher) Publish(ctx context.Context, ev application.OutboxEvent) error {
	if p.js == nil {
		return errors.New("jetstream publisher not initialized")
	}
	msg := &nats.Msg{
		Subject: ev.Subject,
		Data:    ev.Payload,
		Header:  nats.Header{},
	}
	if ev.EventID != "" {
		msg.Header.Set(HeaderMsgID, ev.EventID)
	}
	if ev.TraceParent != "" {
		msg.Header.Set(HeaderTraceParent, ev.TraceParent)
	}
	ver := ev.EventVersion
	if ver <= 0 {
		ver = 1
	}
	msg.Header.Set(HeaderSchemaVersion, strconv.Itoa(int(ver)))

	if _, err := p.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("jetstream publish %s (%s): %w", ev.Subject, ev.EventID, err)
	}
	return nil
}

func (p *JetStreamPublisher) PublishDLQ(ctx context.Context, env DLQEnvelope) error {
	if p.js == nil {
		return errors.New("jetstream publisher not initialized")
	}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal dlq envelope: %w", err)
	}
	msg := &nats.Msg{
		Subject: SubjectDLQTX,
		Data:    data,
		Header:  nats.Header{},
	}
	if env.EventID != "" {
		msg.Header.Set(HeaderMsgID, "dlq-"+env.EventID)
	}
	msg.Header.Set(HeaderSchemaVersion, "1")
	if _, err := p.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("jetstream publish dlq: %w", err)
	}
	return nil
}
