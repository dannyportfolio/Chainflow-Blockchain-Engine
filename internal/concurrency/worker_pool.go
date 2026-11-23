package concurrency

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/chainflow/chainflow/internal/messaging"
)

type MessageHandler interface {
	ProcessMessage(ctx context.Context, subject string, data []byte) ProcessResult
}

type WorkerPoolConfig struct {
	WorkerID        string
	Concurrency     int
	MaxFetchBatch   int
	FetchTimeout    time.Duration
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
}

type WorkerPool struct {
	consumer jetstream.Consumer
	handler  MessageHandler
	dlq      messaging.DLQPublisher
	cfg      WorkerPoolConfig
}

func NewWorkerPool(
	consumer jetstream.Consumer,
	handler MessageHandler,
	dlq messaging.DLQPublisher,
	cfg WorkerPoolConfig,
) *WorkerPool {
	if cfg.WorkerID == "" {
		cfg.WorkerID = "tx-worker-1"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 20
	}
	if cfg.MaxFetchBatch <= 0 || cfg.MaxFetchBatch > cfg.Concurrency {
		cfg.MaxFetchBatch = cfg.Concurrency
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = 2 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 25 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &WorkerPool{
		consumer: consumer,
		handler:  handler,
		dlq:      dlq,
		cfg:      cfg,
	}
}

func (p *WorkerPool) Run(ctx context.Context) error {
	sem := make(chan struct{}, p.cfg.Concurrency)
	var wg sync.WaitGroup

	defer func() {
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(p.cfg.ShutdownTimeout):
			p.cfg.Logger.Warn("worker pool shutdown timeout waiting for in-flight jobs")
		}
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Acquire at least one slot before pulling from JetStream so backpressure stays in the broker.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		// Claim any additional immediately available slots up to MaxFetchBatch.
		slots := 1
		for slots < p.cfg.MaxFetchBatch {
			select {
			case sem <- struct{}{}:
				slots++
			default:
				goto fetch
			}
		}

	fetch:
		batch, err := p.consumer.Fetch(slots, jetstream.FetchMaxWait(p.cfg.FetchTimeout))
		if err != nil {
			for i := 0; i < slots; i++ {
				<-sem
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				p.cfg.Logger.Warn("jetstream fetch failed", "error", err)
			}
			continue
		}

		var received int
		for msg := range batch.Messages() {
			received++
			m := msg
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				p.handleOne(ctx, m)
			}()
		}
		for i := received; i < slots; i++ {
			<-sem
		}

		if err := batch.Error(); err != nil && ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
			p.cfg.Logger.Warn("jetstream batch error", "error", err)
		}
	}
}

func (p *WorkerPool) handleOne(ctx context.Context, msg jetstream.Msg) {
	// Detach parent cancellation during final ack/dlq so an already-started tx finishes cleanly.
	jobCtx := context.WithoutCancel(ctx)
	res := p.handler.ProcessMessage(jobCtx, msg.Subject(), msg.Data())

	switch res.Action {
	case ActionAck:
		if err := msg.Ack(); err != nil {
			p.cfg.Logger.Warn("jetstream ack failed", "subject", msg.Subject(), "error", err)
		}
	case ActionNakDelay:
		delay := res.NakDelay
		if delay <= 0 {
			delay = 2 * time.Second
		}
		if err := msg.NakWithDelay(delay); err != nil {
			p.cfg.Logger.Warn("jetstream nak failed", "subject", msg.Subject(), "error", err)
		}
	case ActionDLQ:
		if p.dlq != nil {
			var deliveries uint64 = 1
			if meta, err := msg.Metadata(); err == nil && meta != nil {
				deliveries = meta.NumDelivered
			}
			_ = p.dlq.PublishDLQ(jobCtx, messaging.DLQEnvelope{
				EventID:             msg.Headers().Get(messaging.HeaderMsgID),
				EventType:           "tx.dlq.v1",
				OriginalSubject:     msg.Subject(),
				FailureReason:       res.Reason,
				DeliveriesAttempted: deliveries,
				LastWorkerID:        p.cfg.WorkerID,
				FailedAt:            time.Now().UTC().Format(time.RFC3339),
				Payload:             json.RawMessage(msg.Data()),
			})
		}
		if err := msg.Term(); err != nil {
			p.cfg.Logger.Warn("jetstream term failed", "subject", msg.Subject(), "error", err)
		}
	}
}
