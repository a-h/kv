package natspusher

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"maps"

	"github.com/a-h/kv"
	"github.com/nats-io/nats.go"
)

// Publisher defines the interface for publishing messages to NATS.
type Publisher interface {
	PublishMsg(msg *nats.Msg) error
}

// Config configures the NATS pusher.
type Config struct {
	// SubjectPrefix is the subject prefix for published messages.
	SubjectPrefix string
	// Logger is used for structured logging.
	Logger *slog.Logger
	// MaxRetries is the number of retries for failed publishes.
	MaxRetries int
	// Headers are added to NATS messages.
	Headers nats.Header
}

// Pusher consumes records from a KV store stream and publishes them to NATS.
type Pusher struct {
	config    Config
	publisher Publisher
	consumer  *kv.StreamConsumer
	logger    *slog.Logger
}

// New creates a new NATS pusher with an existing NATS connection and stream consumer.
func New(nc *nats.Conn, consumer *kv.StreamConsumer, config Config) (*Pusher, error) {
	if nc == nil {
		return nil, fmt.Errorf("NATS connection cannot be nil")
	}
	return NewWithPublisher(nc, consumer, config)
}

// NewWithPublisher creates a new NATS pusher with a custom publisher and stream consumer.
// This constructor is useful for testing with mock publishers.
func NewWithPublisher(publisher Publisher, consumer *kv.StreamConsumer, config Config) (*Pusher, error) {
	if publisher == nil {
		return nil, fmt.Errorf("publisher cannot be nil")
	}
	if consumer == nil {
		return nil, fmt.Errorf("stream consumer cannot be nil")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	return &Pusher{
		config:    config,
		publisher: publisher,
		consumer:  consumer,
		logger:    config.Logger,
	}, nil
}

// Run starts consuming from the KV store stream and publishing to NATS.
// This blocks until the context is canceled.
func (p *Pusher) Run(ctx context.Context) error {
	p.logger.Info("Starting NATS pusher")

	var processed int
loop:
	for {
		if err := ctx.Err(); err != nil {
			p.logger.Info("Context canceled, stopping pusher")
			return nil
		}

		// Get a batch of records without committing
		records, waitFor, err := p.consumer.Get(ctx, 5*time.Minute, 10)
		if err != nil {
			if ctx.Err() != nil {
				p.logger.Info("Context canceled while getting records")
				return nil
			}
			p.logger.Error("Error getting records from stream", slog.String("error", err.Error()))
			return err
		}

		if len(records) == 0 {
			p.logger.Debug("No records available, waiting", slog.Duration("waitFor", waitFor))
			time.Sleep(waitFor)
			continue loop
		}

		p.logger.Debug("Processing batch", slog.Int("count", len(records)))
		var lastSeq int
		for _, record := range records {
			if err := p.publishRecord(ctx, record); err != nil {
				p.logger.Error("Failed to publish record after all retries",
					slog.Int("seq", record.Seq),
					slog.String("key", record.Record.Key),
					slog.String("error", err.Error()))
				// Return error without committing - records will be redelivered
				return fmt.Errorf("failed to publish record seq=%d key=%s: %w", record.Seq, record.Record.Key, err)
			}

			processed++
			lastSeq = record.Seq
			p.logger.Debug("Published record",
				slog.Int("seq", record.Seq),
				slog.String("key", record.Record.Key),
				slog.String("action", string(record.Action)),
				slog.String("type", record.Record.Type))
		}

		// Only commit if all records in the batch were successfully published.
		if err := p.consumer.CommitUpTo(ctx, lastSeq); err != nil {
			p.logger.Error("Failed to commit processed records",
				slog.Int("lastSeq", lastSeq),
				slog.String("error", err.Error()))
			return fmt.Errorf("failed to commit up to seq=%d: %w", lastSeq, err)
		}
		p.logger.Debug("Committed batch",
			slog.Int("count", len(records)),
			slog.Int("lastSeq", lastSeq))
	}
}

// publishRecord publishes a single stream record to NATS.
func (p *Pusher) publishRecord(ctx context.Context, record kv.StreamRecord) error {
	subject := p.buildSubject(record.Record.Key, record.Record.Type)

	// Use just the record value as the payload.
	data := record.Record.Value

	// Create NATS message with headers.
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  make(nats.Header),
	}

	// Add configured headers.
	maps.Copy(msg.Header, p.config.Headers)

	// Add standard headers.
	msg.Header.Set("kv-seq", fmt.Sprintf("%d", record.Seq))
	msg.Header.Set("kv-action", string(record.Action))
	msg.Header.Set("kv-type", record.Record.Type)
	msg.Header.Set("kv-key", record.Record.Key)
	msg.Header.Set("kv-version", fmt.Sprintf("%d", record.Record.Version))
	msg.Header.Set("kv-created", record.Record.Created.Format(time.RFC3339))

	// Publish with retries.
	var lastErr error
	for attempt := 0; attempt <= p.config.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}

		err := p.publisher.PublishMsg(msg)
		if err == nil {
			return nil
		}
		lastErr = err
		p.logger.Warn("Failed to publish, retrying",
			slog.Int("attempt", attempt+1),
			slog.Int("maxRetries", p.config.MaxRetries),
			slog.String("error", err.Error()))
	}

	return fmt.Errorf("failed to publish after %d attempts: %w", p.config.MaxRetries+1, lastErr)
}

// buildSubject constructs the NATS subject from the key and type.
// It converts key separators to dots and appends the type.
func (p *Pusher) buildSubject(key, recordType string) string {
	// Replace common separators with dots for NATS subject hierarchy.
	subject := strings.ReplaceAll(key, "/", ".")
	subject = strings.ReplaceAll(subject, ":", ".")
	subject = strings.ReplaceAll(subject, "_", ".")

	// Remove any double dots.
	for strings.Contains(subject, "..") {
		subject = strings.ReplaceAll(subject, "..", ".")
	}

	// Trim leading/trailing dots.
	subject = strings.Trim(subject, ".")

	// Build final subject with prefix and type.
	var parts []string
	if p.config.SubjectPrefix != "" {
		parts = append(parts, p.config.SubjectPrefix)
	}
	if subject != "" {
		parts = append(parts, subject)
	}
	parts = append(parts, recordType)

	return strings.Join(parts, ".")
}

// Status returns the current status of the pusher's consumer.
func (p *Pusher) Status(ctx context.Context) (kv.StreamConsumerStatus, bool, error) {
	if p.consumer == nil {
		return kv.StreamConsumerStatus{}, false, fmt.Errorf("pusher not started")
	}
	return p.consumer.Status(ctx)
}

// Delete removes the pusher's consumer from the store.
func (p *Pusher) Delete(ctx context.Context) error {
	if p.consumer == nil {
		return fmt.Errorf("pusher not started")
	}
	return p.consumer.Delete(ctx)
}
