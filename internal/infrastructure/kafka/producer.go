package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer publishes outgoing messages to Kafka topics.
type Producer struct {
	emailWriter   *kafka.Writer
	pushWriter    *kafka.Writer
	createdWriter *kafka.Writer
	log           *slog.Logger
}

// ProducerConfig holds configuration for the Kafka producer.
type ProducerConfig struct {
	Brokers []string
}

// EmailMessage is the message published to the notification.email topic.
type EmailMessage struct {
	ToEmail      string          `json:"to_email"`
	UserID       string          `json:"user_id"`
	Subject      string          `json:"subject"`
	Body         string          `json:"body"`
	TemplateType string          `json:"template_type"`
	Data         json.RawMessage `json:"data"`
}

// PushMessage is the message published to the notification.push topic.
type PushMessage struct {
	UserID string          `json:"user_id"`
	Title  string          `json:"title"`
	Body   string          `json:"body"`
	Data   json.RawMessage `json:"data"`
}

// NotificationCreatedMessage is published to notification.created after a notification is persisted.
type NotificationCreatedMessage struct {
	NotificationID string          `json:"notification_id"`
	UserID         string          `json:"user_id"`
	Type           string          `json:"type"`
	Channels       []string        `json:"channels"`
	Data           json.RawMessage `json:"data,omitempty"`
}

// NewProducer creates a new Producer with writers for outgoing topics.
func NewProducer(cfg ProducerConfig, log *slog.Logger) *Producer {
	emailWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  "notification.email",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		BatchSize:              100,
		BatchTimeout:           100 * time.Millisecond,
		MaxAttempts:            3,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
	}

	pushWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  "notification.push",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		BatchSize:              100,
		BatchTimeout:           100 * time.Millisecond,
		MaxAttempts:            3,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
	}

	createdWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  "notification.created",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		BatchSize:              100,
		BatchTimeout:           100 * time.Millisecond,
		MaxAttempts:            3,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
	}

	log.InfoContext(context.Background(), "kafka producer created",
		"brokers", cfg.Brokers,
		"topics", []string{"notification.email", "notification.push", "notification.created"},
	)

	return &Producer{
		emailWriter:   emailWriter,
		pushWriter:    pushWriter,
		createdWriter: createdWriter,
		log:           log,
	}
}

// PublishEmail sends an email delivery command to the notification.email topic.
func (p *Producer) PublishEmail(ctx context.Context, msg EmailMessage) error {
	key := msg.ToEmail
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal email message: %w", err)
	}

	err = p.emailWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		p.log.ErrorContext(ctx, "failed to publish email message", "error", err.Error())
		return fmt.Errorf("publish email: %w", err)
	}
	return nil
}

// PublishPush sends a push notification command to the notification.push topic.
func (p *Producer) PublishPush(ctx context.Context, msg PushMessage) error {
	key := msg.UserID
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal push message: %w", err)
	}

	err = p.pushWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		p.log.ErrorContext(ctx, "failed to publish push message", "error", err.Error())
		return fmt.Errorf("publish push: %w", err)
	}
	return nil
}

// PublishNotificationCreated sends a notification.created event.
func (p *Producer) PublishNotificationCreated(ctx context.Context, msg NotificationCreatedMessage) error {
	key := msg.NotificationID
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal notification created message: %w", err)
	}

	err = p.createdWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		p.log.ErrorContext(ctx, "failed to publish notification.created", "error", err.Error())
		return fmt.Errorf("publish notification.created: %w", err)
	}
	return nil
}

// Close shuts down all Kafka writers gracefully.
func (p *Producer) Close() error {
	var errs []error
	if err := p.emailWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close email writer: %w", err))
	}
	if err := p.pushWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close push writer: %w", err))
	}
	if err := p.createdWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close created writer: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("close producer: %v", errs)
	}
	p.log.InfoContext(context.Background(), "kafka producer closed")
	return nil
}
