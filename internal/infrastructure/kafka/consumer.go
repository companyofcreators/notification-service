package kafka

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageHandler is the interface for processing incoming Kafka messages.
// Implementations handle routing based on topic and parsing the message value.
type MessageHandler interface {
	Process(ctx context.Context, topic string, key, value []byte) error
}

// ConsumerConfig holds configuration for Kafka consumers.
type ConsumerConfig struct {
	Brokers       []string
	ConsumerGroup string
}

// Consumer manages multiple Kafka topic consumers within a single consumer group.
type Consumer struct {
	readers  []*kafka.Reader
	log      *slog.Logger
	shutdown chan struct{}
	wg       sync.WaitGroup
}

// topicConfig defines a topic to consume and how many handler goroutines to spawn.
type topicConfig struct {
	topic    string
	handlers int
}

// All consumed topics. Each topic uses the same consumer group for parallel processing.
var consumedTopics = []topicConfig{
	{topic: "offer.created", handlers: 2},
	{topic: "offer.accepted", handlers: 2},
	{topic: "offer.rejected", handlers: 2},
	{topic: "offer.withdrawn", handlers: 2},
	{topic: "offer.countered", handlers: 2},
	{topic: "chat.message.sent", handlers: 2},
	{topic: "order.created", handlers: 2},
	{topic: "order.assigned", handlers: 2},
	{topic: "order.completed", handlers: 2},
	{topic: "order.cancelled", handlers: 2},
	{topic: "review.created", handlers: 2},
	{topic: "complaint.created", handlers: 2},
	{topic: "complaint.updated", handlers: 2},
	{topic: "user.verification.created", handlers: 2},
	{topic: "user.created", handlers: 2},
}

// NewConsumer creates a new Consumer that listens to all configured topics.
func NewConsumer(cfg ConsumerConfig, log *slog.Logger) *Consumer {
	c := &Consumer{
		readers:  make([]*kafka.Reader, 0, len(consumedTopics)),
		log:      log,
		shutdown: make(chan struct{}),
	}

	for _, tc := range consumedTopics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.Brokers,
			Topic:       tc.topic,
			GroupID:     cfg.ConsumerGroup,
			MinBytes:    1,                 // 1 byte for immediate wake-up
			MaxBytes:               10e6,  // 10MB
			MaxWait:                500 * time.Millisecond,
			StartOffset:            kafka.LastOffset,
		})
		c.readers = append(c.readers, reader)
	}

	log.InfoContext(context.Background(), "kafka consumer created",
		"consumer_group", cfg.ConsumerGroup,
		"topic_count", len(consumedTopics),
	)

	return c
}

// Start begins consuming messages from all configured topics.
// Each topic gets its own reader and handler goroutines.
func (c *Consumer) Start(ctx context.Context, processor MessageHandler) {
	for i, reader := range c.readers {
		tc := consumedTopics[i]
		for j := 0; j < tc.handlers; j++ {
			c.wg.Add(1)
			go c.consumeLoop(ctx, reader, processor)
		}
	}

	c.log.InfoContext(context.Background(), "kafka consumers started",
		"total_goroutines", totalHandlerGoroutines(),
	)
}

// totalHandlerGoroutines calculates total number of handler goroutines.
func totalHandlerGoroutines() int {
	total := 0
	for _, tc := range consumedTopics {
		total += tc.handlers
	}
	return total
}

// consumeLoop runs a single message processing loop for a given reader.
func (c *Consumer) consumeLoop(ctx context.Context, reader *kafka.Reader, processor MessageHandler) {
	defer c.wg.Done()
	for {
		select {
		case <-c.shutdown:
			return
		case <-ctx.Done():
			return
		default:
		}

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.ErrorContext(ctx, "failed to read kafka message", "error", err.Error(), "topic", reader.Config().Topic)
			continue
		}

		c.log.DebugContext(ctx, "received kafka message",
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)

		if err := processor.Process(ctx, msg.Topic, msg.Key, msg.Value); err != nil {
			c.log.ErrorContext(ctx, "failed to process kafka event",
				"topic", msg.Topic,
				"error", err.Error(),
			)
		}
	}
}

// Shutdown gracefully stops all consumer goroutines and closes Kafka readers.
func (c *Consumer) Shutdown() {
	close(c.shutdown)
	c.wg.Wait()

	for _, reader := range c.readers {
		if err := reader.Close(); err != nil {
			c.log.ErrorContext(context.Background(), "failed to close kafka reader", "error", err.Error())
		}
	}

	c.log.InfoContext(context.Background(), "kafka consumers shut down")
}
