package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"

	notifapp "github.com/companyofcreators/notification-service/internal/application/notification"
	"github.com/companyofcreators/notification-service/internal/config"
	domain "github.com/companyofcreators/notification-service/internal/domain/notification"
	dbinfra "github.com/companyofcreators/notification-service/internal/infrastructure/db"
	kafkainfra "github.com/companyofcreators/notification-service/internal/infrastructure/kafka"
	wsHub "github.com/companyofcreators/notification-service/internal/infrastructure/ws"
	httphandler "github.com/companyofcreators/notification-service/internal/interfaces/http"
	wshandler "github.com/companyofcreators/notification-service/internal/interfaces/ws"
	pkg "github.com/companyofcreators/notification-service/internal/pkg"
)

// Container holds all the wired dependencies for the notification service.
type Container struct {
	Config        *config.Config
	Log           *slog.Logger
	Pool          *sqlx.DB
	Hub           *wsHub.Hub
	KafkaProducer *kafkainfra.Producer
	KafkaConsumer *kafkainfra.Consumer

	// Repositories
	NotificationRepo domain.NotificationRepository

	// Use cases
	ProcessEvent *notifapp.ProcessEvent
	Deliver      *notifapp.Deliver
	List         *notifapp.List

	// HTTP handlers
	NotificationHandler *httphandler.NotificationHandler
	WSHandler           *wshandler.WSHandler
}

// NewContainer initializes all dependencies and returns a configured Container.
func NewContainer(ctx context.Context) (*Container, error) {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	// Initialize logger
	log, err := pkg.NewLogger(cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("create logger: %w", err)
	}

	log.InfoContext(ctx, "starting notification service",
		"http_address", cfg.HTTPAddress,
		"kafka_group", cfg.ConsumerGroup,
	)

	// Connect to PostgreSQL
	pool, err := dbinfra.NewPostgresPool(ctx, dbinfra.DefaultPostgresConfig(cfg.DBDSN), log)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	// Initialize WebSocket hub
	hub := wsHub.NewHub(log)

	// Initialize Kafka producer
	producer := kafkainfra.NewProducer(kafkainfra.ProducerConfig{
		Brokers: cfg.KafkaBrokersList(),
	}, log)

	// Initialize Kafka consumer
	consumer := kafkainfra.NewConsumer(kafkainfra.ConsumerConfig{
		Brokers:       cfg.KafkaBrokersList(),
		ConsumerGroup: cfg.ConsumerGroup,
	}, log)

	// Initialize repositories
	notificationRepo := dbinfra.NewNotificationRepo(pool, log)

	// Initialize use cases
	processEvent := notifapp.NewProcessEvent(notificationRepo, producer, hub, log)
	deliver := notifapp.NewDeliver(notificationRepo, hub, log)
	list := notifapp.NewList(notificationRepo, log)

	// Initialize HTTP handlers
	notificationHandler := httphandler.NewNotificationHandler(list, deliver, log)
	wsHandler := wshandler.NewWSHandler(hub, log)

	return &Container{
		Config:              cfg,
		Log:                 log,
		Pool:                pool,
		Hub:                 hub,
		KafkaProducer:       producer,
		KafkaConsumer:       consumer,
		NotificationRepo:    notificationRepo,
		ProcessEvent:        processEvent,
		Deliver:             deliver,
		List:                list,
		NotificationHandler: notificationHandler,
		WSHandler:           wsHandler,
	}, nil
}

// Shutdown gracefully shuts down all dependencies.
func (c *Container) Shutdown() {
	c.Log.InfoContext(context.Background(), "shutting down notification service")

	// Stop Kafka consumers first
	c.KafkaConsumer.Shutdown()

	// Close WebSocket connections
	c.Hub.CloseAll()

	// Close Kafka producer
	if err := c.KafkaProducer.Close(); err != nil {
		c.Log.ErrorContext(context.Background(), "failed to close kafka producer", "error", err.Error())
	}

	// Close database pool
	c.Pool.Close()
}
