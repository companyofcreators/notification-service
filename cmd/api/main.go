package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/companyofcreators/notification-service/internal/app"
	httphandler "github.com/companyofcreators/notification-service/internal/interfaces/http"
)

func main() {
	ctx := context.Background()

	// Initialize the dependency container
	container, err := app.NewContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize container: %v\n", err)
		os.Exit(1)
	}

	log := container.Log
	log.InfoContext(ctx, "notification service initializing",
		"version", "1.0.0",
	)

	// Start Kafka consumers in the background
	kafkaCtx, kafkaCancel := context.WithCancel(ctx)
	go container.KafkaConsumer.Start(kafkaCtx, container.ProcessEvent)

	// Build HTTP router
	router := httphandler.NewRouter(container.NotificationHandler, container.WSHandler, log)

	// Create HTTP server
	srv := &http.Server{
		Addr:         container.Config.HTTPAddress,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start HTTP server in a goroutine
	go func() {
		log.InfoContext(ctx, "http server starting", "address", container.Config.HTTPAddress)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.ErrorContext(ctx, "http server failed", "error", err.Error())
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.InfoContext(ctx, "received shutdown signal, shutting down gracefully")

	// Cancel the Kafka consumer context
	kafkaCancel()

	// Gracefully shut down HTTP server with a timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.ErrorContext(ctx, "http server forced to shutdown", "error", err.Error())
	}

	// Shut down all dependencies
	container.Shutdown()

	log.InfoContext(ctx, "notification service stopped")
}
