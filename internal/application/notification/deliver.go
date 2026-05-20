package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	domain "github.com/companyofcreators/notification-service/internal/domain/notification"
	"github.com/companyofcreators/notification-service/internal/infrastructure/ws"
)

// Deliver handles the delivery of notifications via WebSocket and other channels.
type Deliver struct {
	repo domain.NotificationRepository
	hub  *ws.Hub
	log  *slog.Logger
}

// NewDeliver creates a new Deliver use case.
func NewDeliver(repo domain.NotificationRepository, hub *ws.Hub, log *slog.Logger) *Deliver {
	return &Deliver{repo: repo, hub: hub, log: log}
}

// DeliverToUser pushes a notification to the user via WebSocket if they are online.
// Returns true if the user was online and received the notification.
func (d *Deliver) DeliverToUser(ctx context.Context, userID uuid.UUID, n *domain.Notification) (bool, error) {
	payload, err := json.Marshal(n)
	if err != nil {
		return false, fmt.Errorf("marshal notification: %w", err)
	}

	if err := d.hub.SendToUser(userID, "notification.new", payload); err != nil {
		d.log.ErrorContext(ctx, "failed to deliver notification via websocket",
			"notification_id", n.ID.String(),
			"user_id", userID.String(),
			"error", err,
		)
		return false, fmt.Errorf("ws send: %w", err)
	}

	return d.hub.IsUserOnline(userID), nil
}

// PushUnreadCount sends the updated unread count to the user via WebSocket.
func (d *Deliver) PushUnreadCount(ctx context.Context, userID uuid.UUID) error {
	count, err := d.repo.GetUnreadCount(ctx, userID)
	if err != nil {
		return fmt.Errorf("get unread count: %w", err)
	}

	if err := d.hub.SendUnreadCount(userID, count); err != nil {
		d.log.ErrorContext(ctx, "failed to push unread count",
			"user_id", userID.String(),
			"error", err,
		)
		return fmt.Errorf("ws send unread count: %w", err)
	}

	return nil
}
