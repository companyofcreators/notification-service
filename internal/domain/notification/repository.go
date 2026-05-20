package notification

import (
	"context"

	"github.com/google/uuid"
)

// NotificationRepository defines persistence operations for notifications and preferences.
type NotificationRepository interface {
	// Notification CRUD
	Create(ctx context.Context, n *Notification) error
	CreateBatch(ctx context.Context, notifications []*Notification) error
	ListByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]*Notification, int, error)
	MarkAsRead(ctx context.Context, id, userID uuid.UUID) error
	MarkAllAsRead(ctx context.Context, userID uuid.UUID) error
	GetUnreadCount(ctx context.Context, userID uuid.UUID) (int, error)
	Delete(ctx context.Context, id, userID uuid.UUID) error

	// Preferences
	GetPreferences(ctx context.Context, userID uuid.UUID, notifType NotificationType) (*NotificationPreference, error)
	UpsertPreferences(ctx context.Context, pref *NotificationPreference) error
}
