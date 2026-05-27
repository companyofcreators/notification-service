package notification

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	domain "github.com/companyofcreators/notification-service/internal/domain/notification"
)

// ListQuery represents the query parameters for listing notifications.
type ListQuery struct {
	UserID uuid.UUID
	Limit  int
	Offset int
}

// ListResult contains paginated notification results.
type ListResult struct {
	Notifications []*domain.Notification `json:"notifications"`
	Total         int                    `json:"total"`
	UnreadCount   int                    `json:"unread_count"`
}

// List handles listing notifications for a user with pagination and unread count.
type List struct {
	repo domain.NotificationRepository
	log  *slog.Logger
}

// NewList creates a new List use case.
func NewList(repo domain.NotificationRepository, log *slog.Logger) *List {
	return &List{repo: repo, log: log}
}

// Execute retrieves paginated notifications for a user along with the unread count.
func (l *List) Execute(ctx context.Context, query ListQuery) (*ListResult, error) {
	if query.Limit <= 0 {
		query.Limit = 20
	}
	if query.Limit > 100 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}

	notifications, total, err := l.repo.ListByUser(ctx, query.UserID, query.Limit, query.Offset)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}

	unreadCount, err := l.repo.GetUnreadCount(ctx, query.UserID)
	if err != nil {
		l.log.WarnContext(ctx, "failed to get unread count", "user_id", query.UserID.String(), "error", err)
		unreadCount = 0
	}

	if notifications == nil {
		notifications = []*domain.Notification{}
	}

	return &ListResult{
		Notifications: notifications,
		Total:         total,
		UnreadCount:   unreadCount,
	}, nil
}

// GetUnreadCount returns the number of unread notifications for a user.
func (l *List) GetUnreadCount(ctx context.Context, userID uuid.UUID) (int, error) {
	count, err := l.repo.GetUnreadCount(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("get unread count: %w", err)
	}
	return count, nil
}

// MarkAsRead marks a single notification as read.
func (l *List) MarkAsRead(ctx context.Context, id, userID uuid.UUID) error {
	err := l.repo.MarkAsRead(ctx, id, userID)
	if err != nil {
		return fmt.Errorf("mark as read: %w", err)
	}
	return nil
}

// MarkAllAsRead marks all notifications as read for a user.
func (l *List) MarkAllAsRead(ctx context.Context, userID uuid.UUID) error {
	err := l.repo.MarkAllAsRead(ctx, userID)
	if err != nil {
		return fmt.Errorf("mark all as read: %w", err)
	}
	return nil
}

// Delete removes a notification by ID, scoped to the owning user.
func (l *List) Delete(ctx context.Context, id, userID uuid.UUID) error {
	err := l.repo.Delete(ctx, id, userID)
	if err != nil {
		return fmt.Errorf("delete notification: %w", err)
	}
	return nil
}
