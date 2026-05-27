package http

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	domain "github.com/companyofcreators/notification-service/internal/domain/notification"
)

// NotificationResponse is the DTO returned for a single notification.
type NotificationResponse struct {
	ID        uuid.UUID       `json:"id"`
	UserID    uuid.UUID       `json:"user_id"`
	Type      string          `json:"type"`
	Title     string          `json:"title"`
	Body      string          `json:"body"`
	Data      json.RawMessage `json:"data,omitempty"`
	IsRead    bool            `json:"is_read"`
	Channels  []string        `json:"channels"`
	CreatedAt time.Time       `json:"created_at"`
}

// NotificationListResponse is the paginated response for listing notifications.
type NotificationListResponse struct {
	Notifications []NotificationResponse `json:"notifications"`
	Total         int                    `json:"total"`
	UnreadCount   int                    `json:"unread_count"`
}

// UnreadCountResponse holds the unread notification count.
type UnreadCountResponse struct {
	Count int `json:"count"`
}

// MarkAllReadResponse confirms all notifications were marked as read.
type MarkAllReadResponse struct {
	MarkedRead bool `json:"marked_read"`
}

// DeleteResponse confirms notification deletion.
type DeleteResponse struct {
	Deleted bool `json:"deleted"`
}

// toNotificationResponse converts a domain notification to its DTO representation.
func toNotificationResponse(n *domain.Notification) NotificationResponse {
	channels := make([]string, len(n.Channels))
	for i, ch := range n.Channels {
		channels[i] = string(ch)
	}

	return NotificationResponse{
		ID:        n.ID,
		UserID:    n.UserID,
		Type:      string(n.Type),
		Title:     n.Title,
		Body:      n.Body,
		Data:      n.Data,
		IsRead:    n.IsRead,
		Channels:  channels,
		CreatedAt: n.CreatedAt,
	}
}

// toNotificationListResponse converts a list of domain notifications to a DTO list.
func toNotificationListResponse(notifications []*domain.Notification, total, unreadCount int) NotificationListResponse {
	items := make([]NotificationResponse, len(notifications))
	for i, n := range notifications {
		items[i] = toNotificationResponse(n)
	}

	return NotificationListResponse{
		Notifications: items,
		Total:         total,
		UnreadCount:   unreadCount,
	}
}
