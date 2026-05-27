package notification

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// NotificationType enumerates the kinds of notifications the system can produce.
type NotificationType string

const (
	NotifNewOffer          NotificationType = "new_offer"
	NotifOfferAccepted     NotificationType = "offer_accepted"
	NotifOfferRejected     NotificationType = "offer_rejected"
	NotifOfferCountered    NotificationType = "offer_countered"
	NotifOfferWithdrawn    NotificationType = "offer_withdrawn"
	NotifNewMessage        NotificationType = "new_message"
	NotifOrderCompleted    NotificationType = "order_completed"
	NotifOrderCancelled    NotificationType = "order_cancelled"
	NotifOrderAssigned     NotificationType = "order_assigned"
	NotifNewReview         NotificationType = "new_review"
	NotifComplaintCreated  NotificationType = "complaint_created"
	NotifComplaintUpdated  NotificationType = "complaint_updated"
	NotifRoleChanged       NotificationType = "role_changed"
	NotifVerification      NotificationType = "email_verification"
	NotifSystemMessage     NotificationType = "system_message"
	NotifUserCreated       NotificationType = "user_created"
)

// DeliveryChannel represents a channel through which a notification can be delivered.
type DeliveryChannel string

const (
	ChannelWebSocket DeliveryChannel = "websocket"
	ChannelEmail     DeliveryChannel = "email"
	ChannelPush      DeliveryChannel = "push"
)

// Notification is the core domain entity representing a notification to be delivered.
type Notification struct {
	ID        uuid.UUID          `json:"id"`
	UserID    uuid.UUID          `json:"user_id"`
	Type      NotificationType   `json:"type"`
	Title     string             `json:"title"`
	Body      string             `json:"body"`
	Data      json.RawMessage    `json:"data"`
	IsRead    bool               `json:"is_read"`
	Channels  []DeliveryChannel  `json:"channels"`
	CreatedAt time.Time          `json:"created_at"`
}

// NotificationPreference determines how a user wants to receive notifications of a given type.
type NotificationPreference struct {
	UserID   uuid.UUID
	Type     NotificationType
	Channels []DeliveryChannel
	Enabled  bool
}

// NewNotification creates a new notification with the current timestamp.
func NewNotification(
	userID uuid.UUID,
	notifType NotificationType,
	title, body string,
	data json.RawMessage,
	channels []DeliveryChannel,
) *Notification {
	if channels == nil {
		channels = []DeliveryChannel{ChannelWebSocket}
	}
	if data == nil {
		data = json.RawMessage("{}")
	}
	return &Notification{
		ID:        uuid.New(),
		UserID:    userID,
		Type:      notifType,
		Title:     title,
		Body:      body,
		Data:      data,
		IsRead:    false,
		Channels:  channels,
		CreatedAt: time.Now().UTC(),
	}
}

// DefaultChannels returns the default delivery channels for a notification type.
func DefaultChannels(nt NotificationType) []DeliveryChannel {
	switch nt {
	case NotifVerification:
		return []DeliveryChannel{ChannelEmail}
	case NotifNewMessage:
		return []DeliveryChannel{ChannelWebSocket, ChannelEmail}
	default:
		return []DeliveryChannel{ChannelWebSocket, ChannelEmail}
	}
}

// ResolveChannels merges user preferences with default channels.
func ResolveChannels(nt NotificationType, prefs *NotificationPreference) []DeliveryChannel {
	if prefs != nil && !prefs.Enabled {
		return nil
	}
	if prefs != nil && len(prefs.Channels) > 0 {
		return prefs.Channels
	}
	return DefaultChannels(nt)
}
