package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	domain "github.com/companyofcreators/notification-service/internal/domain/notification"
	kafkainfra "github.com/companyofcreators/notification-service/internal/infrastructure/kafka"
	"github.com/companyofcreators/notification-service/internal/infrastructure/ws"
)

// ProcessEvent is the use case that processes incoming Kafka events and creates notifications.
type ProcessEvent struct {
	repo     domain.NotificationRepository
	producer *kafkainfra.Producer
	hub      *ws.Hub
	log      *slog.Logger
}

// NewProcessEvent creates a new ProcessEvent use case.
func NewProcessEvent(
	repo domain.NotificationRepository,
	producer *kafkainfra.Producer,
	hub *ws.Hub,
	log *slog.Logger,
) *ProcessEvent {
	return &ProcessEvent{
		repo:     repo,
		producer: producer,
		hub:      hub,
		log:      log,
	}
}

// EventEnvelope is a generic Kafka event payload for routing.
type EventEnvelope struct {
	EventID string          `json:"event_id"`
	Payload json.RawMessage `json:"payload"`
}

// Process routes a raw Kafka message to the appropriate handler based on topic.
func (p *ProcessEvent) Process(ctx context.Context, topic string, key, value []byte) error {
	switch topic {
	case "offer.created":
		return p.handleOfferCreated(ctx, value)
	case "offer.accepted":
		return p.handleOfferAccepted(ctx, value)
	case "offer.rejected":
		return p.handleOfferRejected(ctx, value)
	case "offer.withdrawn":
		return p.handleOfferWithdrawn(ctx, value)
	case "offer.countered":
		return p.handleOfferCountered(ctx, value)
	case "chat.message.sent":
		return p.handleChatMessageSent(ctx, value)
	case "order.created":
		return p.handleOrderCreated(ctx, value)
	case "order.assigned":
		return p.handleOrderAssigned(ctx, value)
	case "order.completed":
		return p.handleOrderCompleted(ctx, value)
	case "order.cancelled":
		return p.handleOrderCancelled(ctx, value)
	case "review.created":
		return p.handleReviewCreated(ctx, value)
	case "complaint.created":
		return p.handleComplaintCreated(ctx, value)
	case "complaint.updated":
		return p.handleComplaintUpdated(ctx, value)
	case "user.verification.created":
		return p.handleUserVerificationCreated(ctx, value)
	case "user.created":
		return p.handleUserCreated(ctx, value)
	default:
		p.log.WarnContext(ctx, "unknown kafka topic", "topic", topic)
		return nil
	}
}

// createAndDeliver persists a notification and triggers delivery via all configured channels.
func (p *ProcessEvent) createAndDeliver(ctx context.Context, n *domain.Notification) error {
	if err := p.repo.Create(ctx, n); err != nil {
		return fmt.Errorf("create notification: %w", err)
	}

	p.log.DebugContext(ctx, "notification created",
		"id", n.ID.String(),
		"type", string(n.Type),
		"user_id", n.UserID.String(),
		"channels", n.Channels,
	)

	// Publish notification.created event
	p.producer.PublishNotificationCreated(ctx, kafkainfra.NotificationCreatedMessage{
		NotificationID: n.ID.String(),
		UserID:         n.UserID.String(),
		Type:           string(n.Type),
		Channels:       deliveryChannelsToStrings(n.Channels),
		Data:           n.Data,
	})

	// Deliver via each channel
	for _, ch := range n.Channels {
		switch ch {
		case domain.ChannelWebSocket:
			payload, err := json.Marshal(n)
			if err != nil {
				p.log.ErrorContext(ctx, "failed to marshal notification payload", "error", err)
			} else if serr := p.hub.SendToUser(n.UserID, "notification.new", payload); serr != nil {
				p.log.ErrorContext(ctx, "failed to send ws notification", "error", serr)
			}
		case domain.ChannelEmail:
			if n.Type == domain.NotifVerification {
				continue
			}
			toEmail := resolveEmail(n)
			p.producer.PublishEmail(ctx, kafkainfra.EmailMessage{
				ToEmail:      toEmail,
				UserID:       n.UserID.String(),
				Subject:      n.Title,
				Body:         n.Body,
				TemplateType: string(n.Type),
				Data:         n.Data,
			})
		case domain.ChannelPush:
			p.producer.PublishPush(ctx, kafkainfra.PushMessage{
				UserID: n.UserID.String(),
				Title:  n.Title,
				Body:   n.Body,
				Data:   n.Data,
			})
		}
	}

	return nil
}

// resolveEmail extracts the _email override from notification data, falling back to a synthetic address.
func resolveEmail(n *domain.Notification) string {
	if n.Data != nil {
		var dataMap map[string]interface{}
		if json.Unmarshal(n.Data, &dataMap) == nil {
			if email, ok := dataMap["_email"].(string); ok && email != "" {
				return email
			}
		}
	}
	return "ostap00092@gmail.com"
}

// createAndDeliverBatch persists multiple notifications and delivers them.
func (p *ProcessEvent) createAndDeliverBatch(ctx context.Context, notifications []*domain.Notification) error {
	if err := p.repo.CreateBatch(ctx, notifications); err != nil {
		return fmt.Errorf("create batch notifications: %w", err)
	}

	for _, n := range notifications {
		p.producer.PublishNotificationCreated(ctx, kafkainfra.NotificationCreatedMessage{
			NotificationID: n.ID.String(),
			UserID:         n.UserID.String(),
			Type:           string(n.Type),
			Channels:       deliveryChannelsToStrings(n.Channels),
			Data:           n.Data,
		})

		for _, ch := range n.Channels {
			switch ch {
			case domain.ChannelWebSocket:
				payload, err := json.Marshal(n)
				if err != nil {
					p.log.ErrorContext(ctx, "failed to marshal notification payload", "error", err)
				}
				if serr := p.hub.SendToUser(n.UserID, "notification.new", payload); serr != nil {
					p.log.ErrorContext(ctx, "failed to send ws notification", "error", serr)
				}
			case domain.ChannelEmail:
				if n.Type == domain.NotifVerification {
					continue
				}
				p.producer.PublishEmail(ctx, kafkainfra.EmailMessage{
					ToEmail:      resolveEmail(n),
					UserID:       n.UserID.String(),
					Subject:      n.Title,
					Body:         n.Body,
					TemplateType: string(n.Type),
					Data:         n.Data,
				})
			case domain.ChannelPush:
				p.producer.PublishPush(ctx, kafkainfra.PushMessage{
					UserID: n.UserID.String(),
					Title:  n.Title,
					Body:   n.Body,
					Data:   n.Data,
				})
			}
		}
	}

	return nil
}

// resolveChannelsForUser merges default channels with user preferences.
func (p *ProcessEvent) resolveChannelsForUser(ctx context.Context, userID uuid.UUID, notifType domain.NotificationType) []domain.DeliveryChannel {
	prefs, err := p.repo.GetPreferences(ctx, userID, notifType)
	if err != nil {
		p.log.WarnContext(ctx, "failed to get notification preferences", "error", err)
		return domain.DefaultChannels(notifType)
	}
	channels := domain.ResolveChannels(notifType, prefs)
	if len(channels) == 0 {
		return domain.DefaultChannels(notifType)
	}
	return channels
}

func deliveryChannelsToStrings(channels []domain.DeliveryChannel) []string {
	result := make([]string, len(channels))
	for i, ch := range channels {
		result[i] = string(ch)
	}
	return result
}

// ---- Kafka Event Handlers ----

// handleOfferCreated processes the offer.created event.
// Notify the customer who posted the order about the new offer.
func (p *ProcessEvent) handleOfferCreated(ctx context.Context, value []byte) error {
	var event struct {
		OfferID     string  `json:"offer_id"`
		OrderID     string  `json:"order_id"`
		MasterID    string  `json:"master_id"`
		CustomerID  string  `json:"customer_id"`
		Price       float64 `json:"price"`
		Message     string  `json:"message"`
		MasterEmail string  `json:"master_email"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal offer.created: %w", err)
	}

	customerID := uuid.Nil
	if event.CustomerID != "" {
		var err error
		customerID, err = uuid.Parse(event.CustomerID)
		if err != nil {
			customerID = uuid.Nil
		}
	}

	data, err := json.Marshal(map[string]interface{}{
		"offer_id":     event.OfferID,
		"order_id":     event.OrderID,
		"master_id":    event.MasterID,
		"price":        event.Price,
		"master_email": event.MasterEmail,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	channels := p.resolveChannelsForUser(ctx, customerID, domain.NotifNewOffer)
	n := domain.NewNotification(customerID, domain.NotifNewOffer,
		"Новое предложение",
		fmt.Sprintf("Мастер предложил цену %.2f", event.Price),
		data, channels,
	)

	return p.createAndDeliver(ctx, n)
}

// handleOfferAccepted processes the offer.accepted event.
// Notify both the master (offer accepted) and the customer (you accepted).
func (p *ProcessEvent) handleOfferAccepted(ctx context.Context, value []byte) error {
	var event struct {
		OfferID       string  `json:"offer_id"`
		OrderID       string  `json:"order_id"`
		MasterID      string  `json:"master_id"`
		CustomerID    string  `json:"customer_id"`
		Price         float64 `json:"price"`
		MasterEmail   string  `json:"master_email"`
		CustomerEmail string  `json:"customer_email"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal offer.accepted: %w", err)
	}

	masterID, err := uuid.Parse(event.MasterID)
	if err != nil {
		return fmt.Errorf("parse master_id: %w", err)
	}
	customerID := uuid.Nil
	if event.CustomerID != "" {
		var err error
		customerID, err = uuid.Parse(event.CustomerID)
		if err != nil {
			customerID = uuid.Nil
		}
	}

	masterData, err := json.Marshal(map[string]interface{}{
		"offer_id":       event.OfferID,
		"order_id":       event.OrderID,
		"price":          event.Price,
		"master_email":   event.MasterEmail,
		"customer_email": event.CustomerEmail,
		"_email":         event.MasterEmail,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	customerData, err := json.Marshal(map[string]interface{}{
		"offer_id":       event.OfferID,
		"order_id":       event.OrderID,
		"price":          event.Price,
		"master_email":   event.MasterEmail,
		"customer_email": event.CustomerEmail,
		"_email":         event.CustomerEmail,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	notifications := []*domain.Notification{
		domain.NewNotification(masterID, domain.NotifOfferAccepted,
			"Предложение принято",
			fmt.Sprintf("Ваше предложение на %.2f принято", event.Price),
			masterData, p.resolveChannelsForUser(ctx, masterID, domain.NotifOfferAccepted),
		),
		domain.NewNotification(customerID, domain.NotifOfferAccepted,
			"Предложение принято",
			fmt.Sprintf("Вы приняли предложение на %.2f", event.Price),
			customerData, p.resolveChannelsForUser(ctx, customerID, domain.NotifOfferAccepted),
		),
	}

	return p.createAndDeliverBatch(ctx, notifications)
}

// handleOfferRejected processes the offer.rejected event.
// Notify the master.
func (p *ProcessEvent) handleOfferRejected(ctx context.Context, value []byte) error {
	var event struct {
		OfferID  string  `json:"offer_id"`
		OrderID  string  `json:"order_id"`
		MasterID string  `json:"master_id"`
		Price    float64 `json:"price"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal offer.rejected: %w", err)
	}

	masterID, err := uuid.Parse(event.MasterID)
	if err != nil {
		return fmt.Errorf("parse master_id: %w", err)
	}

	data, err := json.Marshal(map[string]interface{}{
		"offer_id": event.OfferID,
		"order_id": event.OrderID,
		"price":    event.Price,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	n := domain.NewNotification(masterID, domain.NotifOfferRejected,
		"Предложение отклонено",
		fmt.Sprintf("Ваше предложение на %.2f отклонено", event.Price),
		data, p.resolveChannelsForUser(ctx, masterID, domain.NotifOfferRejected),
	)

	return p.createAndDeliver(ctx, n)
}

// handleOfferWithdrawn processes the offer.withdrawn event.
// Notify the customer.
func (p *ProcessEvent) handleOfferWithdrawn(ctx context.Context, value []byte) error {
	var event struct {
		OfferID    string  `json:"offer_id"`
		OrderID    string  `json:"order_id"`
		CustomerID string  `json:"customer_id"`
		MasterID   string  `json:"master_id"`
		Price      float64 `json:"price"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal offer.withdrawn: %w", err)
	}

	customerID := uuid.Nil
	if event.CustomerID != "" {
		var err error
		customerID, err = uuid.Parse(event.CustomerID)
		if err != nil {
			customerID = uuid.Nil
		}
	}

	data, err := json.Marshal(map[string]interface{}{
		"offer_id":  event.OfferID,
		"order_id":  event.OrderID,
		"master_id": event.MasterID,
		"price":     event.Price,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	n := domain.NewNotification(customerID, domain.NotifOfferWithdrawn,
		"Предложение отозвано",
		"Мастер отозвал своё предложение",
		data, p.resolveChannelsForUser(ctx, customerID, domain.NotifOfferWithdrawn),
	)

	return p.createAndDeliver(ctx, n)
}

// handleOfferCountered processes the offer.countered event.
// Notify the master that the customer proposed a new price.
func (p *ProcessEvent) handleOfferCountered(ctx context.Context, value []byte) error {
	var event struct {
		OfferID       string  `json:"offer_id"`
		OrderID       string  `json:"order_id"`
		MasterID      string  `json:"master_id"`
		CustomerID    string  `json:"customer_id"`
		CounterPrice  float64 `json:"counter_price"`
		ProposedPrice float64 `json:"proposed_price"`
		Message       string  `json:"message"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal offer.countered: %w", err)
	}

	if event.CounterPrice == 0 {
		event.CounterPrice = event.ProposedPrice
	}
	if event.MasterID == "" {
		return nil
	}
	masterID, err := uuid.Parse(event.MasterID)
	if err != nil {
		return fmt.Errorf("parse master_id: %w", err)
	}

	data, err := json.Marshal(map[string]interface{}{
		"offer_id":      event.OfferID,
		"order_id":      event.OrderID,
		"customer_id":   event.CustomerID,
		"counter_price": event.CounterPrice,
		"message":       event.Message,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	n := domain.NewNotification(masterID, domain.NotifOfferCountered,
		"Встречное предложение",
		fmt.Sprintf("Заказчик предложил новую цену %.2f", event.CounterPrice),
		data, p.resolveChannelsForUser(ctx, masterID, domain.NotifOfferCountered),
	)

	return p.createAndDeliver(ctx, n)
}

// handleChatMessageSent processes the chat.message.sent event.
// Notify the message receiver.
func (p *ProcessEvent) handleChatMessageSent(ctx context.Context, value []byte) error {
	var event struct {
		MessageID  string `json:"message_id"`
		ChatID     string `json:"chat_id"`
		SenderID   string `json:"sender_id"`
		ReceiverID string `json:"receiver_id"`
		Content    string `json:"content"`
		SenderName string `json:"sender_name"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal chat.message.sent: %w", err)
	}

	receiverID, err := uuid.Parse(event.ReceiverID)
	if err != nil {
		return fmt.Errorf("parse receiver_id: %w", err)
	}

	senderName := event.SenderName
	if senderName == "" {
		senderName = "Кто-то"
	}

	data, err := json.Marshal(map[string]interface{}{
		"message_id":  event.MessageID,
		"chat_id":     event.ChatID,
		"sender_id":   event.SenderID,
		"sender_name": senderName,
		"content":     truncateContent(event.Content, 100),
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	n := domain.NewNotification(receiverID, domain.NotifNewMessage,
		"Новое сообщение",
		fmt.Sprintf("Новое сообщение от %s: %s", senderName, truncateContent(event.Content, 50)),
		data, p.resolveChannelsForUser(ctx, receiverID, domain.NotifNewMessage),
	)

	return p.createAndDeliver(ctx, n)
}

// handleOrderCreated processes the order.created event.
func (p *ProcessEvent) handleOrderCreated(ctx context.Context, value []byte) error {
	var event orderCreatedEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal order.created: %w", err)
	}
	orderID, err := uuid.Parse(event.OrderID)
	if err != nil {
		return fmt.Errorf("parse order.id: %w", err)
	}
	userID, err := uuid.Parse(event.CustomerID)
	if err != nil {
		return fmt.Errorf("parse customer_id: %w", err)
	}
	n := &domain.Notification{
		ID:        uuid.New(),
		UserID:    userID,
		Type:      domain.NotifSystemMessage,
		Title:     "Заказ создан",
		Body:      fmt.Sprintf("Ваш заказ #%s успешно создан", event.Title),
		Data:      json.RawMessage("{}"),
		IsRead:    false,
		Channels:  []domain.DeliveryChannel{domain.ChannelWebSocket, domain.ChannelEmail},
		CreatedAt: time.Now().UTC(),
	}
	_ = orderID
	return p.createAndDeliver(ctx, n)
}

type orderCreatedEvent struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Title      string `json:"title"`
}

// handleOrderAssigned processes the order.assigned event.
// Notify both the master and customer.
func (p *ProcessEvent) handleOrderAssigned(ctx context.Context, value []byte) error {
	var event struct {
		OrderID    string `json:"order_id"`
		MasterID   string `json:"master_id"`
		CustomerID string `json:"customer_id"`
		OrderTitle string `json:"order_title"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal order.assigned: %w", err)
	}

	if event.MasterID == "" {
		customerID, err := uuid.Parse(event.CustomerID)
		if err != nil {
			return nil
		}
		data, err := json.Marshal(map[string]interface{}{
			"order_id":    event.OrderID,
			"order_title": event.OrderTitle,
		})
		if err != nil {
			p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
		}
		n := domain.NewNotification(customerID, domain.NotifOrderAssigned,
			"Р—Р°РєР°Р· РЅР°Р·РЅР°С‡РµРЅ",
			"РњР°СЃС‚РµСЂ РЅР°Р·РЅР°С‡РµРЅ РЅР° РІР°С€ Р·Р°РєР°Р·",
			data, p.resolveChannelsForUser(ctx, customerID, domain.NotifOrderAssigned),
		)
		return p.createAndDeliver(ctx, n)
	}

	masterID, err := uuid.Parse(event.MasterID)
	if err != nil {
		return fmt.Errorf("parse master_id: %w", err)
	}
	customerID := uuid.Nil
	if event.CustomerID != "" {
		var err error
		customerID, err = uuid.Parse(event.CustomerID)
		if err != nil {
			customerID = uuid.Nil
		}
	}

	data, err := json.Marshal(map[string]interface{}{
		"order_id":    event.OrderID,
		"master_id":   event.MasterID,
		"order_title": event.OrderTitle,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	title := "Заказ назначен"
	if event.OrderTitle != "" {
		title = "Заказ назначен"
	}

	notifications := []*domain.Notification{
		domain.NewNotification(masterID, domain.NotifOrderAssigned,
			title,
			"Вам назначен заказ",
			data, p.resolveChannelsForUser(ctx, masterID, domain.NotifOrderAssigned),
		),
		domain.NewNotification(customerID, domain.NotifOrderAssigned,
			title,
			"Мастер назначен на ваш заказ",
			data, p.resolveChannelsForUser(ctx, customerID, domain.NotifOrderAssigned),
		),
	}

	return p.createAndDeliverBatch(ctx, notifications)
}

// handleOrderCompleted processes the order.completed event.
// Notify both parties.
func (p *ProcessEvent) handleOrderCompleted(ctx context.Context, value []byte) error {
	var event struct {
		OrderID    string `json:"order_id"`
		MasterID   string `json:"master_id"`
		CustomerID string `json:"customer_id"`
		OrderTitle string `json:"order_title"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal order.completed: %w", err)
	}

	if event.MasterID == "" {
		customerID, err := uuid.Parse(event.CustomerID)
		if err != nil {
			return nil
		}
		data, err := json.Marshal(map[string]interface{}{
			"order_id":    event.OrderID,
			"order_title": event.OrderTitle,
		})
		if err != nil {
			p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
		}
		n := domain.NewNotification(customerID, domain.NotifOrderCompleted,
			"Р—Р°РєР°Р· Р·Р°РІРµСЂС€С‘РЅ",
			"Р’Р°С€ Р·Р°РєР°Р· РѕС‚РјРµС‡РµРЅ РєР°Рє Р·Р°РІРµСЂС€С‘РЅРЅС‹Р№",
			data, p.resolveChannelsForUser(ctx, customerID, domain.NotifOrderCompleted),
		)
		return p.createAndDeliver(ctx, n)
	}

	masterID, err := uuid.Parse(event.MasterID)
	if err != nil {
		return fmt.Errorf("parse master_id: %w", err)
	}
	customerID := uuid.Nil
	if event.CustomerID != "" {
		var err error
		customerID, err = uuid.Parse(event.CustomerID)
		if err != nil {
			customerID = uuid.Nil
		}
	}

	data, err := json.Marshal(map[string]interface{}{
		"order_id":    event.OrderID,
		"order_title": event.OrderTitle,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	notifications := []*domain.Notification{
		domain.NewNotification(masterID, domain.NotifOrderCompleted,
			"Заказ завершён",
			"Заказ отмечен как завершённый",
			data, p.resolveChannelsForUser(ctx, masterID, domain.NotifOrderCompleted),
		),
		domain.NewNotification(customerID, domain.NotifOrderCompleted,
			"Заказ завершён",
			"Ваш заказ отмечен как завершённый",
			data, p.resolveChannelsForUser(ctx, customerID, domain.NotifOrderCompleted),
		),
	}

	return p.createAndDeliverBatch(ctx, notifications)
}

// handleOrderCancelled processes the order.cancelled event.
// Notify affected parties.
func (p *ProcessEvent) handleOrderCancelled(ctx context.Context, value []byte) error {
	var event struct {
		OrderID     string `json:"order_id"`
		CancelledBy string `json:"cancelled_by"`
		CustomerID  string `json:"customer_id"`
		MasterID    string `json:"master_id"`
		Reason      string `json:"reason"`
		OrderTitle  string `json:"order_title"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal order.cancelled: %w", err)
	}

	customerID := uuid.Nil
	if event.CustomerID != "" {
		var err error
		customerID, err = uuid.Parse(event.CustomerID)
		if err != nil {
			customerID = uuid.Nil
		}
	}

	reason := event.Reason
	if reason == "" {
		reason = "Причина не указана"
	}

	data, err := json.Marshal(map[string]interface{}{
		"order_id":     event.OrderID,
		"cancelled_by": event.CancelledBy,
		"reason":       reason,
		"order_title":  event.OrderTitle,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	notifications := []*domain.Notification{
		domain.NewNotification(customerID, domain.NotifOrderCancelled,
			"Заказ отменён",
			fmt.Sprintf("Ваш заказ отменён: %s", reason),
			data, p.resolveChannelsForUser(ctx, customerID, domain.NotifOrderCancelled),
		),
	}

	// Notify master if assigned
	if event.MasterID != "" {
		masterID, err := uuid.Parse(event.MasterID)
		if err == nil {
			notifications = append(notifications,
				domain.NewNotification(masterID, domain.NotifOrderCancelled,
					"Заказ отменён",
					fmt.Sprintf("Назначенный заказ отменён: %s", reason),
					data, p.resolveChannelsForUser(ctx, masterID, domain.NotifOrderCancelled),
				),
			)
		}
	}

	return p.createAndDeliverBatch(ctx, notifications)
}

// handleReviewCreated processes the review.created event.
// Notify the user being reviewed.
func (p *ProcessEvent) handleReviewCreated(ctx context.Context, value []byte) error {
	var event struct {
		ReviewID       string `json:"review_id"`
		ReviewerID     string `json:"reviewer_id"`
		ReviewedUserID string `json:"reviewed_user_id"`
		FromUserID     string `json:"from_user_id"`
		ToUserID       string `json:"to_user_id"`
		Rating         int    `json:"rating"`
		Comment        string `json:"comment"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal review.created: %w", err)
	}

	if event.ReviewerID == "" {
		event.ReviewerID = event.FromUserID
	}
	if event.ReviewedUserID == "" {
		event.ReviewedUserID = event.ToUserID
	}
	reviewedUserID, err := uuid.Parse(event.ReviewedUserID)
	if err != nil {
		return fmt.Errorf("parse reviewed_user_id: %w", err)
	}

	data, err := json.Marshal(map[string]interface{}{
		"review_id":   event.ReviewID,
		"reviewer_id": event.ReviewerID,
		"rating":      event.Rating,
		"comment":     truncateContent(event.Comment, 100),
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	n := domain.NewNotification(reviewedUserID, domain.NotifNewReview,
		"Новый отзыв",
		fmt.Sprintf("Вы получили новый отзыв с оценкой %d/5", event.Rating),
		data, p.resolveChannelsForUser(ctx, reviewedUserID, domain.NotifNewReview),
	)

	return p.createAndDeliver(ctx, n)
}

// handleComplaintCreated processes the complaint.created event.
// Notify moderators (sent to system topic for admin notification).
func (p *ProcessEvent) handleComplaintCreated(ctx context.Context, value []byte) error {
	var event struct {
		ComplaintID string `json:"complaint_id"`
		ReporterID  string `json:"reporter_id"`
		Subject     string `json:"subject"`
		OrderID     string `json:"order_id"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal complaint.created: %w", err)
	}

	reporterID, err := uuid.Parse(event.ReporterID)
	if err != nil {
		return fmt.Errorf("parse reporter_id: %w", err)
	}

	data, err := json.Marshal(map[string]interface{}{
		"complaint_id": event.ComplaintID,
		"subject":      event.Subject,
		"order_id":     event.OrderID,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	n := domain.NewNotification(reporterID, domain.NotifComplaintCreated,
		"Жалоба отправлена",
		fmt.Sprintf("Ваша жалоба \"%s\" отправлена на рассмотрение", truncateContent(event.Subject, 50)),
		data, p.resolveChannelsForUser(ctx, reporterID, domain.NotifComplaintCreated),
	)

	return p.createAndDeliver(ctx, n)
}

// handleComplaintUpdated processes the complaint.updated event.
// Notify the reporter about status change.
func (p *ProcessEvent) handleComplaintUpdated(ctx context.Context, value []byte) error {
	var event struct {
		ComplaintID string `json:"complaint_id"`
		ReporterID  string `json:"reporter_id"`
		NewStatus   string `json:"new_status"`
		Resolution  string `json:"resolution"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal complaint.updated: %w", err)
	}

	reporterID, err := uuid.Parse(event.ReporterID)
	if err != nil {
		return fmt.Errorf("parse reporter_id: %w", err)
	}

	data, err := json.Marshal(map[string]interface{}{
		"complaint_id": event.ComplaintID,
		"new_status":   event.NewStatus,
		"resolution":   event.Resolution,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	n := domain.NewNotification(reporterID, domain.NotifComplaintUpdated,
		"Жалоба обновлена",
		fmt.Sprintf("Статус вашей жалобы обновлён на: %s", event.NewStatus),
		data, p.resolveChannelsForUser(ctx, reporterID, domain.NotifComplaintUpdated),
	)

	return p.createAndDeliver(ctx, n)
}

// handleUserVerificationCreated processes the user.verification.created event.
// Notify the user to verify their email.
func (p *ProcessEvent) handleUserVerificationCreated(ctx context.Context, value []byte) error {
	var event struct {
		UserID            string `json:"user_id"`
		Email             string `json:"email"`
		VerificationToken string `json:"verification_token"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal user.verification.created: %w", err)
	}

	userID, err := uuid.Parse(event.UserID)
	if err != nil {
		return fmt.Errorf("parse user_id: %w", err)
	}

	data, err := json.Marshal(map[string]interface{}{
		"email":              event.Email,
		"verification_token": event.VerificationToken,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	// Verification notifications should always go via email
	n := domain.NewNotification(userID, domain.NotifVerification,
		"Подтвердите email",
		"Пожалуйста, подтвердите email для завершения регистрации",
		data, []domain.DeliveryChannel{domain.ChannelEmail},
	)

	// Don't use preferences for verification - always send via email
	return p.createAndDeliver(ctx, n)
}

// handleUserCreated processes the user.created event.
// Send a welcome notification to the new user.
func (p *ProcessEvent) handleUserCreated(ctx context.Context, value []byte) error {
	var event struct {
		UserID    string `json:"user_id"`
		Email     string `json:"email"`
		FirstName string `json:"first_name"`
		Role      string `json:"role"`
	}
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal user.created: %w", err)
	}

	userID, err := uuid.Parse(event.UserID)
	if err != nil {
		return fmt.Errorf("parse user_id: %w", err)
	}

	name := event.FirstName
	if name == "" {
		name = event.Email
	}

	data, err := json.Marshal(map[string]interface{}{
		"role":   event.Role,
		"email":  event.Email,
		"_email": event.Email,
	})

	if err != nil {
		p.log.ErrorContext(ctx, "failed to marshal notification data", "error", err)
	}

	n := domain.NewNotification(userID, domain.NotifUserCreated,
		"Добро пожаловать на платформу",
		fmt.Sprintf("%s, добро пожаловать! Мы рады видеть вас на платформе.", name),
		data, p.resolveChannelsForUser(ctx, userID, domain.NotifUserCreated),
	)

	return p.createAndDeliver(ctx, n)
}

// truncateContent truncates a string to the given max length, appending "..." if truncated.
func truncateContent(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}
