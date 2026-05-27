package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

// Client represents a single WebSocket connection for a given user.
type Client struct {
	UserID uuid.UUID
	conn   *websocket.Conn
	hub    *Hub
	send   chan []byte
	ctx    context.Context
	cancel context.CancelFunc
}

// writePump writes messages to the WebSocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				return
			}
			writeCtx, cancel := context.WithTimeout(c.ctx, writeWait)
			if err := c.conn.Write(writeCtx, websocket.MessageText, message); err != nil {
				cancel()
				return
			}
			cancel()
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(c.ctx, writeWait)
			if err := c.conn.Ping(pingCtx); err != nil {
				cancel()
				return
			}
			cancel()
		case <-c.ctx.Done():
			return
		}
	}
}

// Hub maintains the set of active clients grouped by user ID.
type Hub struct {
	clients    map[uuid.UUID]map[*Client]struct{}
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
	log        *slog.Logger
}

// NotificationMessage is the JSON payload sent over WebSocket to clients.
type NotificationMessage struct {
	Type         string          `json:"type"`
	Notification json.RawMessage `json:"notification,omitempty"`
	Count        int             `json:"count,omitempty"`
}

// NewHub creates a new Hub and starts its run loop.
func NewHub(log *slog.Logger) *Hub {
	h := &Hub{
		clients:    make(map[uuid.UUID]map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		log:        log,
	}
	go h.run()
	return h
}

// run processes register and unregister events.
func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			if _, ok := h.clients[client.UserID]; !ok {
				h.clients[client.UserID] = make(map[*Client]struct{})
			}
			h.clients[client.UserID][client] = struct{}{}
			h.mu.Unlock()
			h.log.DebugContext(context.Background(), "ws client connected",
				"user_id", client.UserID.String(),
			)

		case client := <-h.unregister:
			h.mu.Lock()
			if clients, ok := h.clients[client.UserID]; ok {
				if _, exists := clients[client]; exists {
					delete(clients, client)
					close(client.send)
					client.cancel()
					if len(clients) == 0 {
						delete(h.clients, client.UserID)
					}
				}
			}
			h.mu.Unlock()
			h.log.DebugContext(context.Background(), "ws client disconnected",
				"user_id", client.UserID.String(),
			)
		}
	}
}

// RegisterClient registers a new WebSocket connection for a user.
func (h *Hub) RegisterClient(userID uuid.UUID, conn *websocket.Conn) *Client {
	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		UserID: userID,
		conn:   conn,
		hub:    h,
		send:   make(chan []byte, 64),
		ctx:    ctx,
		cancel: cancel,
	}
	h.register <- client

	go client.writePump()

	// Send welcome message
	welcome, _ := json.Marshal(NotificationMessage{
		Type: "notification.connected",
	})
	select {
	case client.send <- welcome:
	default:
	}

	return client
}

// UnregisterClient removes a client from the hub.
func (h *Hub) UnregisterClient(userID uuid.UUID, conn *websocket.Conn) {
	h.mu.Lock()
	if clients, ok := h.clients[userID]; ok {
		for client := range clients {
			if client.conn == conn {
				h.mu.Unlock()
				h.unregister <- client
				return
			}
		}
	}
	h.mu.Unlock()
}

// SendToUser sends a JSON message to all WebSocket connections of a specific user.
func (h *Hub) SendToUser(userID uuid.UUID, msgType string, notificationPayload json.RawMessage) error {
	msg := NotificationMessage{
		Type:         msgType,
		Notification: notificationPayload,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal ws message: %w", err)
	}

	h.mu.RLock()
	clients, ok := h.clients[userID]
	h.mu.RUnlock()

	if !ok || len(clients) == 0 {
		return nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients[userID] {
		select {
		case client.send <- data:
		default:
			h.log.WarnContext(context.Background(), "client send buffer full",
				"user_id", userID.String(),
			)
			go func(c *Client) { h.unregister <- c }(client)
		}
	}

	return nil
}

// SendUnreadCount sends the unread notification count to a user.
func (h *Hub) SendUnreadCount(userID uuid.UUID, count int) error {
	msg := NotificationMessage{
		Type:  "notification.unread_count",
		Count: count,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal unread count: %w", err)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients[userID] {
		select {
		case client.send <- data:
		default:
			go func(c *Client) { h.unregister <- c }(client)
		}
	}

	return nil
}

// IsUserOnline returns true if the user has at least one active WebSocket connection.
func (h *Hub) IsUserOnline(userID uuid.UUID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients[userID]) > 0
}

// CloseAll closes all WebSocket connections gracefully.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for userID, clients := range h.clients {
		for client := range clients {
			client.conn.Close(websocket.StatusGoingAway, "server shutting down")
			close(client.send)
			client.cancel()
		}
		h.log.InfoContext(context.Background(), "closed ws connections", "user_id", userID.String())
	}

	h.clients = make(map[uuid.UUID]map[*Client]struct{})
}
