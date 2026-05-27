package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

// Client represents a single WebSocket connection for a given user.
type Client struct {
	UserID uuid.UUID
	conn   *websocket.Conn
	hub    *Hub
	send   chan []byte
}

// readPump reads messages from the WebSocket connection.
// In push-only mode, we only listen for pong messages and close signals.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// writePump writes messages to the WebSocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// Hub maintains the set of active clients grouped by user ID and broadcasts messages to them.
type Hub struct {
	// clients is a map from userID to a set of connections (supports multiple devices).
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
			h.log.DebugContext(context.Background(), "websocket client connected",
				"user_id", client.UserID.String(),
			)

		case client := <-h.unregister:
			h.mu.Lock()
			if clients, ok := h.clients[client.UserID]; ok {
				if _, exists := clients[client]; exists {
					delete(clients, client)
					close(client.send)
					if len(clients) == 0 {
						delete(h.clients, client.UserID)
					}
				}
			}
			h.mu.Unlock()
			h.log.DebugContext(context.Background(), "websocket client disconnected",
				"user_id", client.UserID.String(),
			)
		}
	}
}

// RegisterClient upgrades an HTTP connection to WebSocket and registers the client.
func (h *Hub) RegisterClient(userID uuid.UUID, conn *websocket.Conn) *Client {
	client := &Client{
		UserID: userID,
		conn:   conn,
		hub:    h,
		send:   make(chan []byte, 64),
	}
	h.register <- client

	go client.writePump()
	go client.readPump()

	// Send welcome message so client knows connection is established.
	welcome, _ := json.Marshal(NotificationMessage{
		Type: "notification.connected",
	})
	select {
	case client.send <- welcome:
	default:
	}

	return client
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
		return nil // User not connected, not an error
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients[userID] {
		select {
		case client.send <- data:
		default:
			// Client buffer is full, drop the message and close connection
			h.log.WarnContext(context.Background(), "client send buffer full, dropping message",
				"user_id", userID.String(),
			)
			go func(c *Client) {
				h.unregister <- c
			}(client)
		}
	}

	return nil
}

// SendUnreadCount sends the unread notification count to all connections of a user.
func (h *Hub) SendUnreadCount(userID uuid.UUID, count int) error {
	msg := NotificationMessage{
		Type:  "notification.unread_count",
		Count: count,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal unread count message: %w", err)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients[userID] {
		select {
		case client.send <- data:
		default:
			go func(c *Client) {
				h.unregister <- c
			}(client)
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
			client.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutting down"))
			client.conn.Close()
			close(client.send)
		}
		h.log.InfoContext(context.Background(), "closed websocket connections for user", "user_id", userID.String())
	}

	h.clients = make(map[uuid.UUID]map[*Client]struct{})
}
