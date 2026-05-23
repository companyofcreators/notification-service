package ws

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	wsHub "github.com/companyofcreators/notification-service/internal/infrastructure/ws"
)

// WSHandler handles WebSocket upgrade requests.
type WSHandler struct {
	hub           *wsHub.Hub
	log           *slog.Logger
	upgrader      websocket.Upgrader
	allowedOrigin string
}

// NewWSHandler creates a new WebSocket handler.
func NewWSHandler(hub *wsHub.Hub, log *slog.Logger, allowedOrigin string) *WSHandler {
	h := &WSHandler{
		hub:           hub,
		log:           log,
		allowedOrigin: allowedOrigin,
	}
	h.upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     h.checkOrigin,
	}
	return h
}

// checkOrigin validates the Origin header.
// Allows localhost, 127.0.0.1, 192.168.0.103 (any port), and same-origin.
func (h *WSHandler) checkOrigin(r *http.Request) bool {
	if h.allowedOrigin != "" {
		origin := r.Header.Get("Origin")
		return origin == h.allowedOrigin
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // non-browser clients may not send Origin
	}
	// Same-origin
	if origin == "http://"+r.Host || origin == "https://"+r.Host {
		return true
	}
	// Local network for development (any port)
	originLower := strings.ToLower(origin)
	return strings.Contains(originLower, "//localhost:") ||
		strings.Contains(originLower, "//127.0.0.1:") ||
		strings.Contains(originLower, "//192.168.0.103")
}

// HandleWebSocket upgrades an HTTP connection to a WebSocket and registers the client.
// The user is identified by the X-User-ID header.
func (h *WSHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	userIDStr := r.Header.Get("X-User-ID")
	if userIDStr == "" {
		// Also try query parameter for WebSocket connections from browsers
		userIDStr = r.URL.Query().Get("user_id")
	}
	if userIDStr == "" {
		http.Error(w, "user_id обязателен", http.StatusUnauthorized)
		return
	}

	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		http.Error(w, "недействительный user_id", http.StatusBadRequest)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.ErrorContext(r.Context(), "failed to upgrade websocket connection", "error", err)
		return
	}

	client := h.hub.RegisterClient(userID, conn)

	h.log.InfoContext(r.Context(), "websocket client registered",
		"user_id", userID.String(),
		"client", client,
	)
}
