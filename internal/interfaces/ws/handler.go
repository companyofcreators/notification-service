package ws

import (
	"context"
	"crypto/rsa"
	"log/slog"
	"net/http"
	"strings"

	"github.com/coder/websocket"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	wsHub "github.com/companyofcreators/notification-service/internal/infrastructure/ws"
)

// WSHandler handles WebSocket upgrade requests with JWT authentication.
type WSHandler struct {
	hub        *wsHub.Hub
	log        *slog.Logger
	jwtPubKey  *rsa.PublicKey
}

// NewWSHandler creates a new WebSocket handler.
func NewWSHandler(hub *wsHub.Hub, jwtPubKey *rsa.PublicKey, log *slog.Logger) *WSHandler {
	return &WSHandler{
		hub:       hub,
		log:       log,
		jwtPubKey: jwtPubKey,
	}
}

// Claims represents the JWT claims for WebSocket authentication.
type Claims struct {
	jwt.RegisteredClaims
	Email string `json:"email"`
	Role  string `json:"role"`
}

// HandleWebSocket upgrades an HTTP connection to a WebSocket after JWT validation.
// Requires ?token=JWT query parameter.
func (h *WSHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		http.Error(w, "отсутствует параметр token", http.StatusUnauthorized)
		return
	}

	claims, err := h.validateToken(tokenStr)
	if err != nil {
		h.log.WarnContext(r.Context(), "invalid JWT token for websocket",
			"error", err.Error(),
		)
		http.Error(w, "недействительный токен", http.StatusUnauthorized)
		return
	}

	userID, err := uuid.Parse(claims.Subject)
	if err != nil {
		http.Error(w, "недействительный user_id в токене", http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		h.log.ErrorContext(r.Context(), "failed to upgrade websocket", "error", err)
		return
	}

	client := h.hub.RegisterClient(userID, conn)

	h.log.InfoContext(r.Context(), "websocket client registered",
		"user_id", userID.String(),
		"client", client,
	)

	// Start read pump to handle pings and disconnect
	go h.readPump(userID, conn)
}

// validateToken parses and validates a RS256 JWT token.
func (h *WSHandler) validateToken(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{},
		func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return h.jwtPubKey, nil
		},
	)
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, jwt.ErrSignatureInvalid
	}

	return claims, nil
}

// readPump keeps the WebSocket connection alive by reading pings.
func (h *WSHandler) readPump(userID uuid.UUID, conn *websocket.Conn) {
	defer func() {
		h.hub.UnregisterClient(userID, conn)
		conn.Close(websocket.StatusNormalClosure, "")
	}()

	ctx := context.Background()

	for {
		_, _, err := conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusGoingAway ||
				websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return
			}
			h.log.Debug("websocket read error",
				"user_id", userID.String(),
				"error", err.Error(),
			)
			return
		}
	}
}

// checkOrigin validates the Origin header for browser clients.
func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	originLower := strings.ToLower(origin)
	return strings.HasPrefix(originLower, "http://localhost") ||
		strings.HasPrefix(originLower, "http://127.0.0.1") ||
		strings.Contains(originLower, "192.168.0.")
}
