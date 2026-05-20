package http

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	wshandler "github.com/companyofcreators/notification-service/internal/interfaces/ws"
)

// NewRouter creates and configures the HTTP router with all notification endpoints.
func NewRouter(handler *NotificationHandler, wsHandler *wshandler.WSHandler, log *slog.Logger) *chi.Mux {
	r := chi.NewRouter()

	// Middleware stack
	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(chimiddleware.Logger)
	r.Use(chimiddleware.Recoverer)
	r.Use(requestLogger(log))

	// Health check
	r.Get("/internal/health", handler.Health)

	// WebSocket endpoint for real-time push
	r.Get("/ws", wsHandler.HandleWebSocket)

	// Notification endpoints
	r.Route("/internal/notifications", func(r chi.Router) {
		r.Get("/", handler.List)
		r.Get("/unread-count", handler.GetUnreadCount)
		r.Post("/read-all", handler.MarkAllAsRead)
		r.Post("/{id}/read", handler.MarkAsRead)
		r.Delete("/{id}", handler.Delete)
	})

	return r
}

// requestLogger returns middleware that logs incoming requests using slog.
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.DebugContext(r.Context(), "incoming request",
				"method", r.Method,
				"path", r.URL.Path,
				"remote_addr", r.RemoteAddr,
				"request_id", r.Header.Get("X-Request-ID"),
			)
			next.ServeHTTP(w, r)
		})
	}
}
