package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	domain "github.com/companyofcreators/notification-service/internal/domain/notification"
	"github.com/companyofcreators/notification-service/internal/application/notification"
)

// NotificationHandler handles HTTP requests for notifications.
type NotificationHandler struct {
	list    *notification.List
	deliver *notification.Deliver
	log     *slog.Logger
}

// NewNotificationHandler creates a new NotificationHandler.
func NewNotificationHandler(list *notification.List, deliver *notification.Deliver, log *slog.Logger) *NotificationHandler {
	return &NotificationHandler{
		list:    list,
		deliver: deliver,
		log:     log,
	}
}

// List returns paginated notifications for the authenticated user.
func (h *NotificationHandler) List(w http.ResponseWriter, r *http.Request) {
	userIDStr := r.Header.Get("X-User-ID")
	if userIDStr == "" {
		writeJSON(w, http.StatusUnauthorized, errorResponse("пользователь не авторизован"))
		return
	}

	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("недействительный ID пользователя"))
		return
	}

	limit := parseQueryInt(r, "limit", 20)
	offset := parseQueryInt(r, "offset", 0)

	query := notification.ListQuery{
		UserID: userID,
		Limit:  limit,
		Offset: offset,
	}

	result, err := h.list.Execute(r.Context(), query)
	if err != nil {
		h.log.ErrorContext(r.Context(), "не удалось получить список уведомлений", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("не удалось получить список уведомлений"))
		return
	}

	response := toNotificationListResponse(result.Notifications, result.Total, result.UnreadCount)
	writeJSON(w, http.StatusOK, successResponse(response))
}

// GetUnreadCount returns the count of unread notifications.
func (h *NotificationHandler) GetUnreadCount(w http.ResponseWriter, r *http.Request) {
	userIDStr := r.Header.Get("X-User-ID")
	if userIDStr == "" {
		writeJSON(w, http.StatusUnauthorized, errorResponse("пользователь не авторизован"))
		return
	}

	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("недействительный ID пользователя"))
		return
	}

	count, err := h.list.GetUnreadCount(r.Context(), userID)
	if err != nil {
		h.log.ErrorContext(r.Context(), "не удалось получить количество непрочитанных", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("не удалось получить количество непрочитанных"))
		return
	}

	writeJSON(w, http.StatusOK, successResponse(UnreadCountResponse{Count: count}))
}

// MarkAsRead marks a single notification as read.
func (h *NotificationHandler) MarkAsRead(w http.ResponseWriter, r *http.Request) {
	userIDStr := r.Header.Get("X-User-ID")
	if userIDStr == "" {
		writeJSON(w, http.StatusUnauthorized, errorResponse("пользователь не авторизован"))
		return
	}

	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("недействительный ID пользователя"))
		return
	}

	notifIDStr := chi.URLParam(r, "id")
	notifID, err := uuid.Parse(notifIDStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("недействительный ID уведомления"))
		return
	}

	err = h.list.MarkAsRead(r.Context(), notifID, userID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse("уведомление не найдено"))
			return
		}
		h.log.ErrorContext(r.Context(), "failed to mark notification as read", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("не удалось отметить как прочитанное"))
		return
	}

	// Push updated unread count
	h.deliver.PushUnreadCount(r.Context(), userID)

	writeJSON(w, http.StatusOK, successResponse(map[string]bool{"marked_read": true}))
}

// MarkAllAsRead marks all notifications as read for the authenticated user.
func (h *NotificationHandler) MarkAllAsRead(w http.ResponseWriter, r *http.Request) {
	userIDStr := r.Header.Get("X-User-ID")
	if userIDStr == "" {
		writeJSON(w, http.StatusUnauthorized, errorResponse("пользователь не авторизован"))
		return
	}

	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("недействительный ID пользователя"))
		return
	}

	err = h.list.MarkAllAsRead(r.Context(), userID)
	if err != nil {
		h.log.ErrorContext(r.Context(), "не удалось отметить все как прочитанные", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("не удалось отметить все как прочитанные"))
		return
	}

	// Push updated unread count (should be 0)
	h.deliver.PushUnreadCount(r.Context(), userID)

	writeJSON(w, http.StatusOK, successResponse(MarkAllReadResponse{MarkedRead: true}))
}

// Delete removes a notification.
func (h *NotificationHandler) Delete(w http.ResponseWriter, r *http.Request) {
	userIDStr := r.Header.Get("X-User-ID")
	if userIDStr == "" {
		writeJSON(w, http.StatusUnauthorized, errorResponse("пользователь не авторизован"))
		return
	}

	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("недействительный ID пользователя"))
		return
	}

	notifIDStr := chi.URLParam(r, "id")
	notifID, err := uuid.Parse(notifIDStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("недействительный ID уведомления"))
		return
	}

	err = h.list.Delete(r.Context(), notifID, userID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, errorResponse("уведомление не найдено"))
			return
		}
		h.log.ErrorContext(r.Context(), "не удалось удалить уведомление", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("не удалось удалить уведомление"))
		return
	}

	writeJSON(w, http.StatusOK, successResponse(DeleteResponse{Deleted: true}))
}

// Health returns a health check response.
func (h *NotificationHandler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, successResponse(map[string]string{
		"status":  "ok",
		"service": "notification-service",
	}))
}

// ---- Response Helpers ----

type apiResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   *apiError   `json:"error,omitempty"`
}

type apiError struct {
	Message string `json:"message"`
}

func successResponse(data interface{}) apiResponse {
	return apiResponse{Success: true, Data: data}
}

func errorResponse(message string) apiResponse {
	return apiResponse{Success: false, Error: &apiError{Message: message}}
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if data != nil {
		if err := json.NewEncoder(w).Encode(data); err != nil {
			http.Error(w, `{"success":false,"error":{"message":"внутренняя ошибка кодирования"}}`, http.StatusInternalServerError)
		}
	}
}

func parseQueryInt(r *http.Request, key string, defaultVal int) int {
	val := r.URL.Query().Get(key)
	if val == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(val)
	if err != nil || i < 0 {
		return defaultVal
	}
	return i
}
