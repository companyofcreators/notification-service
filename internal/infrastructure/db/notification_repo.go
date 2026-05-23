package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/lib/pq"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/companyofcreators/notification-service/internal/domain/notification"
)

// NotificationRepo implements notification.NotificationRepository using PostgreSQL.
type NotificationRepo struct {
	pool *sqlx.DB
	log  *slog.Logger
}

// NewNotificationRepo creates a new NotificationRepo.
func NewNotificationRepo(pool *sqlx.DB, log *slog.Logger) *NotificationRepo {
	return &NotificationRepo{pool: pool, log: log}
}

// Create inserts a single notification into the database.
func (r *NotificationRepo) Create(ctx context.Context, n *notification.Notification) error {
	channels := channelsToStringSlice(n.Channels)
	_, err := r.pool.ExecContext(ctx,
		`INSERT INTO notifications (id, user_id, type, title, body, data, is_read, channels, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		n.ID, n.UserID, string(n.Type), n.Title, n.Body, n.Data, n.IsRead, pq.Array(channels), n.CreatedAt,
	)
	if err != nil {
		r.log.ErrorContext(ctx, "failed to create notification", "error", err.Error())
		return fmt.Errorf("create notification: %w", err)
	}
	return nil
}

// CreateBatch inserts multiple notifications using a single transaction.
func (r *NotificationRepo) CreateBatch(ctx context.Context, notifications []*notification.Notification) error {
	if len(notifications) == 0 {
		return nil
	}

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO notifications (id, user_id, type, title, body, data, is_read, channels, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (id) DO NOTHING`,
	)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, n := range notifications {
		channels := channelsToStringSlice(n.Channels)
		_, err := stmt.ExecContext(ctx,
			n.ID, n.UserID, string(n.Type), n.Title, n.Body, n.Data, n.IsRead, pq.Array(channels), n.CreatedAt,
		)
		if err != nil {
			r.log.ErrorContext(ctx, "failed to create notification in batch",
				"index", i,
				"error", err.Error(),
			)
			return fmt.Errorf("create batch notification at index %d: %w", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// ListByUser returns paginated notifications for a user, ordered by created_at DESC.
func (r *NotificationRepo) ListByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]*notification.Notification, int, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	var total int
	err := r.pool.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM notifications WHERE user_id = $1`,
		userID,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("count notifications: %w", err)
	}

	rows, err := r.pool.QueryContext(ctx,
		`SELECT id, user_id, type, title, body, data, is_read, channels, created_at
		 FROM notifications
		 WHERE user_id = $1
		 ORDER BY created_at DESC
		 LIMIT $2 OFFSET $3`,
		userID, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()

	notifications := make([]*notification.Notification, 0)
	for rows.Next() {
		n := &notification.Notification{}
		var channelsStr pq.StringArray
		var typeStr string
		if err := rows.Scan(
			&n.ID, &n.UserID, &typeStr, &n.Title, &n.Body, &n.Data,
			&n.IsRead, &channelsStr, &n.CreatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan notification: %w", err)
		}
		n.Type = notification.NotificationType(typeStr)
		n.Channels = stringSliceToChannels([]string(channelsStr))
		notifications = append(notifications, n)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("rows iteration: %w", err)
	}

	return notifications, total, nil
}

// MarkAsRead marks a single notification as read for the given user.
func (r *NotificationRepo) MarkAsRead(ctx context.Context, id, userID uuid.UUID) error {
	result, err := r.pool.ExecContext(ctx,
		`UPDATE notifications SET is_read = TRUE WHERE id = $1 AND user_id = $2`,
		id, userID,
	)
	if err != nil {
		return fmt.Errorf("mark as read: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		r.log.ErrorContext(ctx, "failed to get rows affected", "error", err.Error())
		return fmt.Errorf("rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return notification.ErrNotFound
	}
	return nil
}

// MarkAllAsRead marks all notifications as read for the given user.
func (r *NotificationRepo) MarkAllAsRead(ctx context.Context, userID uuid.UUID) error {
	_, err := r.pool.ExecContext(ctx,
		`UPDATE notifications SET is_read = TRUE WHERE user_id = $1 AND is_read = FALSE`,
		userID,
	)
	if err != nil {
		return fmt.Errorf("mark all as read: %w", err)
	}
	return nil
}

// GetUnreadCount returns the number of unread notifications for a user.
func (r *NotificationRepo) GetUnreadCount(ctx context.Context, userID uuid.UUID) (int, error) {
	var count int
	err := r.pool.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = FALSE`,
		userID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("get unread count: %w", err)
	}
	return count, nil
}

// Delete removes a notification by ID, scoped to the owning user.
func (r *NotificationRepo) Delete(ctx context.Context, id, userID uuid.UUID) error {
	result, err := r.pool.ExecContext(ctx,
		`DELETE FROM notifications WHERE id = $1 AND user_id = $2`,
		id, userID,
	)
	if err != nil {
		return fmt.Errorf("delete notification: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		r.log.ErrorContext(ctx, "failed to get rows affected", "error", err.Error())
		return fmt.Errorf("rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return notification.ErrNotFound
	}
	return nil
}

// GetPreferences returns the notification preferences for a user and notification type.
func (r *NotificationRepo) GetPreferences(ctx context.Context, userID uuid.UUID, notifType notification.NotificationType) (*notification.NotificationPreference, error) {
	pref := &notification.NotificationPreference{}
	var channelsStr pq.StringArray
	var typeStr string
	err := r.pool.QueryRowContext(ctx,
		`SELECT user_id, type, channels, enabled
		 FROM notification_preferences
		 WHERE user_id = $1 AND type = $2`,
		userID, string(notifType),
	).Scan(&pref.UserID, &typeStr, &channelsStr, &pref.Enabled)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get preferences: %w", err)
	}
	pref.Type = notification.NotificationType(typeStr)
	pref.Channels = stringSliceToChannels([]string(channelsStr))
	return pref, nil
}

// UpsertPreferences inserts or updates notification preferences.
func (r *NotificationRepo) UpsertPreferences(ctx context.Context, pref *notification.NotificationPreference) error {
	channels := channelsToStringSlice(pref.Channels)
	_, err := r.pool.ExecContext(ctx,
		`INSERT INTO notification_preferences (user_id, type, channels, enabled)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (user_id, type) DO UPDATE SET channels = $3, enabled = $4`,
		pref.UserID, string(pref.Type), pq.Array(channels), pref.Enabled,
	)
	if err != nil {
		return fmt.Errorf("upsert preferences: %w", err)
	}
	return nil
}

// channelsToStringSlice converts []DeliveryChannel to []string for PostgreSQL TEXT[].
func channelsToStringSlice(channels []notification.DeliveryChannel) []string {
	result := make([]string, len(channels))
	for i, ch := range channels {
		result[i] = string(ch)
	}
	return result
}

// stringSliceToChannels converts []string from PostgreSQL TEXT[] to []DeliveryChannel.
func stringSliceToChannels(strs []string) []notification.DeliveryChannel {
	result := make([]notification.DeliveryChannel, len(strs))
	for i, s := range strs {
		result[i] = notification.DeliveryChannel(s)
	}
	return result
}

// Ensure NotificationRepo satisfies the repository interface.
var _ notification.NotificationRepository = (*NotificationRepo)(nil)

