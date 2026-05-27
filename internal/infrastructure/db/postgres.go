package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// PostgresConfig holds the configuration for PostgreSQL connection.
type PostgresConfig struct {
	DSN             string
	MaxConns        int
	MaxIdleConns    int
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// DefaultPostgresConfig returns a configuration with sensible defaults.
func DefaultPostgresConfig(dsn string) PostgresConfig {
	return PostgresConfig{
		DSN:             dsn,
		MaxConns:        25,
		MaxIdleConns:    5,
		MaxConnLifetime: 5 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
	}
}

// NewPostgresPool creates a new connection pool to PostgreSQL.
func NewPostgresPool(ctx context.Context, cfg PostgresConfig, log *slog.Logger) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.MaxConnLifetime)
	db.SetConnMaxIdleTime(cfg.MaxConnIdleTime)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.InfoContext(ctx, "connected to PostgreSQL",
		"dsn", maskDSN(cfg.DSN),
		"max_connections", cfg.MaxConns,
	)

	return db, nil
}

// maskDSN hides the password in the DSN for logging.
func maskDSN(dsn string) string {
	result := []byte(dsn)
	for i := 0; i < len(result); i++ {
		if i+9 < len(result) && string(result[i:i+9]) == "password=" {
			j := i + 9
			for j < len(result) && result[j] != ' ' && result[j] != '&' {
				result[j] = '*'
				j++
			}
			break
		}
	}
	return string(result)
}
