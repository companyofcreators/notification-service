package config

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

// Config holds all configuration for the notification service.
type Config struct {
	HTTPAddress   string `env:"HTTP_ADDRESS" env-default:":8087"`
	DBDSN         string `env:"DB_DSN" env-required:"true"`
	KafkaBrokers  string `env:"KAFKA_BROKERS" env-default:"localhost:9092"`
	ConsumerGroup string `env:"KAFKA_CONSUMER_GROUP" env-default:"notification-service"`
	HeaderHMACKey string `env:"HEADER_HMAC_KEY" env-default:"diploma-internal-hmac-secret-key-2026"`
	LogLevel      string `env:"LOG_LEVEL" env-default:"info"`
	AllowedOrigin     string `env:"WS_ALLOWED_ORIGIN" env-default:""`
	UserServiceURL string `env:"USER_SERVICE_URL" env-default:"http://localhost:8082"`
	JWTPublicKeyPath  string `env:"JWT_PUBLIC_KEY_PATH" env-default:"../keys/public.pem"`
}

// Load reads configuration from environment variables and .env file.
func Load() (*Config, error) {
	if err := godotenv.Load(".env"); err != nil {
		slog.Warn(".env file not found, using environment variables", "error", err)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(".env", &cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	if c.DBDSN == "" {
		return fmt.Errorf("DB_DSN is required")
	}
	if c.KafkaBrokers == "" {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	if c.ConsumerGroup == "" {
		return fmt.Errorf("KAFKA_CONSUMER_GROUP is required")
	}
	return nil
}

func (c *Config) KafkaBrokersList() []string {
	if c.KafkaBrokers == "" {
		return []string{"localhost:9092"}
	}
	return splitBrokers(c.KafkaBrokers)
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
