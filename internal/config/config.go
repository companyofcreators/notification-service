package config

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

// Config holds all configuration for the notification service.
type Config struct {
	HTTPAddress   string `env:"HTTP_ADDRESS" env-default:":8087"`
	DBDSN         string `env:"DB_DSN" env-required:"true"`
	KafkaBrokers  string `env:"KAFKA_BROKERS" env-default:"localhost:9092"`
	ConsumerGroup string `env:"KAFKA_CONSUMER_GROUP" env-default:"notification-service"`
	LogLevel      string `env:"LOG_LEVEL" env-default:"info"`
}

// Load reads configuration from environment variables and .env file.
func Load() (*Config, error) {
	_ = godotenv.Load(".env")

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
	return splitAndTrim(c.KafkaBrokers)
}

func splitAndTrim(s string) []string {
	var result []string
	current := ""
	for _, ch := range s {
		if ch == ',' {
			trimmed := trimSpace(current)
			if trimmed != "" {
				result = append(result, trimmed)
			}
			current = ""
		} else {
			current += string(ch)
		}
	}
	trimmed := trimSpace(current)
	if trimmed != "" {
		result = append(result, trimmed)
	}
	return result
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
