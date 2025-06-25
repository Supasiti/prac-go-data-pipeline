package config

import (
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	OpenSearch *OpenSearchConfig
}

func NewConfig() (*Config, error) {
	err := godotenv.Load()
	if err != nil {
		slog.Error("Error loading .env file", slog.Any("error", err))
		return nil, err
	}

	cfg := &Config{
		OpenSearch: newOpenSearchConfig(),
	}

	return cfg, nil
}

type OpenSearchConfig struct {
	Username string
	Password string
	Url      string
}

func newOpenSearchConfig() *OpenSearchConfig {
	return &OpenSearchConfig{
		Username: os.Getenv("OPENSEARCH_USERNAME"),
		Password: os.Getenv("OPENSEARCH_PASSWORD"),
		Url:      os.Getenv("OPENSEARCH_URL"),
	}
}
