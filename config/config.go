package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DBPath string
	Port   string
}

func Load() *Config {
	_ = godotenv.Load()
	return &Config{
		DBPath: envOr("DB_PATH", "tracker.db"),
		Port:   envOr("PORT", "8080"),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}
