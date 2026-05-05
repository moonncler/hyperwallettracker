package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	TelegramToken string
	AdminIDs      []int64
	DBPath        string
	HLWebsocket   string
	HLInfoURL     string
	LocalBotAPI   string // optional: URL of local Telegram Bot API server
}

func Load() *Config {
	_ = godotenv.Load()

	cfg := &Config{
		TelegramToken: mustEnv("TELEGRAM_BOT_TOKEN"),
		DBPath:        envOr("DB_PATH", "tracker.db"),
		HLWebsocket:   "wss://api.hyperliquid.xyz/ws",
		HLInfoURL:     "https://api.hyperliquid.xyz/info",
		LocalBotAPI:   envOr("LOCAL_BOT_API", ""),
	}

	for _, s := range strings.Split(envOr("ADMIN_IDS", ""), ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			log.Printf("invalid ADMIN_IDS entry %q: %v", s, err)
			continue
		}
		cfg.AdminIDs = append(cfg.AdminIDs, id)
	}

	return cfg
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
