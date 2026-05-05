package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"hyperwallettracker/bot"
	"hyperwallettracker/config"
	"hyperwallettracker/db"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg := config.Load()

	database, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer database.Close()

	tgBot, err := bot.New(cfg, database)
	if err != nil {
		log.Fatalf("bot: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Restore wallets from DB and start all WS clients
	if err := tgBot.Manager().Start(ctx); err != nil {
		log.Fatalf("tracker start: %v", err)
	}

	log.Println("Bot started. Press Ctrl+C to stop.")
	tgBot.Run(ctx) // blocks until ctx cancelled
	log.Println("Shutdown complete.")
}
