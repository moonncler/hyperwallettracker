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
	"hyperwallettracker/web"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg := config.Load()

	database, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer database.Close()

	// WebSocket hub for browser clients
	hub := web.NewHub()

	tgBot, err := bot.New(cfg, database)
	if err != nil {
		log.Fatalf("bot: %v", err)
	}

	// wire hub broadcast into tracker
	tgBot.Manager().SetBroadcast(hub.Broadcast)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := tgBot.Manager().Start(ctx); err != nil {
		log.Fatalf("tracker start: %v", err)
	}

	// web server
	addr := ":" + cfg.Port
	webSrv := web.NewServer(hub, addr)
	go func() {
		if err := webSrv.Run(); err != nil {
			log.Printf("[web] server error: %v", err)
		}
	}()

	log.Println("Started. Web UI at http://localhost" + addr)
	tgBot.Run(ctx)
	log.Println("Shutdown complete.")
}
