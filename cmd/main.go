package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"hyperwallettracker/config"
	"hyperwallettracker/db"
	"hyperwallettracker/tracker"
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

	hub := web.NewHub()
	mgr := tracker.NewManager(database, hub.Broadcast)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := mgr.Start(ctx); err != nil {
		log.Fatalf("tracker start: %v", err)
	}

	addr := ":" + cfg.Port
	webSrv := web.NewServer(hub, addr, mgr)
	go func() {
		if err := webSrv.Run(); err != nil {
			log.Printf("[web] %v", err)
		}
	}()

	log.Printf("[web] UI at http://localhost%s", addr)
	<-ctx.Done()
	log.Println("Shutdown complete.")
}
