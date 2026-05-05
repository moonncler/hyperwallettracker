package bot

import (
	"context"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"hyperwallettracker/config"
	"hyperwallettracker/db"
	"hyperwallettracker/tracker"
)

const (
	sendWorkers  = 8
	sendQueueCap = 512
)

type sendJob struct {
	chatID int64
	text   string
}

type Bot struct {
	api     *tgbotapi.BotAPI
	cfg     *config.Config
	db      *db.DB
	manager *tracker.Manager
	sendQ   chan sendJob
}

func New(cfg *config.Config, database *db.DB) (*Bot, error) {
	// Timeout must be > long-poll interval (30s). 60s covers send + polling.
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        sendWorkers * 2,
			MaxIdleConnsPerHost: sendWorkers * 2,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second,
		},
		Timeout: 60 * time.Second,
	}

	endpoint := tgbotapi.APIEndpoint
	if cfg.LocalBotAPI != "" {
		endpoint = cfg.LocalBotAPI + "/bot%s/%s"
	}
	api, err := tgbotapi.NewBotAPIWithClient(cfg.TelegramToken, endpoint, httpClient)
	if err != nil {
		return nil, err
	}

	b := &Bot{
		api:   api,
		cfg:   cfg,
		db:    database,
		sendQ: make(chan sendJob, sendQueueCap),
	}
	b.manager = tracker.NewManager(database, b.Send, nil)
	return b, nil
}

func (b *Bot) Manager() *tracker.Manager { return b.manager }

func (b *Bot) startSenders(ctx context.Context) {
	for i := 0; i < sendWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case job := <-b.sendQ:
					t0 := time.Now()
					msg := tgbotapi.NewMessage(job.chatID, job.text)
					msg.ParseMode = tgbotapi.ModeHTML
					msg.DisableWebPagePreview = true
					if _, err := b.api.Send(msg); err != nil {
						log.Printf("[bot] send to %d: %v", job.chatID, err)
					} else {
						log.Printf("[tg] sendMessage took %dms", time.Since(t0).Milliseconds())
					}
				}
			}
		}()
	}
}

func (b *Bot) Send(chatID int64, text string) {
	select {
	case b.sendQ <- sendJob{chatID, text}:
	default:
		log.Printf("[bot] send queue full, dropping message to %d", chatID)
	}
}

func (b *Bot) reply(chatID int64, text string) {
	b.Send(chatID, text)
}

func (b *Bot) Run(ctx context.Context) {
	b.startSenders(ctx)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	updates := b.api.GetUpdatesChan(u)

	log.Printf("[bot] @%s running", b.api.Self.UserName)

	for {
		select {
		case <-ctx.Done():
			b.api.StopReceivingUpdates()
			return
		case upd, ok := <-updates:
			if !ok {
				return
			}
			if upd.Message == nil {
				continue
			}
			go b.handleMessage(ctx, upd.Message)
		}
	}
}

func (b *Bot) handleMessage(ctx context.Context, msg *tgbotapi.Message) {
	if !msg.IsCommand() {
		return
	}
	chatID := msg.Chat.ID
	args := strings.Fields(msg.CommandArguments())

	switch msg.Command() {
	case "start", "help":
		b.cmdHelp(chatID)
	case "add":
		b.cmdAdd(ctx, chatID, args)
	case "remove", "del":
		b.cmdRemove(ctx, chatID, args)
	case "list":
		b.cmdList(ctx, chatID)
	case "positions":
		b.cmdPositions(ctx, chatID, args)
	case "orders":
		b.cmdOrders(ctx, chatID, args)
	default:
		b.reply(chatID, "❓ Unknown command. Use /help")
	}
}
