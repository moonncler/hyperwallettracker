package bot

import (
	"context"
	"log"
	"strings"
	"sync"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"hyperwallettracker/config"
	"hyperwallettracker/db"
	"hyperwallettracker/tracker"
)

type Bot struct {
	api     *tgbotapi.BotAPI
	cfg     *config.Config
	db      *db.DB
	manager *tracker.Manager
	mu      sync.Mutex
}

func New(cfg *config.Config, database *db.DB) (*Bot, error) {
	api, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		return nil, err
	}
	b := &Bot{
		api: api,
		cfg: cfg,
		db:  database,
	}
	b.manager = tracker.NewManager(database, b.Send)
	return b, nil
}

func (b *Bot) Manager() *tracker.Manager { return b.manager }

// Send delivers a message to a chat (called from tracker goroutines).
func (b *Bot) Send(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = tgbotapi.ModeHTML
	msg.DisableWebPagePreview = true
	if _, err := b.api.Send(msg); err != nil {
		log.Printf("[bot] send to %d: %v", chatID, err)
	}
}

func (b *Bot) reply(chatID int64, text string) {
	b.Send(chatID, text)
}

// Run starts the update polling loop (blocking).
func (b *Bot) Run(ctx context.Context) {
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
			b.handleMessage(ctx, upd.Message)
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
