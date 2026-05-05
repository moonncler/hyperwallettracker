package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"hyperwallettracker/db"
	"hyperwallettracker/hl"
)

// Notify is the callback the bot registers: send message to chatID.
type Notify func(chatID int64, text string)

// Manager tracks all wallets, starts/stops hl.Client goroutines.
type Manager struct {
	db      *db.DB
	notify  Notify
	mu      sync.RWMutex
	clients map[string]*walletEntry // keyed by lowercase address
}

type walletEntry struct {
	client *hl.Client
	cancel context.CancelFunc
}

func NewManager(database *db.DB, notify Notify) *Manager {
	return &Manager{
		db:      database,
		notify:  notify,
		clients: make(map[string]*walletEntry),
	}
}

// Start restores all wallets from DB and begins tracking.
func (m *Manager) Start(ctx context.Context) error {
	wallets, err := m.db.ListWallets(ctx)
	if err != nil {
		return err
	}
	for _, w := range wallets {
		m.startClient(ctx, w.Address)
	}
	return nil
}

// AddWallet adds a wallet to the DB and starts tracking if not already.
func (m *Manager) AddWallet(ctx context.Context, address, label string, chatID int64) error {
	address = strings.ToLower(address)
	if err := m.db.AddWallet(ctx, address, label, chatID); err != nil {
		return err
	}
	m.mu.Lock()
	_, exists := m.clients[address]
	m.mu.Unlock()
	if !exists {
		m.startClient(ctx, address)
	}
	return nil
}

// RemoveWallet removes a chat's subscription; stops the WS client only if no
// other chats are still watching the same address.
func (m *Manager) RemoveWallet(ctx context.Context, address string, chatID int64) error {
	address = strings.ToLower(address)
	if err := m.db.RemoveWallet(ctx, address, chatID); err != nil {
		return err
	}
	remaining, err := m.db.GetChatsForWallet(ctx, address)
	if err != nil {
		return err
	}
	if len(remaining) == 0 {
		m.stopClient(address)
	}
	return nil
}

func (m *Manager) startClient(parentCtx context.Context, address string) {
	ctx, cancel := context.WithCancel(parentCtx)
	client := hl.NewClient(ctx, address)

	m.mu.Lock()
	m.clients[address] = &walletEntry{client: client, cancel: cancel}
	m.mu.Unlock()

	go func() {
		log.Printf("[tracker] start %s", address)
		client.Run() // blocks; reconnects internally
		log.Printf("[tracker] stopped %s", address)
	}()

	go m.consume(ctx, address, client)
}

func (m *Manager) stopClient(address string) {
	m.mu.Lock()
	entry, ok := m.clients[address]
	if ok {
		delete(m.clients, address)
	}
	m.mu.Unlock()
	if ok {
		entry.cancel()
	}
}

func (m *Manager) consume(ctx context.Context, address string, client *hl.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-client.Events:
			if !ok {
				return
			}
			m.dispatch(ctx, evt)
		}
	}
}

func (m *Manager) dispatch(ctx context.Context, evt hl.Event) {
	chatIDs, err := m.db.GetChatsForWallet(ctx, evt.Address)
	if err != nil || len(chatIDs) == 0 {
		return
	}

	text := formatEvent(evt)
	if text == "" {
		return
	}

	// persist to DB (best-effort)
	payload, _ := json.Marshal(evt)
	_ = m.db.SaveEvent(ctx, evt.Address, string(evt.Kind), string(payload))

	for _, chatID := range chatIDs {
		m.notify(chatID, text)
	}
}

// formatEvent builds the Telegram message for an event.
func formatEvent(evt hl.Event) string {
	switch evt.Kind {

	case hl.KindFill:
		f := evt.Fill
		side := "🟢 BUY"
		if f.Side == "A" {
			side = "🔴 SELL"
		}
		twap := ""
		if f.TwapID != nil {
			twap = " [TWAP]"
		}
		dir := ""
		if f.Dir != "" {
			dir = " · " + f.Dir
		}
		pnl := ""
		if f.ClosedPnl != "" && f.ClosedPnl != "0" {
			pnl = "\n💰 PnL: " + f.ClosedPnl + " USDC"
		}
		return "📊 <b>FILL" + twap + "</b>" + dir + "\n" +
			"👛 <code>" + short(evt.Address) + "</code>\n" +
			"📌 " + f.Coin + " · " + side + "\n" +
			"💲 Price: " + f.Px + " · Size: " + f.Sz +
			pnl + "\n" +
			"💸 Fee: " + f.Fee + " " + f.FeeToken

	case hl.KindFunding:
		fn := evt.Funding
		sign := "📈"
		if fn.Usdc[0] == '-' {
			sign = "📉"
		}
		return sign + " <b>FUNDING</b>\n" +
			"👛 <code>" + short(evt.Address) + "</code>\n" +
			"📌 " + fn.Coin + "\n" +
			"💵 " + fn.Usdc + " USDC · Rate: " + fn.FundingRate

	case hl.KindLiquidation:
		liq := evt.Liq
		return "🚨 <b>LIQUIDATION</b>\n" +
			"👛 <code>" + short(evt.Address) + "</code>\n" +
			"💸 Notional: " + liq.LiquidatedNtl + "\n" +
			"💰 Fee: " + liq.LiquidatedFee

	case hl.KindNonUserCancel:
		c := evt.Cancel
		return "⚠️ <b>ORDER CANCELLED (system)</b>\n" +
			"👛 <code>" + short(evt.Address) + "</code>\n" +
			"📌 " + c.Coin + " · OID: " + itoa(c.Oid)

	case hl.KindOrderUpdate:
		o := evt.Order
		statusEmoji := orderEmoji(o.Status)
		twap := ""
		if o.Order.TwapID != nil {
			twap = " [TWAP]"
		}
		return statusEmoji + " <b>ORDER " + strings.ToUpper(o.Status) + twap + "</b>\n" +
			"👛 <code>" + short(evt.Address) + "</code>\n" +
			"📌 " + o.Order.Coin + " · " + sideStr(o.Order.Side) + "\n" +
			"📋 Type: " + o.Order.OrderType + "\n" +
			"💲 " + o.Order.LimitPx + " · Sz: " + o.Order.Sz
	}
	return ""
}

func orderEmoji(status string) string {
	switch status {
	case "open":
		return "🔷"
	case "filled":
		return "✅"
	case "canceled", "marginCanceled":
		return "❌"
	case "triggered":
		return "⚡"
	case "rejected":
		return "🚫"
	default:
		return "🔹"
	}
}

func sideStr(s string) string {
	if s == "B" {
		return "BUY"
	}
	return "SELL"
}

func short(addr string) string {
	if len(addr) <= 10 {
		return addr
	}
	return addr[:6] + "…" + addr[len(addr)-4:]
}

func itoa(n int64) string {
	return fmt.Sprintf("%d", n)
}
