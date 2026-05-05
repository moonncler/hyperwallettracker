package tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"hyperwallettracker/db"
	"hyperwallettracker/hl"
)

type Notify func(chatID int64, text string)

type Manager struct {
	db      *db.DB
	notify  Notify
	mu      sync.RWMutex
	clients map[string]*walletEntry
	// in-memory cache: address -> []chatID (avoids DB hit on every event)
	chatCache map[string][]int64
}

type walletEntry struct {
	client *hl.Client
	cancel context.CancelFunc
}

func NewManager(database *db.DB, notify Notify) *Manager {
	return &Manager{
		db:        database,
		notify:    notify,
		clients:   make(map[string]*walletEntry),
		chatCache: make(map[string][]int64),
	}
}

func (m *Manager) Start(ctx context.Context) error {
	wallets, err := m.db.ListWallets(ctx)
	if err != nil {
		return err
	}
	for _, w := range wallets {
		m.addToCache(w.Address, w.ChatID)
		m.startClient(ctx, w.Address)
	}
	return nil
}

func (m *Manager) AddWallet(ctx context.Context, address, label string, chatID int64) error {
	address = strings.ToLower(address)
	if err := m.db.AddWallet(ctx, address, label, chatID); err != nil {
		return err
	}
	m.addToCache(address, chatID)

	m.mu.RLock()
	_, exists := m.clients[address]
	m.mu.RUnlock()
	if !exists {
		m.startClient(ctx, address)
	}
	return nil
}

func (m *Manager) RemoveWallet(ctx context.Context, address string, chatID int64) error {
	address = strings.ToLower(address)
	if err := m.db.RemoveWallet(ctx, address, chatID); err != nil {
		return err
	}
	m.removeFromCache(address, chatID)

	m.mu.RLock()
	remaining := m.getCached(address)
	m.mu.RUnlock()
	if len(remaining) == 0 {
		m.stopClient(address)
	}
	return nil
}

func (m *Manager) addToCache(address string, chatID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.chatCache[address] {
		if id == chatID {
			return
		}
	}
	m.chatCache[address] = append(m.chatCache[address], chatID)
}

func (m *Manager) removeFromCache(address string, chatID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := m.chatCache[address]
	for i, id := range ids {
		if id == chatID {
			m.chatCache[address] = append(ids[:i], ids[i+1:]...)
			return
		}
	}
}

func (m *Manager) getCached(address string) []int64 {
	return m.chatCache[address]
}

func (m *Manager) startClient(parentCtx context.Context, address string) {
	ctx, cancel := context.WithCancel(parentCtx)
	client := hl.NewClient(ctx, address)

	m.mu.Lock()
	m.clients[address] = &walletEntry{client: client, cancel: cancel}
	m.mu.Unlock()

	go func() {
		log.Printf("[tracker] start %s", address)
		client.Run()
		log.Printf("[tracker] stopped %s", address)
	}()

	go m.consume(ctx, address, client)
}

func (m *Manager) stopClient(address string) {
	m.mu.Lock()
	entry, ok := m.clients[address]
	if ok {
		delete(m.clients, address)
		delete(m.chatCache, address)
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
	text := formatEvent(evt)
	if text == "" {
		return
	}

	m.mu.RLock()
	ids := make([]int64, len(m.chatCache[evt.Address]))
	copy(ids, m.chatCache[evt.Address])
	m.mu.RUnlock()

	if len(ids) == 0 {
		return
	}

	recvAt := time.Now()
	eventTime := evt.EventTime() // ms timestamp from HL
	if eventTime > 0 {
		lag := recvAt.UnixMilli() - eventTime
		log.Printf("[lag] %s %s: HL→server %dms", evt.Kind, evt.Address[:8], lag)
	}

	for _, chatID := range ids {
		chatID := chatID
		go m.notify(chatID, text)
	}

	go func() {
		payload, _ := json.Marshal(evt)
		_ = m.db.SaveEvent(ctx, evt.Address, string(evt.Kind), string(payload))
	}()
}

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
		if len(fn.Usdc) > 0 && fn.Usdc[0] == '-' {
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

	case hl.KindTwapUpdate:
		t := evt.Twap
		emoji, action := twapStatusEmoji(t.Status)
		side := "🟢 BUY"
		if t.Twap.Side == "A" {
			side = "🔴 SELL"
		}
		reduceOnly := ""
		if t.Twap.ReduceOnly {
			reduceOnly = " · Reduce Only"
		}
		return fmt.Sprintf(
			"%s <b>TWAP %s</b>\n"+
				"👛 <code>%s</code>\n"+
				"📌 %s · %s%s\n"+
				"📦 Size: %s · Duration: %dm\n"+
				"🆔 ID: %d",
			emoji, action,
			short(evt.Address),
			t.Twap.Coin, side, reduceOnly,
			t.Twap.Sz, t.Twap.Minutes,
			t.Twap.TwapID,
		)
	}
	return ""
}

func twapStatusEmoji(status string) (string, string) {
	switch status {
	case "activated":
		return "🚀", "STARTED"
	case "terminated":
		return "🛑", "CANCELLED"
	case "finished":
		return "✅", "FINISHED"
	default:
		return "🔄", strings.ToUpper(status)
	}
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
