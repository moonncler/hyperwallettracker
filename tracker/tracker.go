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

type Broadcast func(v interface{})

type Manager struct {
	db        *db.DB
	broadcast Broadcast
	rootCtx   context.Context
	mu        sync.RWMutex
	clients   map[string]*walletEntry
	chatCache map[string][]int64
}

type walletEntry struct {
	client *hl.Client
	cancel context.CancelFunc
}

func NewManager(database *db.DB, broadcast Broadcast) *Manager {
	return &Manager{
		db:        database,
		broadcast: broadcast,
		clients:   make(map[string]*walletEntry),
		chatCache: make(map[string][]int64),
	}
}

func (m *Manager) SetBroadcast(b Broadcast) { m.broadcast = b }

func (m *Manager) Start(ctx context.Context) error {
	m.rootCtx = ctx
	wallets, err := m.db.ListWallets(ctx)
	if err != nil {
		return err
	}
	for _, w := range wallets {
		m.addToCache(w.Address, w.ChatID)
		m.startClient(w.Address)
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
		m.startClient(address)
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

func (m *Manager) ListTracked() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.clients))
	for addr := range m.clients {
		out = append(out, addr)
	}
	return out
}

func (m *Manager) startClient(address string) {
	ctx, cancel := context.WithCancel(m.rootCtx)
	client := hl.NewClient(ctx, address)
	m.mu.Lock()
	m.clients[address] = &walletEntry{client: client, cancel: cancel}
	m.mu.Unlock()
	go func() {
		log.Printf("[tracker] start %s", address)
		client.Run()
		log.Printf("[tracker] stopped %s", address)
	}()
	go m.consume(ctx, client)
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

func (m *Manager) consume(ctx context.Context, client *hl.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-client.Events:
			if !ok {
				return
			}
			m.dispatch(evt)
		}
	}
}

func (m *Manager) dispatch(evt hl.Event) {
	eventTime := evt.EventTime()
	if eventTime > 0 {
		log.Printf("[lag] %s %s: HL→server %dms", evt.Kind, evt.Address[:8], time.Now().UnixMilli()-eventTime)
	}
	if m.broadcast != nil {
		m.broadcast(webPayload(evt))
	}
	go func() {
		ctx := context.Background()
		payload, _ := json.Marshal(evt)
		_ = m.db.SaveEvent(ctx, evt.Address, string(evt.Kind), string(payload))
	}()
}

func webPayload(evt hl.Event) map[string]interface{} {
	p := map[string]interface{}{
		"kind":    evt.Kind,
		"address": evt.Address,
	}
	switch evt.Kind {
	case hl.KindFill:
		p["fill"] = evt.Fill
	case hl.KindFunding:
		p["funding"] = evt.Funding
	case hl.KindLiquidation:
		p["liq"] = evt.Liq
	case hl.KindNonUserCancel:
		p["cancel"] = evt.Cancel
	case hl.KindOrderUpdate:
		p["order"] = evt.Order
	case hl.KindTwapUpdate:
		p["twap"] = evt.Twap
	}
	return p
}

func itoa(n int64) string { return fmt.Sprintf("%d", n) }
