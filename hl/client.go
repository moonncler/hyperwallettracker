package hl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsURL   = "wss://api.hyperliquid.xyz/ws"
	infoURL = "https://api.hyperliquid.xyz/info"
)

type Client struct {
	Address    string
	Events     chan Event
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex // guards conn
	dedupMu    sync.Mutex // guards seenOrders
	conn       *websocket.Conn
	seenOrders map[string]struct{}
}

func NewClient(ctx context.Context, address string) *Client {
	ctx, cancel := context.WithCancel(ctx)
	return &Client{
		Address:    strings.ToLower(address),
		Events:     make(chan Event, 512),
		ctx:        ctx,
		cancel:     cancel,
		seenOrders: make(map[string]struct{}, 64),
	}
}

func (c *Client) Stop() {
	c.cancel()
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.mu.Unlock()
}

func (c *Client) Run() {
	defer close(c.Events)
	backoff := time.Second
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		err := c.connect()
		if c.ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("[hl] %s error: %v — reconnect in %s", c.Address, err, backoff)
		} else {
			log.Printf("[hl] %s disconnected — reconnect in %s", c.Address, backoff)
		}
		select {
		case <-time.After(backoff):
		case <-c.ctx.Done():
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) connect() error {
	dialer := websocket.Dialer{
		HandshakeTimeout:  10 * time.Second,
		ReadBufferSize:    65536,
		WriteBufferSize:   4096,
		EnableCompression: false,
	}
	log.Printf("[hl] %s dialing…", c.Address)
	conn, _, err := dialer.DialContext(c.ctx, wsURL, nil)
	if err != nil {
		log.Printf("[hl] %s dial error: %v", c.Address, err)
		return err
	}
	log.Printf("[hl] %s connected", c.Address)
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		conn.Close()
	}()

	// Hyperliquid requires JSON ping {"method":"ping"}, not WS ping frames
	go func() {
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-tick.C:
				c.mu.Lock()
				ok := c.conn == conn
				c.mu.Unlock()
				if !ok {
					return
				}
				if err := conn.WriteJSON(map[string]string{"method": "ping"}); err != nil {
					return
				}
			}
		}
	}()

	subs := []map[string]interface{}{
		{"type": "userEvents", "user": c.Address},
		{"type": "orderUpdates", "user": c.Address},
		{"type": "userTwapHistory", "user": c.Address},
	}
	for _, sub := range subs {
		if err := conn.WriteJSON(map[string]interface{}{
			"method":       "subscribe",
			"subscription": sub,
		}); err != nil {
			return fmt.Errorf("subscribe %v: %w", sub["type"], err)
		}
		log.Printf("[hl] %s subscribed: %s", c.Address, sub["type"])
	}

	// decouple read from parse
	rawCh := make(chan []byte, 256)
	go func() {
		defer close(rawCh)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				log.Printf("[hl] %s read error: %v", c.Address, err)
				return
			}
			select {
			case rawCh <- raw:
			default:
			}
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			return nil
		case raw, ok := <-rawCh:
			if !ok {
				if c.ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("ws read loop exited")
			}
			c.handleRaw(raw)
		}
	}
}

func (c *Client) handleRaw(raw []byte) {
	var env struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}

	switch env.Channel {
	case "userEvents":
		var d UserEventsData
		if err := json.Unmarshal(env.Data, &d); err != nil {
			return
		}
		// fills, funding, liquidation, nonUserCancel only
		// order events come exclusively from orderUpdates to avoid duplicates
		for i := range d.Fills {
			if !c.dedup("fill:" + d.Fills[i].Hash) {
				continue
			}
			c.emit(Event{Address: c.Address, Kind: KindFill, Fill: &d.Fills[i]})
		}
		for i := range d.Funding {
			c.emit(Event{Address: c.Address, Kind: KindFunding, Funding: &d.Funding[i]})
		}
		if d.Liquidation != nil {
			c.emit(Event{Address: c.Address, Kind: KindLiquidation, Liq: d.Liquidation})
		}
		for i := range d.NonUserCancel {
			c.emit(Event{Address: c.Address, Kind: KindNonUserCancel, Cancel: &d.NonUserCancel[i]})
		}
		// NOTE: d.Fills from userEvents can duplicate orderUpdates fills — dedup via seenOrders covers this

	case "orderUpdates":
		var updates []OrderUpdate
		if err := json.Unmarshal(env.Data, &updates); err != nil {
			return
		}
		for i := range updates {
			key := fmt.Sprintf("order:%d:%s", updates[i].Order.Oid, updates[i].Status)
			if !c.dedup(key) {
				continue
			}
			c.emit(Event{Address: c.Address, Kind: KindOrderUpdate, Order: &updates[i]})
		}

	case "userTwapHistory":
		var wrapper struct {
			IsSnapshot bool         `json:"isSnapshot"`
			History    []TwapUpdate `json:"history"`
		}
		if err := json.Unmarshal(env.Data, &wrapper); err != nil {
			return
		}
		if wrapper.IsSnapshot {
			return
		}
		for i := range wrapper.History {
			c.emit(Event{Address: c.Address, Kind: KindTwapUpdate, Twap: &wrapper.History[i]})
		}
	}
}

// dedup returns true if key is new (first time seen), false if duplicate.
func (c *Client) dedup(key string) bool {
	c.dedupMu.Lock()
	defer c.dedupMu.Unlock()
	if _, seen := c.seenOrders[key]; seen {
		return false
	}
	c.seenOrders[key] = struct{}{}
	if len(c.seenOrders) > 1024 {
		c.seenOrders = make(map[string]struct{}, 64)
	}
	return true
}

func (c *Client) emit(e Event) {
	select {
	case c.Events <- e:
	default:
	}
}

// ── REST ──────────────────────────────────────────────────────────────────────

var restClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     60 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		DisableCompression:  true,
	},
	Timeout: 8 * time.Second,
}

func post(ctx context.Context, payload interface{}) ([]byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, infoURL, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := restClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func FetchPositions(ctx context.Context, address string) ([]Position, MarginSummary, error) {
	data, err := post(ctx, map[string]string{"type": "clearinghouseState", "user": address})
	if err != nil {
		return nil, MarginSummary{}, err
	}
	var state ClearinghouseState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, MarginSummary{}, err
	}
	var positions []Position
	for _, ap := range state.AssetPositions {
		if ap.Position.Szi != "" && ap.Position.Szi != "0" {
			positions = append(positions, ap.Position)
		}
	}
	return positions, state.MarginSummary, nil
}

func FetchOpenOrders(ctx context.Context, address string) ([]OpenOrder, error) {
	data, err := post(ctx, map[string]string{"type": "openOrders", "user": address})
	if err != nil {
		return nil, err
	}
	var orders []OpenOrder
	if err := json.Unmarshal(data, &orders); err != nil {
		return nil, err
	}
	return orders, nil
}
