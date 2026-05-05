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
	Address string
	Events  chan Event
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	conn    *websocket.Conn
}

func NewClient(ctx context.Context, address string) *Client {
	ctx, cancel := context.WithCancel(ctx)
	return &Client{
		Address: strings.ToLower(address),
		Events:  make(chan Event, 512),
		ctx:     ctx,
		cancel:  cancel,
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
		if err != nil && c.ctx.Err() == nil {
			log.Printf("[hl] %s reconnect in %s: %v", c.Address, backoff, err)
			select {
			case <-time.After(backoff):
			case <-c.ctx.Done():
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		} else {
			backoff = time.Second
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
	conn, _, err := dialer.DialContext(c.ctx, wsURL, nil)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		conn.Close()
	}()

	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Subscribe:
	// - userEvents  → fills, funding, liquidation, nonUserCancel
	// - orderUpdates → order lifecycle
	// - userTwapHistory → twap start/stop
	// NOTE: userFills is intentionally NOT subscribed — userEvents already
	// contains fills. Subscribing both causes duplicate notifications.
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
			return fmt.Errorf("subscribe: %w", err)
		}
	}

	// keepalive ping
	go func() {
		tick := time.NewTicker(20 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-tick.C:
				c.mu.Lock()
				ok := c.conn == conn
				c.mu.Unlock()
				if ok {
					_ = conn.WriteMessage(websocket.PingMessage, nil)
				}
			}
		}
	}()

	// raw message channel — decouple read from parse
	rawCh := make(chan []byte, 128)
	go func() {
		defer close(rawCh)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			select {
			case rawCh <- raw:
			default:
				// parser falling behind — drop rather than block reader
			}
		}
	}()

	// parse loop runs in this goroutine
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case raw, ok := <-rawCh:
			if !ok {
				if c.ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("connection closed")
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
		for i := range d.Fills {
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

	case "orderUpdates":
		var updates []OrderUpdate
		if err := json.Unmarshal(env.Data, &updates); err != nil {
			return
		}
		for i := range updates {
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
