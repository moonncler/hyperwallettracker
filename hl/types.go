package hl

// ── WebSocket envelope ──────────────────────────────────────────────────────

type WSMessage struct {
	Channel string          `json:"channel"`
	Data    RawOrSubscribed `json:"data"`
}

// RawOrSubscribed is decoded per-channel after the envelope is parsed.
type RawOrSubscribed = interface{}

// ── userFills ───────────────────────────────────────────────────────────────

type UserFillsData struct {
	IsSnapshot bool   `json:"isSnapshot"`
	User       string `json:"user"`
	Fills      []Fill `json:"fills"`
}

type Fill struct {
	Coin        string  `json:"coin"`
	Px          string  `json:"px"`   // price
	Sz          string  `json:"sz"`   // size
	Side        string  `json:"side"` // "B" buy / "A" ask(sell)
	Time        int64   `json:"time"`
	StartPos    string  `json:"startPosition"`
	Dir         string  `json:"dir"`   // "Open Long", "Close Short", etc.
	ClosedPnl   string  `json:"closedPnl"`
	Hash        string  `json:"hash"`
	Oid         int64   `json:"oid"`
	Crossed     bool    `json:"crossed"`
	Fee         string  `json:"fee"`
	FeeToken    string  `json:"feeToken"`
	TwapID      *int64  `json:"twapId"`
	BuilderFee  *string `json:"builderFee"`
}

// ── userEvents ──────────────────────────────────────────────────────────────

type UserEventsData struct {
	Fills         []Fill        `json:"fills"`
	Funding       []FundingEvt  `json:"funding"`
	Liquidation   *Liquidation  `json:"liquidation"`
	NonUserCancel []OrderCancel `json:"nonUserCancel"`
}

type FundingEvt struct {
	Time        int64  `json:"time"`
	Coin        string `json:"coin"`
	Usdc        string `json:"usdc"`
	Szi         string `json:"szi"`
	FundingRate string `json:"fundingRate"`
}

type Liquidation struct {
	Lid           int64  `json:"lid"`
	Liquidator    string `json:"liquidator"`
	LiquidatedNtl string `json:"liquidatedNtl"`
	LiquidatedAcc string `json:"liquidatedAcc"`
	LiquidatedFee string `json:"liquidatedFee"`
}

type OrderCancel struct {
	Coin string `json:"coin"`
	Oid  int64  `json:"oid"`
}

// ── orderUpdates ────────────────────────────────────────────────────────────

type OrderUpdate struct {
	Order     OrderInfo `json:"order"`
	Status    string    `json:"status"` // "open","filled","canceled","triggered","rejected","marginCanceled"
	StatusMsg string    `json:"statusMsg"`
}

type OrderInfo struct {
	Coin      string `json:"coin"`
	Side      string `json:"side"`
	LimitPx   string `json:"limitPx"`
	Sz        string `json:"sz"`
	Oid       int64  `json:"oid"`
	Timestamp int64  `json:"timestamp"`
	OrigSz    string `json:"origSz"`
	OrderType string `json:"orderType"` // "Limit","Market","Stop Market","TWAP", etc.
	TwapID    *int64 `json:"twapId"`
}

// ── REST responses ───────────────────────────────────────────────────────────

type ClearinghouseState struct {
	AssetPositions []AssetPosition `json:"assetPositions"`
	MarginSummary  MarginSummary   `json:"marginSummary"`
}

type AssetPosition struct {
	Position Position `json:"position"`
	Type     string   `json:"type"`
}

type Position struct {
	Coin           string  `json:"coin"`
	Szi            string  `json:"szi"`
	EntryPx        *string `json:"entryPx"`
	PositionValue  string  `json:"positionValue"`
	UnrealizedPnl  string  `json:"unrealizedPnl"`
	ReturnOnEquity string  `json:"returnOnEquity"`
	Leverage       Lev     `json:"leverage"`
	LiquidationPx  *string `json:"liquidationPx"`
	MarginUsed     string  `json:"marginUsed"`
	MaxLeverage    int     `json:"maxLeverage"`
}

type Lev struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

type MarginSummary struct {
	AccountValue    string `json:"accountValue"`
	TotalMarginUsed string `json:"totalMarginUsed"`
	TotalNtlPos     string `json:"totalNtlPos"`
	TotalRawUsd     string `json:"totalRawUsd"`
}

type OpenOrder struct {
	Coin      string `json:"coin"`
	Side      string `json:"side"`
	LimitPx   string `json:"limitPx"`
	Sz        string `json:"sz"`
	Oid       int64  `json:"oid"`
	Timestamp int64  `json:"timestamp"`
	OrigSz    string `json:"origSz"`
	OrderType string `json:"orderType"`
	TwapID    *int64 `json:"twapId"`
}

// ── Event kinds emitted to tracker ──────────────────────────────────────────

type EventKind string

const (
	KindFill          EventKind = "fill"
	KindFunding       EventKind = "funding"
	KindLiquidation   EventKind = "liquidation"
	KindNonUserCancel EventKind = "non_user_cancel"
	KindOrderUpdate   EventKind = "order_update"
)

type Event struct {
	Address string
	Kind    EventKind
	Fill    *Fill
	Funding *FundingEvt
	Liq     *Liquidation
	Cancel  *OrderCancel
	Order   *OrderUpdate
}
