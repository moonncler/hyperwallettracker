package bot

import (
	"fmt"
	"strconv"
	"strings"

	"hyperwallettracker/hl"
)

func formatPositions(address string, positions []hl.Position, margin hl.MarginSummary) string {
	if len(positions) == 0 {
		return "📭 <b>Відкритих позицій немає</b>\n<code>" + shortAddr(address) + "</code>"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("<b>📊 Позиції</b> · <code>%s</code>\n", shortAddr(address)))
	sb.WriteString(fmt.Sprintf("💼 Equity: <b>%s USDC</b>\n\n", margin.AccountValue))

	for _, p := range positions {
		size, _ := strconv.ParseFloat(p.Szi, 64)
		side := "🟢 LONG"
		if size < 0 {
			side = "🔴 SHORT"
		}

		entry := "—"
		if p.EntryPx != nil {
			entry = *p.EntryPx
		}
		liqPx := "—"
		if p.LiquidationPx != nil {
			liqPx = *p.LiquidationPx
		}

		pnlSign := "+"
		upnl, _ := strconv.ParseFloat(p.UnrealizedPnl, 64)
		if upnl < 0 {
			pnlSign = ""
		}

		sb.WriteString(fmt.Sprintf(
			"<b>%s</b> · %s\n"+
				"  Size: %s · Entry: %s\n"+
				"  uPnL: %s%s USDC\n"+
				"  Lev: %dx · Liq: %s\n\n",
			p.Coin, side,
			p.Szi, entry,
			pnlSign, p.UnrealizedPnl,
			p.Leverage.Value, liqPx,
		))
	}
	return strings.TrimRight(sb.String(), "\n")
}

func formatOrders(address string, orders []hl.OpenOrder) string {
	if len(orders) == 0 {
		return "📭 <b>Відкритих ордерів немає</b>\n<code>" + shortAddr(address) + "</code>"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("<b>📋 Ордери (%d)</b> · <code>%s</code>\n\n", len(orders), shortAddr(address)))

	for _, o := range orders {
		side := "BUY"
		emoji := "🟢"
		if o.Side == "A" {
			side = "SELL"
			emoji = "🔴"
		}
		twap := ""
		if o.TwapID != nil {
			twap = " [TWAP]"
		}
		filled := ""
		origF, e1 := strconv.ParseFloat(o.OrigSz, 64)
		remF, e2 := strconv.ParseFloat(o.Sz, 64)
		if e1 == nil && e2 == nil && origF > 0 {
			pct := (origF - remF) / origF * 100
			filled = fmt.Sprintf(" (%.1f%% filled)", pct)
		}
		sb.WriteString(fmt.Sprintf(
			"%s <b>%s%s</b> · %s%s\n"+
				"  💲 %s · Sz: %s%s\n\n",
			emoji, o.Coin, twap, side, o.OrderType,
			o.LimitPx, o.Sz, filled,
		))
	}
	return strings.TrimRight(sb.String(), "\n")
}
