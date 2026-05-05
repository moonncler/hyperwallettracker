package bot

import (
	"context"
	"fmt"
	"strings"

	"hyperwallettracker/hl"
)

const helpText = `<b>Hyperliquid Wallet Tracker</b>

/add &lt;address&gt; [label] — додати гаманець
/remove &lt;address&gt; — видалити гаманець
/list — список відстежуваних гаманців
/positions &lt;address&gt; — відкриті позиції
/orders &lt;address&gt; — відкриті ордери
/help — ця довідка

Бот надсилає сповіщення про:
• Fills (угоди), в т.ч. TWAP
• Відкриття / закриття позицій
• Ліквідації
• Зміни ордерів
• Funding payments`

func (b *Bot) cmdHelp(chatID int64) {
	b.reply(chatID, helpText)
}

func (b *Bot) cmdAdd(ctx context.Context, chatID int64, args []string) {
	if len(args) == 0 {
		b.reply(chatID, "⚠️ Вкажи адресу: /add 0x...")
		return
	}
	address := strings.ToLower(args[0])
	if !isValidAddress(address) {
		b.reply(chatID, "⚠️ Невалідна адреса (має починатись з 0x, 42 символи).")
		return
	}
	label := ""
	if len(args) > 1 {
		label = strings.Join(args[1:], " ")
	}

	exists, err := b.db.WalletExists(ctx, address, chatID)
	if err != nil {
		b.reply(chatID, "❌ DB error: "+err.Error())
		return
	}
	if exists {
		b.reply(chatID, "ℹ️ Цей гаманець вже відстежується.")
		return
	}

	if err := b.manager.AddWallet(ctx, address, label, chatID); err != nil {
		b.reply(chatID, "❌ Помилка: "+err.Error())
		return
	}

	name := address
	if label != "" {
		name = fmt.Sprintf("%s (%s)", label, shortAddr(address))
	}
	b.reply(chatID, "✅ Гаманець додано: <code>"+name+"</code>\nСповіщення увімкнено!")
}

func (b *Bot) cmdRemove(ctx context.Context, chatID int64, args []string) {
	if len(args) == 0 {
		b.reply(chatID, "⚠️ Вкажи адресу: /remove 0x...")
		return
	}
	address := strings.ToLower(args[0])
	if err := b.manager.RemoveWallet(ctx, address, chatID); err != nil {
		b.reply(chatID, "❌ Помилка: "+err.Error())
		return
	}
	b.reply(chatID, "🗑 Гаманець видалено: <code>"+shortAddr(address)+"</code>")
}

func (b *Bot) cmdList(ctx context.Context, chatID int64) {
	wallets, err := b.db.ListWalletsByChat(ctx, chatID)
	if err != nil {
		b.reply(chatID, "❌ DB error: "+err.Error())
		return
	}
	if len(wallets) == 0 {
		b.reply(chatID, "📭 Немає відстежуваних гаманців.\n/add &lt;address&gt; — додати.")
		return
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("<b>Відстежувані гаманці (%d):</b>\n\n", len(wallets)))
	for i, w := range wallets {
		label := w.Label
		if label == "" {
			label = "без назви"
		}
		sb.WriteString(fmt.Sprintf("%d. <code>%s</code>\n   📝 %s\n", i+1, w.Address, label))
	}
	b.reply(chatID, sb.String())
}

func (b *Bot) cmdPositions(ctx context.Context, chatID int64, args []string) {
	if len(args) == 0 {
		b.reply(chatID, "⚠️ Вкажи адресу: /positions 0x...")
		return
	}
	address := strings.ToLower(args[0])
	b.reply(chatID, "⏳ Завантажую позиції...")

	positions, margin, err := hl.FetchPositions(ctx, address)
	if err != nil {
		b.reply(chatID, "❌ Помилка: "+err.Error())
		return
	}
	b.reply(chatID, formatPositions(address, positions, margin))
}

func (b *Bot) cmdOrders(ctx context.Context, chatID int64, args []string) {
	if len(args) == 0 {
		b.reply(chatID, "⚠️ Вкажи адресу: /orders 0x...")
		return
	}
	address := strings.ToLower(args[0])
	b.reply(chatID, "⏳ Завантажую ордери...")

	orders, err := hl.FetchOpenOrders(ctx, address)
	if err != nil {
		b.reply(chatID, "❌ Помилка: "+err.Error())
		return
	}
	b.reply(chatID, formatOrders(address, orders))
}

func isValidAddress(addr string) bool {
	if len(addr) != 42 {
		return false
	}
	if addr[:2] != "0x" {
		return false
	}
	for _, c := range addr[2:] {
		if !isHex(c) {
			return false
		}
	}
	return true
}

func isHex(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func shortAddr(addr string) string {
	if len(addr) <= 10 {
		return addr
	}
	return addr[:6] + "…" + addr[len(addr)-4:]
}
