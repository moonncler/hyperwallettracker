package db

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	sql *sql.DB
}

func Open(path string) (*DB, error) {
	conn, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(1)
	d := &DB{sql: conn}
	if err := d.migrate(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DB) Close() error { return d.sql.Close() }

func (d *DB) migrate() error {
	_, err := d.sql.Exec(`
	CREATE TABLE IF NOT EXISTS wallets (
		address     TEXT PRIMARY KEY,
		label       TEXT NOT NULL DEFAULT '',
		chat_id     INTEGER NOT NULL,
		added_at    INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS events (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		address     TEXT NOT NULL,
		kind        TEXT NOT NULL,
		payload     TEXT NOT NULL,
		created_at  INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS events_address ON events(address);
	CREATE INDEX IF NOT EXISTS events_created ON events(created_at);
	`)
	return err
}

// ── Wallet ────────────────────────────────────────────────────────────────────

type Wallet struct {
	Address string
	Label   string
	ChatID  int64
	AddedAt time.Time
}

func (d *DB) AddWallet(ctx context.Context, address, label string, chatID int64) error {
	_, err := d.sql.ExecContext(ctx,
		`INSERT OR REPLACE INTO wallets(address, label, chat_id, added_at) VALUES(?,?,?,?)`,
		address, label, chatID, time.Now().Unix(),
	)
	return err
}

func (d *DB) RemoveWallet(ctx context.Context, address string, chatID int64) error {
	_, err := d.sql.ExecContext(ctx,
		`DELETE FROM wallets WHERE address=? AND chat_id=?`,
		address, chatID,
	)
	return err
}

func (d *DB) ListWallets(ctx context.Context) ([]Wallet, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT address, label, chat_id, added_at FROM wallets`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Wallet
	for rows.Next() {
		var w Wallet
		var ts int64
		if err := rows.Scan(&w.Address, &w.Label, &w.ChatID, &ts); err != nil {
			return nil, err
		}
		w.AddedAt = time.Unix(ts, 0)
		out = append(out, w)
	}
	return out, rows.Err()
}

func (d *DB) ListWalletsByChat(ctx context.Context, chatID int64) ([]Wallet, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT address, label, chat_id, added_at FROM wallets WHERE chat_id=?`, chatID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Wallet
	for rows.Next() {
		var w Wallet
		var ts int64
		if err := rows.Scan(&w.Address, &w.Label, &w.ChatID, &ts); err != nil {
			return nil, err
		}
		w.AddedAt = time.Unix(ts, 0)
		out = append(out, w)
	}
	return out, rows.Err()
}

func (d *DB) WalletExists(ctx context.Context, address string, chatID int64) (bool, error) {
	var n int
	err := d.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM wallets WHERE address=? AND chat_id=?`,
		address, chatID,
	).Scan(&n)
	return n > 0, err
}

// GetChatsForWallet returns all chat IDs subscribed to this address.
func (d *DB) GetChatsForWallet(ctx context.Context, address string) ([]int64, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT chat_id FROM wallets WHERE address=?`, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ── Events ────────────────────────────────────────────────────────────────────

func (d *DB) SaveEvent(ctx context.Context, address, kind, payload string) error {
	_, err := d.sql.ExecContext(ctx,
		`INSERT INTO events(address, kind, payload, created_at) VALUES(?,?,?,?)`,
		address, kind, payload, time.Now().Unix(),
	)
	return err
}
