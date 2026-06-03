package sql_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// Msg maps to table "msg" (snake_case of the struct name) with a
// client-set TEXT primary key — the shape kiara's message/tree/proof
// upserts use.
type Msg struct {
	ID     string `db:"column:id; primary_key"`
	Hash   string `db:"column:hash"`
	Status string `db:"column:status"`
}

func openMsgDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1) // :memory: is per-connection
	if _, err := db.Exec(`CREATE TABLE "msg" ("id" TEXT PRIMARY KEY, "hash" TEXT, "status" TEXT)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestE2E_Upsert_DoUpdate(t *testing.T) {
	db := openMsgDB(t)
	defer db.Close()
	c := wctx.New(wsql.New(db))
	ctx := context.Background()
	cc := provider.ConflictClause{Columns: []string{"id"}, Update: []string{"hash", "status"}}

	if err := c.Upsert(ctx, &Msg{ID: "m1", Hash: "h1", Status: "pending"}, cc); err != nil {
		t.Fatal(err)
	}
	if err := c.Upsert(ctx, &Msg{ID: "m1", Hash: "h2", Status: "confirmed"}, cc); err != nil {
		t.Fatal(err)
	}

	var hash, status string
	if err := db.QueryRow(`SELECT hash, status FROM msg WHERE id='m1'`).Scan(&hash, &status); err != nil {
		t.Fatal(err)
	}
	if hash != "h2" || status != "confirmed" {
		t.Fatalf("upsert-update: hash=%q status=%q, want h2/confirmed", hash, status)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM msg`).Scan(&n)
	if n != 1 {
		t.Fatalf("upsert duplicated rows: got %d, want 1", n)
	}
}

func TestE2E_Upsert_DoNothing(t *testing.T) {
	db := openMsgDB(t)
	defer db.Close()
	c := wctx.New(wsql.New(db))
	ctx := context.Background()
	cc := provider.ConflictClause{Columns: []string{"id"}} // empty Update => DO NOTHING

	_ = c.Upsert(ctx, &Msg{ID: "m1", Hash: "h1", Status: "pending"}, cc)
	if err := c.Upsert(ctx, &Msg{ID: "m1", Hash: "h2", Status: "confirmed"}, cc); err != nil {
		t.Fatal(err)
	}

	var hash string
	_ = db.QueryRow(`SELECT hash FROM msg WHERE id='m1'`).Scan(&hash)
	if hash != "h1" {
		t.Fatalf("DO NOTHING should keep original: got %q, want h1", hash)
	}
}

func TestE2E_Transaction_RawCommit(t *testing.T) {
	db := openMsgDB(t)
	defer db.Close()
	c := wctx.New(wsql.New(db))
	ctx := context.Background()

	err := c.Transaction(ctx, func(tx provider.Tx) error {
		raw, ok := tx.(wsql.TxRunner)
		if !ok {
			t.Fatal("a SQL provider.Tx must satisfy wsql.TxRunner")
		}
		_, err := raw.ExecContext(ctx, `INSERT INTO msg (id, hash, status) VALUES (?, ?, ?)`, "tx1", "hh", "ok")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var n int
	_ = db.QueryRow(`SELECT count(*) FROM msg WHERE id='tx1'`).Scan(&n)
	if n != 1 {
		t.Fatalf("committed row missing: got %d", n)
	}
}

func TestE2E_Transaction_Rollback(t *testing.T) {
	db := openMsgDB(t)
	defer db.Close()
	c := wctx.New(wsql.New(db))
	ctx := context.Background()
	boom := errors.New("boom")

	err := c.Transaction(ctx, func(tx provider.Tx) error {
		raw := tx.(wsql.TxRunner)
		if _, err := raw.ExecContext(ctx, `INSERT INTO msg (id, hash, status) VALUES (?, ?, ?)`, "tx2", "hh", "ok"); err != nil {
			return err
		}
		return boom // forces rollback
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}

	var n int
	_ = db.QueryRow(`SELECT count(*) FROM msg WHERE id='tx2'`).Scan(&n)
	if n != 0 {
		t.Fatalf("rolled-back row must not exist: got %d", n)
	}
}
