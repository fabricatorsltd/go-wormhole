package sql_test

import (
	stdctx "context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// cardPayment and wirePayment share the "payment" table, distinguished by the
// "kind" discriminator column.
type cardPayment struct {
	ID     int    `db:"column:id;primary_key;auto_increment;table:payment"`
	Kind   string `db:"column:kind;discriminator:card"`
	Amount int    `db:"column:amount"`
	Card   string `db:"column:card"`
}

type wirePayment struct {
	ID     int    `db:"column:id;primary_key;auto_increment;table:payment"`
	Kind   string `db:"column:kind;discriminator:wire"`
	Amount int    `db:"column:amount"`
	Bank   string `db:"column:bank"`
}

func openTPHDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "payment" (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT,
		"kind" TEXT NOT NULL,
		"amount" INTEGER NOT NULL,
		"card" TEXT,
		"bank" TEXT)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func kindCount(t *testing.T, db *sql.DB, kind string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "payment" WHERE "kind"=?`, kind).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The mapping owns the discriminator: an insert writes this type's value even
// when the struct field was left empty.
func TestE2E_TPH_InsertForcesDiscriminator(t *testing.T) {
	db := openTPHDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	ctx.Add(&cardPayment{Amount: 100, Card: "visa"}) // Kind deliberately unset
	ctx.Add(&wirePayment{Amount: 200, Bank: "acme"})
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if c := kindCount(t, db, "card"); c != 1 {
		t.Errorf("card rows: got %d, want 1", c)
	}
	if c := kindCount(t, db, "wire"); c != 1 {
		t.Errorf("wire rows: got %d, want 1", c)
	}
}

func seedTPH(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO "payment" ("kind","amount","card","bank") VALUES
		('card',10,'visa',NULL),('card',20,'amex',NULL),('wire',30,NULL,'acme')`); err != nil {
		t.Fatal(err)
	}
}

// Every subtype operation touches only its own rows: All, Find, Stream, bulk
// Delete, bulk Update, and a query with IgnoreFilters set (the discriminator is
// unbypassable). This is the isolation gate for the whole feature.
func TestE2E_TPH_CrossTypeIsolation(t *testing.T) {
	db := openTPHDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()
	seedTPH(t, db) // 2 card, 1 wire

	// All
	var cards []cardPayment
	if err := ctx.Set(&cards).All(); err != nil {
		t.Fatal(err)
	}
	if len(cards) != 2 {
		t.Errorf("All: got %d card rows, want 2", len(cards))
	}
	for _, c := range cards {
		if c.Kind != "card" {
			t.Errorf("All returned a non-card row: %+v", c)
		}
	}

	// IgnoreFilters must NOT drop the discriminator.
	var cards2 []cardPayment
	if err := ctx.Set(&cards2).IgnoreFilters().All(); err != nil {
		t.Fatal(err)
	}
	if len(cards2) != 2 {
		t.Errorf("IgnoreFilters All: got %d, want 2 (discriminator must hold)", len(cards2))
	}

	// Find: the wire row's id (3) must not be findable as a cardPayment.
	var c cardPayment
	if err := ctx.Set(&c).Find(3); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("Find(wire id) as card: want ErrNoRows, got %v", err)
	}

	// Stream
	streamed := 0
	for v, err := range ctx.Set(&cardPayment{}).Stream(stdctx.Background()) {
		if err != nil {
			t.Fatal(err)
		}
		if v.(*cardPayment).Kind != "card" {
			t.Errorf("Stream returned a non-card row: %+v", v)
		}
		streamed++
	}
	if streamed != 2 {
		t.Errorf("Stream: got %d, want 2", streamed)
	}

	// Bulk update: only card rows.
	cp := &cardPayment{}
	n, err := ctx.Set(&cardPayment{}).Where(dsl.Gt(cp, &cp.Amount, 0)).Update(dsl.Set(cp, &cp.Amount, 999))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("bulk update affected %d, want 2 (card only)", n)
	}
	if got, _ := amountOf(t, db, 3); got != 30 {
		t.Errorf("wire row amount changed by card update: got %d, want 30", got)
	}

	// Bulk delete: only card rows.
	if _, err := ctx.Set(&cardPayment{}).Delete(); err != nil {
		t.Fatal(err)
	}
	if c := kindCount(t, db, "card"); c != 0 {
		t.Errorf("card rows after delete: got %d, want 0", c)
	}
	if c := kindCount(t, db, "wire"); c != 1 {
		t.Errorf("wire rows after card delete: got %d, want 1 (untouched)", c)
	}
}

func amountOf(t *testing.T, db *sql.DB, id int) (int, bool) {
	t.Helper()
	var a int
	err := db.QueryRow(`SELECT "amount" FROM "payment" WHERE "id"=?`, id).Scan(&a)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return a, true
}

// A stub entity carrying a sibling type's primary key must not delete or
// overwrite that sibling's row: the write-by-PK paths are discriminator-scoped.
func TestE2E_TPH_StubWriteIsolation(t *testing.T) {
	db := openTPHDB(t)
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO "payment" ("id","kind","amount") VALUES (1,'card',10),(2,'wire',20)`); err != nil {
		t.Fatal(err)
	}

	// Remove a cardPayment stub holding the wire row's id: must not delete it.
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()
	ctx.Remove(&cardPayment{ID: 2})
	if err := ctx.Save(); err != nil {
		t.Fatalf("save remove: %v", err)
	}
	if c := kindCount(t, db, "wire"); c != 1 {
		t.Errorf("stub Remove deleted the sibling wire row: wire count %d, want 1", c)
	}

	// Attach a cardPayment stub on the wire id and mutate: must not overwrite it.
	ctx2 := wctx.New(wsql.New(db))
	defer ctx2.Close()
	stub := &cardPayment{ID: 2}
	ctx2.Attach(stub)
	stub.Amount = 9999
	if err := ctx2.Save(); err != nil {
		t.Fatalf("save update: %v", err)
	}
	if got, _ := amountOf(t, db, 2); got != 20 {
		t.Errorf("stub Update overwrote the sibling wire row: amount %d, want 20", got)
	}
}

// Sibling types share a table name, so neither may evict the other from the
// by-name cache (which would corrupt aggregate field resolution).
func TestTPH_NoNameCacheCollision(t *testing.T) {
	schema.Parse(&cardPayment{})
	schema.Parse(&wirePayment{})
	if m := schema.LookupEntity("payment"); m != nil {
		t.Errorf("discriminated types must not populate the by-name cache, got %q", m.Name)
	}
}

// The parser resolves the table override and discriminator metadata.
func TestTPH_ParserMetadata(t *testing.T) {
	meta := schema.Parse(&cardPayment{})
	if meta.Name != "payment" {
		t.Errorf("table override: got %q, want payment", meta.Name)
	}
	if meta.Discriminator == nil || meta.Discriminator.Column != "kind" {
		t.Fatalf("discriminator column not resolved: %+v", meta.Discriminator)
	}
	if meta.DiscriminatorValue != "card" {
		t.Errorf("discriminator value: got %q, want card", meta.DiscriminatorValue)
	}
}

func init() {
	dsl.Register(cardPayment{})
	dsl.Register(wirePayment{})
}
