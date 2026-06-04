package sql_test

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type orderLine struct {
	OrderID int `db:"column:order_id;primary_key"`
	LineNo  int `db:"column:line_no;primary_key"`
	Qty     int `db:"column:qty"`
}

func openCompositeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "order_line" (
		"order_id" INTEGER NOT NULL,
		"line_no" INTEGER NOT NULL,
		"qty" INTEGER NOT NULL,
		PRIMARY KEY ("order_id","line_no"))`); err != nil {
		t.Fatal(err)
	}
	return db
}

func qtyOf(t *testing.T, db *sql.DB, orderID, lineNo int) (int, bool) {
	t.Helper()
	var q int
	err := db.QueryRow(`SELECT "qty" FROM "order_line" WHERE "order_id"=? AND "line_no"=?`, orderID, lineNo).Scan(&q)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return q, true
}

// Full CRUD round trip for a two-column primary key: insert writes both key
// columns, Find(k1,k2) returns the row, a mutation updates exactly that row,
// and Delete removes exactly that row.
func TestE2E_CompositeKey_RoundTrip(t *testing.T) {
	db := openCompositeDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	ctx.Add(&orderLine{OrderID: 1, LineNo: 1, Qty: 10})
	ctx.Add(&orderLine{OrderID: 1, LineNo: 2, Qty: 20})
	if err := ctx.Save(); err != nil {
		t.Fatalf("save inserts: %v", err)
	}

	// Find by the full composite key.
	read := wctx.New(wsql.New(db))
	defer read.Close()
	var l orderLine
	if err := read.Set(&l).Find(1, 2); err != nil {
		t.Fatalf("find (1,2): %v", err)
	}
	if l.Qty != 20 {
		t.Fatalf("find (1,2) qty: got %d, want 20", l.Qty)
	}

	// Mutate and save: only (1,2) changes.
	l.Qty = 99
	if err := read.Save(); err != nil {
		t.Fatalf("save update: %v", err)
	}
	if q, _ := qtyOf(t, db, 1, 2); q != 99 {
		t.Errorf("(1,2) after update: got %d, want 99", q)
	}
	if q, _ := qtyOf(t, db, 1, 1); q != 10 {
		t.Errorf("(1,1) should be untouched: got %d, want 10", q)
	}

	// Delete removes exactly (1,2).
	read.Remove(&l)
	if err := read.Save(); err != nil {
		t.Fatalf("save delete: %v", err)
	}
	if _, ok := qtyOf(t, db, 1, 2); ok {
		t.Errorf("(1,2) should be deleted")
	}
	if _, ok := qtyOf(t, db, 1, 1); !ok {
		t.Errorf("(1,1) should remain")
	}
}

// Two rows sharing the first key column but differing on the second must be
// tracked as distinct entries. If entityKey used only the first column they
// would collide, and saving both mutations would lose one.
func TestE2E_CompositeKey_IdentityDistinct(t *testing.T) {
	db := openCompositeDB(t)
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO "order_line" ("order_id","line_no","qty") VALUES (1,1,10),(1,2,20)`); err != nil {
		t.Fatal(err)
	}

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()
	var a, b orderLine
	if err := ctx.Set(&a).Find(1, 1); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Set(&b).Find(1, 2); err != nil {
		t.Fatal(err)
	}
	a.Qty = 111
	b.Qty = 222
	if err := ctx.Save(); err != nil {
		t.Fatalf("save both: %v", err)
	}
	if q, _ := qtyOf(t, db, 1, 1); q != 111 {
		t.Errorf("(1,1): got %d, want 111 (mutation lost = identity collision)", q)
	}
	if q, _ := qtyOf(t, db, 1, 2); q != 222 {
		t.Errorf("(1,2): got %d, want 222", q)
	}
}

// Find with the wrong number of key values is rejected before hitting the DB.
func TestE2E_CompositeKey_FindArgCount(t *testing.T) {
	db := openCompositeDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var l orderLine
	if err := ctx.Set(&l).Find(1); err == nil {
		t.Error("Find with one value for a two-column key should error")
	}
}

// AsTracking + Select on a composite-key entity must auto-add every key column,
// so a mutation's UPDATE targets the full key. With only the first key column
// added, line_no would be zero and the update would match nothing.
func TestE2E_CompositeKey_AsTrackingSelectKeepsKeys(t *testing.T) {
	db := openCompositeDB(t)
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO "order_line" ("order_id","line_no","qty") VALUES (1,1,10),(1,2,20)`); err != nil {
		t.Fatal(err)
	}
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var rows []*orderLine
	if err := ctx.Set(&rows).AsTracking().Select("qty").OrderBy("line_no", 0).All(); err != nil {
		t.Fatal(err)
	}
	rows[0].Qty = 500 // line_no 1
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if q, _ := qtyOf(t, db, 1, 1); q != 500 {
		t.Errorf("(1,1) after AsTracking+Select update: got %d, want 500", q)
	}
	if q, _ := qtyOf(t, db, 1, 2); q != 20 {
		t.Errorf("(1,2) should be untouched: got %d, want 20", q)
	}
}

type compParent struct {
	K1 int `db:"column:k1;primary_key"`
	K2 int `db:"column:k2;primary_key"`
}

type compChild struct {
	ID     int         `db:"column:id;primary_key;auto_increment"`
	Parent *compParent `db:"ref"` // relation to a composite-PK target
}

// A relation targeting a composite-PK entity is rejected at Save, rather than
// silently writing only the first key column into the foreign key.
func TestE2E_CompositeRelation_Rejected(t *testing.T) {
	db := openCompositeDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	ctx.Add(&compChild{ID: 1, Parent: &compParent{K1: 1, K2: 2}})
	err := ctx.Save()
	if err == nil || !errContains(err, "composite primary key") {
		t.Fatalf("want relation-to-composite rejection, got %v", err)
	}
}

func errContains(err error, sub string) bool {
	return err != nil && strings.Contains(err.Error(), sub)
}

func init() {
	schema.Parse(&orderLine{})
	schema.Parse(&compParent{})
	schema.Parse(&compChild{})
}
