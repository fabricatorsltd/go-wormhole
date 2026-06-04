// Command unitofwork shows the change-tracker controls around reads and writes:
// global query filters (multi-tenant / soft-delete), tracked vs no-tracking
// reads, streaming a result set, and an explicit transaction.
//
//	go run ./examples/unitofwork
package main

import (
	stdctx "context"
	"errors"
	"fmt"
	"log"

	"database/sql"

	_ "github.com/glebarez/sqlite"

	wh "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type Doc struct {
	ID       int    `db:"column:id;primary_key"`
	TenantID string `db:"column:tenant_id"`
	Title    string `db:"column:title"`
	Deleted  bool   `db:"column:deleted"`
}

func init() {
	dsl.Register(Doc{})
}

func main() {
	// Each demo uses its own in-memory database. Closing a context closes the
	// underlying database, so the demos do not share one.
	queryFilters()
	tracking()
	streaming()
	transactions()
}

// queryFilters scope every query on the context: here, tenant A and not soft-
// deleted. The filters apply to All/Find/Stream unless IgnoreFilters opts out.
func queryFilters() {
	db := open()
	ctx := wh.New(wsql.New(db))
	defer ctx.Close()

	d := &Doc{}
	ctx.AddQueryFilter(&Doc{}, dsl.Eq(d, &d.TenantID, "A"))
	ctx.AddQueryFilter(&Doc{}, dsl.Eq(d, &d.Deleted, false))

	var visible []Doc
	must(ctx.Set(&visible).All())
	fmt.Printf("tenant A, live docs: %d\n", len(visible)) // excludes tenant B and the deleted row

	var all []Doc
	must(ctx.Set(&all).IgnoreFilters().All())
	fmt.Printf("all docs (filters bypassed): %d\n", len(all))
}

// tracking contrasts a no-tracking read (a detached snapshot Save ignores) with
// a tracked read (Save detects the change and persists it).
func tracking() {
	db := open()
	ctx := wh.New(wsql.New(db))
	defer ctx.Close()

	var detached Doc
	must(ctx.Set(&detached).NoTracking().Find(1))
	detached.Title = "ignored"
	must(ctx.Save()) // no UPDATE: the entity was never tracked

	var tracked Doc
	must(ctx.Set(&tracked).Find(1))
	tracked.Title = "persisted"
	must(ctx.Save()) // UPDATE doc SET title = ? WHERE id = ?

	var check Doc
	must(ctx.Set(&check).Find(1))
	fmt.Printf("title after no-tracking + tracked edits: %q\n", check.Title)
}

// streaming reads rows one at a time through a range-over-func iterator, so a
// large result never has to be materialized into a slice.
func streaming() {
	db := open()
	ctx := wh.New(wsql.New(db))
	defer ctx.Close()

	count := 0
	for row, err := range wh.Stream[Doc](stdctx.Background(), ctx.Set(&Doc{}).OrderBy("id", query.Asc)) {
		must(err)
		_ = row
		count++
	}
	fmt.Printf("streamed %d rows\n", count)
}

// transactions run a unit of work that must fully commit or fully roll back.
// Returning an error from the closure rolls everything back.
func transactions() {
	db := open()
	ctx := wh.New(wsql.New(db))
	defer ctx.Close()
	meta := schema.Parse(&Doc{})

	err := ctx.Transaction(stdctx.Background(), func(tx provider.Tx) error {
		if _, err := tx.Insert(stdctx.Background(), meta, &Doc{ID: 100, TenantID: "A", Title: "first"}); err != nil {
			return err
		}
		// Something goes wrong: the first insert is rolled back with this one.
		return errors.New("simulated failure")
	})
	fmt.Printf("transaction rolled back: %v\n", err != nil)

	d := &Doc{}
	var rows []Doc
	must(ctx.Set(&rows).IgnoreFilters().Where(dsl.Eq(d, &d.ID, 100)).All())
	fmt.Printf("rows written by the failed transaction: %d\n", len(rows))
}

// open returns a fresh, seeded in-memory database.
func open() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	must(err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE "doc" (
		"id"        INTEGER PRIMARY KEY,
		"tenant_id" TEXT NOT NULL,
		"title"     TEXT NOT NULL,
		"deleted"   INTEGER NOT NULL DEFAULT 0)`)
	must(err)
	_, err = db.Exec(`INSERT INTO "doc" ("id","tenant_id","title","deleted") VALUES
		(1,'A','alpha',0),
		(2,'A','beta',0),
		(3,'A','gamma',1),
		(4,'B','delta',0)`)
	must(err)
	return db
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
