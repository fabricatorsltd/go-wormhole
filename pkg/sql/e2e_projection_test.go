package sql_test

import (
	"database/sql"
	"sort"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type pRow struct {
	ID   int    `db:"column:id;primary_key;auto_increment"`
	City string `db:"column:city"`
	Name string `db:"column:name"`
}

func openProjectionDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "p_row" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "city" TEXT NOT NULL, "name" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	// Two cities, four people: duplicate city values for DISTINCT to collapse.
	if _, err := db.Exec(`INSERT INTO "p_row" ("city","name") VALUES ('NYC','a'),('NYC','b'),('LA','c'),('LA','d')`); err != nil {
		t.Fatal(err)
	}
	return db
}

// Distinct collapses duplicate projected rows.
func TestE2E_Distinct_Projection(t *testing.T) {
	db := openProjectionDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var rows []pRow
	if err := ctx.Set(&rows).Distinct().Select("city").OrderBy("city", query.Asc).All(); err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(rows))
	for i, r := range rows {
		got[i] = r.City
		// Unselected columns stay zero.
		if r.Name != "" || r.ID != 0 {
			t.Errorf("projection leaked unselected columns: %+v", r)
		}
	}
	want := []string{"LA", "NYC"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("distinct cities: got %v, want %v", got, want)
	}
}

// Select without Distinct projects a subset of columns for every row, leaving
// unselected fields at their zero value.
func TestE2E_Select_SubsetColumns(t *testing.T) {
	db := openProjectionDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var rows []pRow
	if err := ctx.Set(&rows).Select("name").OrderBy("name", query.Asc).All(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("row count: got %d, want 4", len(rows))
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.Name
		if r.City != "" || r.ID != 0 {
			t.Errorf("only name should be populated, got %+v", r)
		}
	}
	sort.Strings(names)
	if names[0] != "a" || names[3] != "d" {
		t.Errorf("projected names: got %v", names)
	}
}

// Distinct without a projection dedupes whole rows; with all columns distinct
// here, every row survives.
func TestE2E_Distinct_FullRow(t *testing.T) {
	db := openProjectionDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var rows []pRow
	if err := ctx.Set(&rows).Distinct().All(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Errorf("distinct full rows: got %d, want 4 (all unique by id)", len(rows))
	}
}

// Projection flows through to a DTO destination via From.
func TestE2E_Select_IntoDTO(t *testing.T) {
	db := openProjectionDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	type cityOnly struct {
		City string `db:"column:city"`
	}
	var rows []cityOnly
	if err := ctx.Set(&rows).From("p_row").Distinct().Select("city").OrderBy("city", query.Asc).All(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].City != "LA" || rows[1].City != "NYC" {
		t.Errorf("DTO projection: got %+v", rows)
	}
}

// A projection that omits the key must not break Include: the key is auto-added
// so eager loading can still stitch children. Without that, projecting "name"
// leaves the PK zero and the relation query matches nothing.
func TestE2E_Select_WithIncludeKeepsKey(t *testing.T) {
	db := openIncludeDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var users []*incUser
	if err := ctx.Set(&users).Select("name").Include("Orders").OrderBy("id", query.Asc).All(); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, u := range users {
		total += len(u.Orders)
	}
	if total != 3 {
		t.Errorf("Select+Include should still load relations: got %d orders, want 3", total)
	}
}

// A projection that omits the PK must not break AsTracking: the PK is auto-added
// so the tracked entity can be keyed and a mutation targets the right row.
func TestE2E_Select_AsTrackingKeepsPK(t *testing.T) {
	db := openProjectionDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var rows []*pRow
	if err := ctx.Set(&rows).AsTracking().Select("name").OrderBy("id", query.Asc).All(); err != nil {
		t.Fatal(err)
	}
	rows[0].Name = "renamed"
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "p_row" WHERE "name"='renamed'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("AsTracking+Select mutation should target the right row: got %d updated, want 1", n)
	}
}

func init() {
	schema.Parse(&pRow{})
}
