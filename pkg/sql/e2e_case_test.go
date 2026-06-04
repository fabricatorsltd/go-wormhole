package sql_test

import (
	"database/sql"
	"sort"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type caseUser struct {
	ID   int    `db:"column:id;primary_key;auto_increment"`
	Name string `db:"column:name"`
	Age  int    `db:"column:age"`
}

// tierRow is a DTO holding a projected CASE result.
type tierRow struct {
	Name string `db:"column:name"`
	Tier string `db:"column:tier"`
}

func openCaseDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "case_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL, "age" INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "case_user" ("name","age") VALUES ('alice',35),('bob',15),('carol',20)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// A CASE projection computes a per-row label scanned into a DTO field.
func TestE2E_Case_Projection(t *testing.T) {
	db := openCaseDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &caseUser{}
	tier := dsl.Case().When(dsl.Gte(u, &u.Age, 18), "adult").Else("minor")

	var rows []tierRow
	if err := ctx.Set(&rows).From("case_user").
		Select("name").
		SelectCase(tier, "tier").
		OrderBy("name", query.Asc).
		All(); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r.Name] = r.Tier
	}
	want := map[string]string{"alice": "adult", "bob": "minor", "carol": "adult"}
	for name, tierWant := range want {
		if got[name] != tierWant {
			t.Errorf("%s tier: got %q, want %q", name, got[name], tierWant)
		}
	}
}

// A CASE expression on the left of a WHERE predicate filters rows.
func TestE2E_Case_InWhere(t *testing.T) {
	db := openCaseDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &caseUser{}
	tier := dsl.Case().When(dsl.Gte(u, &u.Age, 18), "adult").Else("minor")

	var rows []caseUser
	if err := ctx.Set(&rows).
		Where(query.Predicate{Case: &tier, Op: query.OpEq, Value: "adult"}).
		All(); err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.Name
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "alice" || names[1] != "carol" {
		t.Errorf("CASE in WHERE: got %v, want [alice carol]", names)
	}
}

func init() {
	dsl.Register(caseUser{})
}
