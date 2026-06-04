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

type soUser struct {
	ID   int    `db:"column:id;primary_key;auto_increment"`
	Name string `db:"column:name"`
	Age  int    `db:"column:age"`
}

func openSetOpDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "so_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL, "age" INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "so_user" ("name","age") VALUES ('alice',35),('bob',15),('carol',25)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func soNames(rows []soUser) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Name
	}
	sort.Strings(out)
	return out
}

func TestE2E_SetOp_Union(t *testing.T) {
	db := openSetOpDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &soUser{}
	var rows []soUser
	// age > 30 UNION age < 20 => alice, bob
	if err := ctx.Set(&rows).
		Where(dsl.Gt(u, &u.Age, 30)).
		Union(ctx.Set(&soUser{}).Where(dsl.Lt(u, &u.Age, 20))).
		All(); err != nil {
		t.Fatal(err)
	}
	got := soNames(rows)
	if len(got) != 2 || got[0] != "alice" || got[1] != "bob" {
		t.Errorf("UNION: got %v, want [alice bob]", got)
	}
}

func TestE2E_SetOp_Intersect(t *testing.T) {
	db := openSetOpDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &soUser{}
	var rows []soUser
	// age > 10 INTERSECT age < 30 => bob(15), carol(25)
	if err := ctx.Set(&rows).
		Where(dsl.Gt(u, &u.Age, 10)).
		Intersect(ctx.Set(&soUser{}).Where(dsl.Lt(u, &u.Age, 30))).
		All(); err != nil {
		t.Fatal(err)
	}
	got := soNames(rows)
	if len(got) != 2 || got[0] != "bob" || got[1] != "carol" {
		t.Errorf("INTERSECT: got %v, want [bob carol]", got)
	}
}

func TestE2E_SetOp_Except(t *testing.T) {
	db := openSetOpDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &soUser{}
	var rows []soUser
	// all EXCEPT age < 20 => alice(35), carol(25)
	if err := ctx.Set(&rows).
		Except(ctx.Set(&soUser{}).Where(dsl.Lt(u, &u.Age, 20))).
		All(); err != nil {
		t.Fatal(err)
	}
	got := soNames(rows)
	if len(got) != 2 || got[0] != "alice" || got[1] != "carol" {
		t.Errorf("EXCEPT: got %v, want [alice carol]", got)
	}
}

// A set operation compiles its operand WHERE into the same parameter list; a
// filtered union returns the right rows (exercises param ordering end-to-end).
func TestE2E_SetOp_BothFiltered(t *testing.T) {
	db := openSetOpDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &soUser{}
	var rows []soUser
	if err := ctx.Set(&rows).
		Where(dsl.Eq(u, &u.Name, "alice")).
		Union(ctx.Set(&soUser{}).Where(dsl.Eq(u, &u.Name, "carol"))).
		All(); err != nil {
		t.Fatal(err)
	}
	got := soNames(rows)
	if len(got) != 2 || got[0] != "alice" || got[1] != "carol" {
		t.Errorf("filtered UNION: got %v, want [alice carol]", got)
	}
}

// The outer ORDER BY / LIMIT bind to the whole compound, not an operand.
func TestE2E_SetOp_OuterOrderLimit(t *testing.T) {
	db := openSetOpDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &soUser{}
	var rows []soUser
	// (age>30) UNION (age<20) = alice(35), bob(15); order by age desc, take 1 => alice.
	if err := ctx.Set(&rows).
		Where(dsl.Gt(u, &u.Age, 30)).
		Union(ctx.Set(&soUser{}).Where(dsl.Lt(u, &u.Age, 20))).
		OrderBy("age", query.Desc).
		Limit(1).
		All(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Name != "alice" {
		t.Errorf("outer ORDER BY/LIMIT on compound: got %v, want [alice]", soNames(rows))
	}
}

// Mixing INTERSECT with another operator is rejected: its precedence is not
// portable (SQLite evaluates left-to-right; other engines bind INTERSECT
// tighter), so the same query would return different rows per backend.
func TestE2E_SetOp_MixedIntersectRejected(t *testing.T) {
	db := openSetOpDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &soUser{}
	var rows []soUser
	err := ctx.Set(&rows).
		Where(dsl.Gt(u, &u.Age, 20)).
		Union(ctx.Set(&soUser{}).Where(dsl.Gt(u, &u.Age, 10))).
		Intersect(ctx.Set(&soUser{}).Where(dsl.Lt(u, &u.Age, 30))).
		All()
	if err == nil {
		t.Fatal("mixing INTERSECT with UNION should be rejected as non-portable")
	}
}

func init() {
	dsl.Register(soUser{})
}
