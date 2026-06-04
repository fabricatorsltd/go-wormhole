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

type subUser struct {
	ID   int    `db:"column:id;primary_key;auto_increment"`
	Name string `db:"column:name"`
}

type subOrder struct {
	ID     int `db:"column:id;primary_key;auto_increment"`
	UserID int `db:"column:user_id"`
	Total  int `db:"column:total"`
}

func openSubqueryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	stmts := []string{
		`CREATE TABLE "sub_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL)`,
		`CREATE TABLE "sub_order" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "user_id" INTEGER NOT NULL, "total" INTEGER NOT NULL)`,
		`INSERT INTO "sub_user" ("id","name") VALUES (1,'alice'),(2,'bob'),(3,'carol')`,
		// alice has a big order, bob has a big order, carol only a small one.
		`INSERT INTO "sub_order" ("user_id","total") VALUES (1,150),(1,50),(2,200),(3,40)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func names(rows []subUser) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Name
	}
	sort.Strings(out)
	return out
}

// id IN (SELECT user_id FROM sub_order WHERE total > 100) returns only the users
// with a qualifying order.
func TestE2E_Subquery_In(t *testing.T) {
	db := openSubqueryDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	o := &subOrder{}
	sub := query.From("sub_order").Select("user_id").
		Filter(dsl.Gt(o, &o.Total, 100)).Build()

	u := &subUser{}
	var rows []subUser
	if err := ctx.Set(&rows).Where(dsl.InSub(u, &u.ID, sub)).All(); err != nil {
		t.Fatal(err)
	}
	got := names(rows)
	if len(got) != 2 || got[0] != "alice" || got[1] != "bob" {
		t.Errorf("IN subquery: got %v, want [alice bob]", got)
	}
}

// A correlated EXISTS keeps users having at least one order over 100.
func TestE2E_Subquery_ExistsCorrelated(t *testing.T) {
	db := openSubqueryDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	sub := query.From("sub_order").Filter(
		query.Predicate{Field: "user_id", Table: "sub_order", Op: query.OpEq, Value: query.ColumnRef{Table: "sub_user", Field: "id"}},
		query.Predicate{Field: "total", Table: "sub_order", Op: query.OpGt, Value: 100},
	).Build()

	var rows []subUser
	if err := ctx.Set(&rows).Where(dsl.Exists(sub)).All(); err != nil {
		t.Fatal(err)
	}
	got := names(rows)
	if len(got) != 2 || got[0] != "alice" || got[1] != "bob" {
		t.Errorf("EXISTS subquery: got %v, want [alice bob]", got)
	}
}

// NOT EXISTS keeps users with no qualifying order.
func TestE2E_Subquery_NotExists(t *testing.T) {
	db := openSubqueryDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	sub := query.From("sub_order").Filter(
		query.Predicate{Field: "user_id", Table: "sub_order", Op: query.OpEq, Value: query.ColumnRef{Table: "sub_user", Field: "id"}},
		query.Predicate{Field: "total", Table: "sub_order", Op: query.OpGt, Value: 100},
	).Build()

	var rows []subUser
	if err := ctx.Set(&rows).Where(dsl.NotExists(sub)).All(); err != nil {
		t.Fatal(err)
	}
	got := names(rows)
	if len(got) != 1 || got[0] != "carol" {
		t.Errorf("NOT EXISTS: got %v, want [carol]", got)
	}
}

// The bulk Delete path also validates the subquery: a multi-column IN subquery
// is rejected, not silently mis-compiled.
func TestE2E_Subquery_BulkDeleteArityGuard(t *testing.T) {
	db := openSubqueryDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &subUser{}
	bad := query.From("sub_order").Select("user_id", "total").Build() // two columns
	if _, err := ctx.Set(&subUser{}).Where(dsl.InSub(u, &u.ID, bad)).Delete(); err == nil {
		t.Fatal("bulk Delete with a multi-column IN subquery should error")
	}
}

func init() {
	dsl.Register(subUser{})
	dsl.Register(subOrder{})
}
