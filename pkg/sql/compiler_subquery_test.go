package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// IN (subquery) renders the nested SELECT and keeps placeholder numbering
// continuous across the outer and inner predicates under numbered ($N) params.
func TestCompileSelect_InSubquery_Numbered(t *testing.T) {
	c := &wsql.Compiler{Numbered: true}
	meta := testMeta()
	sub := query.From("orders").Select("user_id").
		Filter(query.Predicate{Field: "total", Op: query.OpGt, Value: 100}).Build()
	q := query.From("users").
		Filter(query.Predicate{Field: "age", Op: query.OpGt, Value: 18}).
		Filter(query.Subquery{Field: "id", Op: query.OpIn, Query: sub}).
		Build()

	out := c.Select(meta, q)

	want := `SELECT "id", "name", "age" FROM "users" WHERE ("age" > $1 AND "id" IN (SELECT "user_id" FROM "orders" WHERE "total" > $2))`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	if len(out.Params) != 2 || out.Params[0] != 18 || out.Params[1] != 100 {
		t.Errorf("params: got %v, want [18 100]", out.Params)
	}
}

// NOT IN (subquery).
func TestCompileSelect_NotInSubquery(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	sub := query.From("orders").Select("user_id").Build()
	q := query.From("users").Filter(query.Subquery{Field: "id", Op: query.OpNotIn, Query: sub}).Build()

	out := c.Select(meta, q)
	if !strings.Contains(out.SQL, `"id" NOT IN (SELECT "user_id" FROM "orders")`) {
		t.Fatalf("unexpected NOT IN SQL:\n%s", out.SQL)
	}
}

// EXISTS projects "1" and ignores the subquery's Select; a correlated column-ref
// predicate renders qualified on both sides.
func TestCompileSelect_ExistsCorrelated(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	sub := query.From("orders").Filter(
		query.Predicate{Field: "user_id", Table: "orders", Op: query.OpEq, Value: query.ColumnRef{Table: "users", Field: "id"}},
		query.Predicate{Field: "total", Table: "orders", Op: query.OpGt, Value: 100},
	).Build()
	q := query.From("users").Filter(query.Subquery{Op: query.OpExists, Query: sub}).Build()

	out := c.Select(meta, q)
	want := `SELECT "id", "name", "age" FROM "users" WHERE EXISTS (SELECT 1 FROM "orders" WHERE ("orders"."user_id" = "users"."id" AND "orders"."total" > $1))`
	// default dialect uses ? not $1
	want = strings.Replace(want, "$1", "?", 1)
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	if len(out.Params) != 1 || out.Params[0] != 100 {
		t.Errorf("params: got %v, want [100]", out.Params)
	}
}
