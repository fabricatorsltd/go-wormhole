package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// UNION compiles two SELECT bodies joined by the keyword, with one trailing
// ORDER BY/LIMIT bound to the compound and continuous placeholder numbering.
func TestCompileSelect_Union_Numbered(t *testing.T) {
	c := &wsql.Compiler{Numbered: true}
	meta := testMeta()
	right := query.From("users").
		Filter(query.Predicate{Field: "age", Op: query.OpLt, Value: 10}).
		Build()
	q := query.From("users").
		Filter(query.Predicate{Field: "age", Op: query.OpGt, Value: 18}).
		Union(right).
		OrderBy("id", query.Asc).
		Limit(5).
		Build()

	out := c.Select(meta, q)

	want := `SELECT "id", "name", "age" FROM "users" WHERE "age" > $1 UNION SELECT "id", "name", "age" FROM "users" WHERE "age" < $2 ORDER BY "id" ASC LIMIT 5`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	if len(out.Params) != 2 || out.Params[0] != 18 || out.Params[1] != 10 {
		t.Errorf("params: got %v, want [18 10]", out.Params)
	}
}

// Each set-op keyword renders, and an ORDER BY/LIMIT on the operand is ignored
// (only the outer tail binds to the compound).
func TestCompileSelect_SetOpKeywords(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	cases := []struct {
		build func(right query.Query) query.Query
		kw    string
	}{
		{func(r query.Query) query.Query { return query.From("users").UnionAll(r).Build() }, "UNION ALL"},
		{func(r query.Query) query.Query { return query.From("users").Intersect(r).Build() }, "INTERSECT"},
		{func(r query.Query) query.Query { return query.From("users").Except(r).Build() }, "EXCEPT"},
	}
	for _, tc := range cases {
		// operand carries its own ORDER BY + LIMIT, which must be dropped.
		right := query.From("users").OrderBy("name", query.Asc).Limit(99).Build()
		out := c.Select(meta, tc.build(right))
		if !strings.Contains(out.SQL, " "+tc.kw+" SELECT ") {
			t.Errorf("%s: keyword missing:\n%s", tc.kw, out.SQL)
		}
		if strings.Contains(out.SQL, `"name" ASC`) || strings.Contains(out.SQL, "LIMIT 99") {
			t.Errorf("%s: operand ORDER BY/LIMIT leaked:\n%s", tc.kw, out.SQL)
		}
	}
}
