package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// TestCompileSelect_CountStarOverJoin checks that aggregate COUNT(*) works
// against a query that also has joins — i.e. that the SQL compiler emits
// JOIN clauses inside an aggregate query.
func TestCompileSelect_CountStarOverJoin(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Join("post", query.Predicate{
			Field: "user_id", Table: "post",
			Op:    query.OpEq,
			Value: query.ColumnRef{Field: "id", Table: "users"},
		}).
		Where("age", query.OpGt, 18).
		Aggregate(query.AggCount, "*", "total").
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `COUNT(*) AS "total"`) {
		t.Errorf("expected COUNT(*) AS total: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `JOIN "post"`) {
		t.Errorf("expected JOIN in aggregate query: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `"age" > `) {
		t.Errorf("expected WHERE filter: %s", out.SQL)
	}
}
