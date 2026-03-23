package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// TestCompileSelect_CountStar verifies COUNT(*) AS alias compilation.
func TestCompileSelect_CountStar(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Aggregate(query.AggCount, "*", "total").
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `COUNT(*) AS "total"`) {
		t.Fatalf("SQL missing COUNT(*) AS total: %s", out.SQL)
	}
	if len(out.Params) != 0 {
		t.Fatalf("params: want 0, got %d", len(out.Params))
	}
}

// TestCompileSelect_CountField verifies COUNT(field) AS alias compilation.
func TestCompileSelect_CountField(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Aggregate(query.AggCount, "id", "total").
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `COUNT("id") AS "total"`) {
		t.Fatalf("SQL missing COUNT(id): %s", out.SQL)
	}
}

// TestCompileSelect_SumAvgMinMax verifies SUM, AVG, MIN, MAX aggregate compilation.
func TestCompileSelect_SumAvgMinMax(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	tests := []struct {
		fn    query.AggFunc
		field string
		alias string
		want  string
	}{
		{query.AggSum, "age", "total_age", `SUM("age") AS "total_age"`},
		{query.AggAvg, "age", "avg_age", `AVG("age") AS "avg_age"`},
		{query.AggMin, "age", "min_age", `MIN("age") AS "min_age"`},
		{query.AggMax, "age", "max_age", `MAX("age") AS "max_age"`},
	}

	for _, tt := range tests {
		q := query.From("users").
			Aggregate(tt.fn, tt.field, tt.alias).
			Build()
		out := c.Select(meta, q)
		if !strings.Contains(out.SQL, tt.want) {
			t.Errorf("fn=%v: SQL missing %q in: %s", tt.fn, tt.want, out.SQL)
		}
	}
}

// TestCompileSelect_GroupBy verifies GROUP BY clause generation.
func TestCompileSelect_GroupBy(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		GroupBy("age").
		Aggregate(query.AggCount, "*", "count").
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `GROUP BY "age"`) {
		t.Fatalf("SQL missing GROUP BY: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `COUNT(*) AS "count"`) {
		t.Fatalf("SQL missing COUNT(*): %s", out.SQL)
	}
	// GROUP BY field should appear in SELECT too
	if !strings.Contains(out.SQL, `SELECT "age"`) {
		t.Fatalf("SQL missing grouped column in SELECT: %s", out.SQL)
	}
}

// TestCompileSelect_GroupByMultipleFields verifies multiple GROUP BY fields.
func TestCompileSelect_GroupByMultipleFields(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		GroupBy("name", "age").
		Aggregate(query.AggCount, "*", "count").
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `GROUP BY "name", "age"`) {
		t.Fatalf("SQL missing multi-field GROUP BY: %s", out.SQL)
	}
}

// TestCompileSelect_Having verifies HAVING clause generation.
func TestCompileSelect_Having(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		GroupBy("age").
		Aggregate(query.AggCount, "*", "count").
		Having(query.Predicate{Field: "count", Op: query.OpGt, Value: 2}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `HAVING "count" > ?`) {
		t.Fatalf("SQL missing HAVING clause: %s", out.SQL)
	}
	if len(out.Params) != 1 || out.Params[0] != 2 {
		t.Fatalf("params: want [2], got %v", out.Params)
	}
}

// TestCompileSelect_GroupByWhereHaving verifies WHERE + GROUP BY + HAVING order.
func TestCompileSelect_GroupByWhereHaving(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Filter(query.Predicate{Field: "age", Op: query.OpGt, Value: 10}).
		GroupBy("age").
		Aggregate(query.AggCount, "*", "count").
		Having(query.Predicate{Field: "count", Op: query.OpGte, Value: 3}).
		Build()
	out := c.Select(meta, q)

	whereIdx := strings.Index(out.SQL, "WHERE")
	groupIdx := strings.Index(out.SQL, "GROUP BY")
	havingIdx := strings.Index(out.SQL, "HAVING")

	if whereIdx < 0 || groupIdx < 0 || havingIdx < 0 {
		t.Fatalf("SQL missing WHERE/GROUP BY/HAVING: %s", out.SQL)
	}
	if !(whereIdx < groupIdx && groupIdx < havingIdx) {
		t.Fatalf("clause order wrong (want WHERE < GROUP BY < HAVING): %s", out.SQL)
	}
}

// TestCompileSelect_GroupByOrderBy verifies GROUP BY followed by ORDER BY.
func TestCompileSelect_GroupByOrderBy(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		GroupBy("age").
		Aggregate(query.AggCount, "*", "count").
		OrderBy("age", query.Asc).
		Build()
	out := c.Select(meta, q)

	groupIdx := strings.Index(out.SQL, "GROUP BY")
	orderIdx := strings.Index(out.SQL, "ORDER BY")

	if groupIdx < 0 || orderIdx < 0 {
		t.Fatalf("SQL missing GROUP BY or ORDER BY: %s", out.SQL)
	}
	if groupIdx >= orderIdx {
		t.Fatalf("GROUP BY must come before ORDER BY: %s", out.SQL)
	}
}

// TestCompileSelect_AggregateNoAlias verifies aggregate without an alias.
func TestCompileSelect_AggregateNoAlias(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Aggregate(query.AggCount, "*", "").
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, "COUNT(*)") {
		t.Fatalf("SQL missing COUNT(*): %s", out.SQL)
	}
	if strings.Contains(out.SQL, " AS ") {
		t.Fatalf("SQL should not have AS when alias is empty: %s", out.SQL)
	}
}

// TestMSSQLCompile_CountGroupBy verifies GROUP BY compilation in MSSQL dialect.
func TestMSSQLCompile_CountGroupBy(t *testing.T) {
	c := &wsql.Compiler{AtPrefixed: true, BracketQuote: true, UseTOP: true}
	meta := testMeta()

	q := query.From("users").
		GroupBy("age").
		Aggregate(query.AggCount, "*", "count").
		Having(query.Predicate{Field: "count", Op: query.OpGt, Value: 1}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, "[age]") {
		t.Fatalf("expected bracket quoting on age: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, "GROUP BY [age]") {
		t.Fatalf("SQL missing GROUP BY [age]: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, "@p1") {
		t.Fatalf("SQL missing @p1 placeholder: %s", out.SQL)
	}
}
