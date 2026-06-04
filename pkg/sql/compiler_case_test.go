package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func ageTierCase() query.CaseExpr {
	return query.CaseExpr{
		Branches: []query.CaseBranch{
			{When: query.Predicate{Field: "age", Op: query.OpGte, Value: 18}, Then: "adult"},
		},
		Else: "minor",
	}
}

// A CASE expression in the SELECT list renders aliased, with its THEN/ELSE
// params emitted before any WHERE params.
func TestCompileSelect_CaseProjection(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	q := query.From("users").Select("id").SelectCase(ageTierCase(), "tier").Build()

	out := c.Select(meta, q)

	want := `SELECT "id", CASE WHEN "age" >= ? THEN ? ELSE ? END AS "tier" FROM "users"`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	if len(out.Params) != 3 || out.Params[0] != 18 || out.Params[1] != "adult" || out.Params[2] != "minor" {
		t.Errorf("params: got %v, want [18 adult minor]", out.Params)
	}
}

// A CASE expression on the left of a WHERE predicate renders and keeps the
// comparison value as the trailing param (continuous $N numbering).
func TestCompileSelect_CaseInWhere(t *testing.T) {
	c := &wsql.Compiler{Numbered: true}
	meta := testMeta()
	ce := ageTierCase()
	q := query.From("users").
		Filter(query.Predicate{Case: &ce, Op: query.OpEq, Value: "adult"}).
		Build()

	out := c.Select(meta, q)

	want := `SELECT "id", "name", "age" FROM "users" WHERE CASE WHEN "age" >= $1 THEN $2 ELSE $3 END = $4`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	if len(out.Params) != 4 || out.Params[3] != "adult" {
		t.Errorf("params: got %v, want [18 adult minor adult]", out.Params)
	}
}

// Two CASE projections are comma-separated from each other and from the columns.
func TestCompileSelect_MultipleCaseProjections(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	q := query.From("users").Select("id").
		SelectCase(ageTierCase(), "t1").
		SelectCase(ageTierCase(), "t2").
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `AS "t1", CASE`) {
		t.Errorf("missing comma between case selects:\n%s", out.SQL)
	}
	if !strings.Contains(out.SQL, `AS "t2" FROM`) {
		t.Errorf("second case select malformed:\n%s", out.SQL)
	}
}

// A CASE projection combined with an aggregate is rejected (the CASE would
// otherwise be silently dropped in the aggregate branch).
func TestValidateCapabilities_AggregateWithCaseSelect(t *testing.T) {
	caps := provider.Capabilities{CaseExpressions: true, Aggregations: true}
	q := query.Query{
		EntityName: "users",
		Aggregates: []query.Aggregate{{Func: query.AggCount, Field: "*", Alias: "c"}},
		CaseSelects: []query.CaseSelect{
			{Expr: ageTierCase(), Alias: "t"},
		},
	}
	if _, err := provider.ValidateQueryCapabilities(nil, caps, q); err == nil || !strings.Contains(err.Error(), "aggregates") {
		t.Fatalf("aggregate + CASE projection should be rejected, got %v", err)
	}
}

// TestCompileSelect_OrderByCase verifies ORDER BY CASE WHEN … THEN … ELSE … END.
func TestCompileSelect_OrderByCase(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	ce := query.CaseExpr{
		Branches: []query.CaseBranch{
			{When: query.Predicate{Field: "age", Op: query.OpGt, Value: 18}, Then: 0},
		},
		Else: 1,
	}
	q := query.From("users").
		OrderByCase(ce, query.Asc).
		Build()

	out := c.Select(meta, q)

	want := `ORDER BY CASE WHEN "age" > ? THEN ? ELSE ? END ASC`
	if !strings.Contains(out.SQL, want) {
		t.Errorf("SQL = %q\nwant substring %q", out.SQL, want)
	}
	if len(out.Params) != 3 || out.Params[0] != 18 || out.Params[1] != 0 || out.Params[2] != 1 {
		t.Errorf("params = %v, want [18 0 1]", out.Params)
	}
}

// TestCompileSelect_OrderByCase_CompositeWhen verifies an AND-composite WHEN clause.
func TestCompileSelect_OrderByCase_CompositeWhen(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	ce := query.CaseExpr{
		Branches: []query.CaseBranch{
			{
				When: query.Composite{
					Logic: query.LogicAnd,
					Children: []query.Node{
						query.Predicate{Field: "age", Op: query.OpIsNotNil},
						query.Predicate{Field: "age", Op: query.OpGt, Value: 18},
					},
				},
				Then: 0,
			},
		},
		Else: 1,
	}
	q := query.From("users").OrderByCase(ce, query.Asc).Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, "WHEN") || !strings.Contains(out.SQL, "AND") {
		t.Errorf("SQL missing WHEN/AND: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `END ASC`) {
		t.Errorf("SQL missing closing END: %s", out.SQL)
	}
}

// TestCompileSelect_OrderByCase_MultipleBranches_NoElse verifies multi-WHEN
// and that omitting ELSE yields a CASE without an ELSE clause.
func TestCompileSelect_OrderByCase_MultipleBranches_NoElse(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	ce := query.CaseExpr{
		Branches: []query.CaseBranch{
			{When: query.Predicate{Field: "age", Op: query.OpLt, Value: 18}, Then: 1},
			{When: query.Predicate{Field: "age", Op: query.OpLt, Value: 65}, Then: 2},
		},
	}
	q := query.From("users").OrderByCase(ce, query.Desc).Build()
	out := c.Select(meta, q)

	if strings.Contains(out.SQL, "ELSE") {
		t.Errorf("SQL should not contain ELSE when Else is nil: %s", out.SQL)
	}
	whenCount := strings.Count(out.SQL, "WHEN")
	if whenCount != 2 {
		t.Errorf("expected 2 WHEN branches, got %d: %s", whenCount, out.SQL)
	}
	if !strings.Contains(out.SQL, "DESC") {
		t.Errorf("direction missing: %s", out.SQL)
	}
}

// TestCompileSelect_OrderByCase_MixedWithColumnSort verifies a CASE sort
// can be combined with a regular column sort in a single ORDER BY clause.
func TestCompileSelect_OrderByCase_MixedWithColumnSort(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	ce := query.CaseExpr{
		Branches: []query.CaseBranch{
			{When: query.Predicate{Field: "name", Op: query.OpIsNotNil}, Then: 0},
		},
		Else: 1,
	}
	q := query.From("users").
		OrderByCase(ce, query.Asc).
		OrderBy("name", query.Desc).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `END ASC, "name" DESC`) {
		t.Errorf("expected CASE END ASC, name DESC: %s", out.SQL)
	}
}

// TestCompileSelect_OrderByCase_Numbered verifies Postgres $N placeholders.
func TestCompileSelect_OrderByCase_Numbered(t *testing.T) {
	c := &wsql.Compiler{Numbered: true}
	meta := testMeta()

	ce := query.CaseExpr{
		Branches: []query.CaseBranch{
			{When: query.Predicate{Field: "age", Op: query.OpGt, Value: 18}, Then: 0},
		},
		Else: 1,
	}
	q := query.From("users").OrderByCase(ce, query.Asc).Build()
	out := c.Select(meta, q)

	for _, ph := range []string{"$1", "$2", "$3"} {
		if !strings.Contains(out.SQL, ph) {
			t.Errorf("expected placeholder %s in SQL: %s", ph, out.SQL)
		}
	}
}
