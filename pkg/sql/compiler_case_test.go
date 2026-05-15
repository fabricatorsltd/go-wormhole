package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

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
