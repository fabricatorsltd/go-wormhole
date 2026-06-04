package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func coalesce(args ...query.CoalesceArg) query.CoalesceExpr {
	return query.CoalesceExpr{Args: args}
}

// A COALESCE projection renders COALESCE(col, col, ?) AS alias, with literal
// operands bound as parameters.
func TestCompileSelect_Coalesce_Projection(t *testing.T) {
	meta := testMeta()
	expr := coalesce(query.CoalesceArg{Column: "nickname"}, query.CoalesceArg{Column: "name"}, query.CoalesceArg{Value: "Anonymous"})
	q := query.From("user").Select("id").SelectCoalesce(expr, "display").Build()

	out := (&wsql.Compiler{}).Select(meta, q)
	want := `COALESCE("nickname", "name", ?) AS "display"`
	if !strings.Contains(out.SQL, want) {
		t.Errorf("want SQL to contain %q, got %q", want, out.SQL)
	}
	if len(out.Params) != 1 || out.Params[0] != "Anonymous" {
		t.Errorf("params: got %v, want [Anonymous]", out.Params)
	}
}

// A COALESCE expression works on the left of a WHERE predicate.
func TestCompileSelect_Coalesce_Where(t *testing.T) {
	meta := testMeta()
	expr := coalesce(query.CoalesceArg{Column: "deleted"}, query.CoalesceArg{Value: false})
	q := query.From("user").Filter(query.Predicate{Coalesce: &expr, Op: query.OpEq, Value: false}).Build()

	out := (&wsql.Compiler{Numbered: true}).Select(meta, q) // postgres
	want := `WHERE COALESCE("deleted", $1) = $2`
	if !strings.Contains(out.SQL, want) {
		t.Errorf("want SQL to contain %q, got %q", want, out.SQL)
	}
	if len(out.Params) != 2 || out.Params[0] != false || out.Params[1] != false {
		t.Errorf("params: got %v, want [false false]", out.Params)
	}
}

// A COALESCE expression is usable as an ORDER BY term.
func TestCompileSelect_Coalesce_OrderBy(t *testing.T) {
	meta := testMeta()
	expr := coalesce(query.CoalesceArg{Column: "sort_key"}, query.CoalesceArg{Value: 999})
	q := query.From("user").OrderByCoalesce(expr, query.Asc).Build()

	out := (&wsql.Compiler{}).Select(meta, q)
	want := `ORDER BY COALESCE("sort_key", ?) ASC`
	if !strings.Contains(out.SQL, want) {
		t.Errorf("want SQL to contain %q, got %q", want, out.SQL)
	}
	if len(out.Params) != 1 || out.Params[0] != 999 {
		t.Errorf("params: got %v, want [999]", out.Params)
	}
}

// Projection literals are numbered before WHERE literals, keeping placeholder
// numbering continuous across the SELECT list and the WHERE clause.
func TestCompileSelect_Coalesce_ParamOrder(t *testing.T) {
	meta := testMeta()
	proj := coalesce(query.CoalesceArg{Column: "a"}, query.CoalesceArg{Value: "X"})
	where := coalesce(query.CoalesceArg{Column: "b"}, query.CoalesceArg{Value: 7})
	q := query.From("user").
		Select("id").
		SelectCoalesce(proj, "d").
		Filter(query.Predicate{Coalesce: &where, Op: query.OpGt, Value: 0}).
		Build()

	out := (&wsql.Compiler{Numbered: true}).Select(meta, q)
	if !strings.Contains(out.SQL, `COALESCE("a", $1) AS "d"`) || !strings.Contains(out.SQL, `COALESCE("b", $2) > $3`) {
		t.Fatalf("placeholder numbering wrong: %s", out.SQL)
	}
	if len(out.Params) != 3 || out.Params[0] != "X" || out.Params[1] != 7 || out.Params[2] != 0 {
		t.Errorf("params: got %v, want [X 7 0]", out.Params)
	}
}
