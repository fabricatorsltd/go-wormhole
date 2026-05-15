package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// TestCompileDeleteWhere_NoWhere verifies an unconditional DELETE.
func TestCompileDeleteWhere_NoWhere(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").Build()
	out := c.DeleteWhere(meta, q)

	want := `DELETE FROM "users"`
	if out.SQL != want {
		t.Errorf("SQL = %q, want %q", out.SQL, want)
	}
	if len(out.Params) != 0 {
		t.Errorf("params = %v, want []", out.Params)
	}
}

// TestCompileDeleteWhere_SinglePredicate verifies DELETE with one filter.
func TestCompileDeleteWhere_SinglePredicate(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Where("age", query.OpLt, 18).
		Build()
	out := c.DeleteWhere(meta, q)

	if !strings.Contains(out.SQL, `DELETE FROM "users" WHERE "age" < `) {
		t.Errorf("SQL missing expected DELETE WHERE shape: %s", out.SQL)
	}
	if len(out.Params) != 1 || out.Params[0] != 18 {
		t.Errorf("params = %v, want [18]", out.Params)
	}
}

// TestCompileDeleteWhere_Numbered verifies $N placeholders for Postgres.
func TestCompileDeleteWhere_Numbered(t *testing.T) {
	c := &wsql.Compiler{Numbered: true}
	meta := testMeta()

	q := query.From("users").
		Where("age", query.OpGt, 30).
		Build()
	out := c.DeleteWhere(meta, q)

	if !strings.Contains(out.SQL, `$1`) {
		t.Errorf("expected $1 placeholder, got: %s", out.SQL)
	}
	if strings.Contains(out.SQL, `?`) {
		t.Errorf("expected no ? placeholder in numbered mode, got: %s", out.SQL)
	}
}

// TestCompileDeleteWhere_IgnoresOrderLimitGroupBy verifies that DELETE
// strips OrderBy/Limit/Offset/GroupBy/Aggregates — these are unsupported
// in portable SQL DELETE.
func TestCompileDeleteWhere_IgnoresOrderLimitGroupBy(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Where("age", query.OpEq, 99).
		OrderBy("name", query.Asc).
		Limit(10).
		Offset(5).
		Build()
	out := c.DeleteWhere(meta, q)

	for _, banned := range []string{"ORDER BY", "LIMIT", "OFFSET", "GROUP BY"} {
		if strings.Contains(out.SQL, banned) {
			t.Errorf("DELETE should not contain %q, got: %s", banned, out.SQL)
		}
	}
}
