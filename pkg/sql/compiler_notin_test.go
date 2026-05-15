package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// TestCompileSelect_NotInBasic verifies WHERE col NOT IN (a, b).
func TestCompileSelect_NotInBasic(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Where("age", query.OpNotIn, []any{18, 21}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `"age" NOT IN (`) {
		t.Errorf("expected NOT IN syntax: %s", out.SQL)
	}
	if len(out.Params) != 2 || out.Params[0] != 18 || out.Params[1] != 21 {
		t.Errorf("params = %v, want [18 21]", out.Params)
	}
}

// TestCompileSelect_NotInEmpty verifies the empty-list NOT IN degrades to
// "IS NOT NULL" (always-true-for-non-null) so the surrounding SQL still parses.
func TestCompileSelect_NotInEmpty(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Where("age", query.OpNotIn, []any{}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `"age" IS NOT NULL`) {
		t.Errorf("expected IS NOT NULL fallback: %s", out.SQL)
	}
}

// TestCompileSelect_InEmpty verifies the empty-list IN degrades to a
// contradiction so the surrounding SQL still parses (Postgres etc.).
func TestCompileSelect_InEmpty(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Where("age", query.OpIn, []any{}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `"age" = NULL`) {
		t.Errorf("expected = NULL contradiction fallback: %s", out.SQL)
	}
}

// TestCompileSelect_NotInNumbered verifies $N placeholders for Postgres.
func TestCompileSelect_NotInNumbered(t *testing.T) {
	c := &wsql.Compiler{Numbered: true}
	meta := testMeta()

	q := query.From("users").
		Where("age", query.OpNotIn, []any{3, -3}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `NOT IN ($1, $2)`) {
		t.Errorf("expected $1, $2 placeholders: %s", out.SQL)
	}
}
