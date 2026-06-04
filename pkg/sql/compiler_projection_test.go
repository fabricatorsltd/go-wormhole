package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// Distinct emits SELECT DISTINCT before the column list.
func TestCompileSelect_Distinct(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	q := query.From("users").Distinct().Build()

	out := c.Select(meta, q)

	if !strings.HasPrefix(out.SQL, `SELECT DISTINCT "id", "name", "age" FROM "users"`) {
		t.Fatalf("unexpected DISTINCT SQL:\n%s", out.SQL)
	}
}

// Select restricts the column list to the projected subset, resolving field
// names to columns.
func TestCompileSelect_Projection(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	q := query.From("users").Select("Name", "age").Build()

	out := c.Select(meta, q)

	if !strings.HasPrefix(out.SQL, `SELECT "name", "age" FROM "users"`) {
		t.Fatalf("unexpected projection SQL:\n%s", out.SQL)
	}
}

// Distinct and Select compose into SELECT DISTINCT <subset>.
func TestCompileSelect_DistinctProjection(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	q := query.From("users").Distinct().Select("age").Build()

	out := c.Select(meta, q)

	if !strings.HasPrefix(out.SQL, `SELECT DISTINCT "age" FROM "users"`) {
		t.Fatalf("unexpected DISTINCT projection SQL:\n%s", out.SQL)
	}
}

// On SQL Server, DISTINCT precedes TOP: SELECT DISTINCT TOP n.
func TestCompileSelect_MSSQLDistinctTop(t *testing.T) {
	c := &wsql.Compiler{AtPrefixed: true, BracketQuote: true, UseTOP: true}
	meta := testMeta()
	q := query.From("users").Distinct().Select("name").Limit(5).Build()

	out := c.Select(meta, q)

	if !strings.HasPrefix(out.SQL, `SELECT DISTINCT TOP 5 [name] FROM [users]`) {
		t.Fatalf("unexpected MSSQL DISTINCT TOP SQL:\n%s", out.SQL)
	}
}
