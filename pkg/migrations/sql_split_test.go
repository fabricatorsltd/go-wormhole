package migrations_test

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
)

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		name   string
		script string
		want   []string
	}{
		{
			name:   "plain",
			script: "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT);",
			want:   []string{"CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)"},
		},
		{
			name:   "semicolon inside a line comment does not split",
			script: "-- drop; then recreate\nCREATE TABLE a (id INT);",
			want:   []string{"CREATE TABLE a (id INT)"},
		},
		{
			// The exact failure from the bug report: a multi-line comment whose
			// first line ends in a semicolon, before a real statement.
			name: "report repro: semicolon in header comment",
			script: "-- Removes orphaned tables. IF EXISTS makes this a no-op on fresh DBs; CASCADE\n" +
				"-- clears the inter-table foreign keys.\n\n" +
				"DROP TABLE IF EXISTS foo, bar CASCADE;",
			want: []string{"DROP TABLE IF EXISTS foo, bar CASCADE"},
		},
		{
			name:   "inline trailing comment with semicolon",
			script: "CREATE TABLE a (id INT); -- note; here\nCREATE TABLE b (id INT);",
			want:   []string{"CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)"},
		},
		{
			name:   "block comment with semicolon",
			script: "/* a; b */ CREATE TABLE a (id INT);",
			want:   []string{"CREATE TABLE a (id INT)"},
		},
		{
			name:   "semicolon inside a single-quoted string",
			script: "INSERT INTO t (v) VALUES ('a;b');\nINSERT INTO t (v) VALUES ('c');",
			want:   []string{"INSERT INTO t (v) VALUES ('a;b')", "INSERT INTO t (v) VALUES ('c')"},
		},
		{
			name:   "escaped quote inside string",
			script: "INSERT INTO t (v) VALUES ('O''Brien; Jr');",
			want:   []string{"INSERT INTO t (v) VALUES ('O''Brien; Jr')"},
		},
		{
			name: "semicolons inside a dollar-quoted function body",
			script: "CREATE FUNCTION f() RETURNS int AS $$\n" +
				"BEGIN\n  x := 1; y := 2;\n  RETURN x;\nEND;\n$$ LANGUAGE plpgsql;\n" +
				"SELECT f();",
			want: []string{
				"CREATE FUNCTION f() RETURNS int AS $$\nBEGIN\n  x := 1; y := 2;\n  RETURN x;\nEND;\n$$ LANGUAGE plpgsql",
				"SELECT f()",
			},
		},
		{
			name: "tagged dollar quote",
			script: "CREATE FUNCTION g() RETURNS text AS $body$ SELECT 'a;b'; $body$ LANGUAGE sql;",
			want:   []string{"CREATE FUNCTION g() RETURNS text AS $body$ SELECT 'a;b'; $body$ LANGUAGE sql"},
		},
		{
			// A digit-led $1$ token is a parameter context, not a dollar quote,
			// so the two statements must stay split (a naive tag match would
			// treat $1$ ... $2$ as one quoted body and swallow the semicolon).
			name:   "digit-led dollar token is not a dollar quote",
			script: "INSERT INTO t VALUES ($1$); INSERT INTO t VALUES ($2$);",
			want:   []string{"INSERT INTO t VALUES ($1$)", "INSERT INTO t VALUES ($2$)"},
		},
		{
			name:   "header comment block then statement",
			script: "-- generated\n-- dialect: pg\n\nCREATE TABLE a (id INT);",
			want:   []string{"CREATE TABLE a (id INT)"},
		},
		{
			name:   "comment-only script yields nothing",
			script: "-- just a note\n/* and another */\n",
			want:   nil,
		},
		{
			name:   "trailing statement without terminator",
			script: "CREATE TABLE a (id INT)",
			want:   []string{"CREATE TABLE a (id INT)"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := migrations.SplitStatements(c.script)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %#v\nwant %#v", got, c.want)
			}
		})
	}
}
