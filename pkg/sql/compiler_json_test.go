package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// A JSON-path predicate compiles to the dialect-specific extraction expression.
// PostgreSQL casts the text column to jsonb (its -> operators do not work on
// text); the others use their JSON path functions.
func TestCompileSelect_JSONPath_PerDialect(t *testing.T) {
	meta := testMeta()
	q := query.From("users").
		Filter(query.Predicate{Field: "profile", JSONPath: "address.city", Op: query.OpEq, Value: "Berlin"}).
		Build()

	cases := []struct {
		name string
		c    *wsql.Compiler
		want string
	}{
		{"sqlite", &wsql.Compiler{}, `json_extract("profile", '$.address.city')`},
		{"postgres", &wsql.Compiler{Numbered: true}, `("profile"::jsonb #>> '{address,city}')`},
		{"mysql", &wsql.Compiler{Backtick: true}, "JSON_UNQUOTE(JSON_EXTRACT(`profile`, '$.address.city'))"},
		{"mssql", &wsql.Compiler{AtPrefixed: true, BracketQuote: true}, `JSON_VALUE([profile], '$.address.city')`},
	}
	for _, tc := range cases {
		out := tc.c.Select(meta, q)
		if !strings.Contains(out.SQL, tc.want) {
			t.Errorf("%s: want SQL to contain %q, got %q", tc.name, tc.want, out.SQL)
		}
		if len(out.Params) != 1 || out.Params[0] != "Berlin" {
			t.Errorf("%s: params: got %v, want [Berlin]", tc.name, out.Params)
		}
	}
}

// The JSON path is interpolated into the SQL string (it cannot be bound), so it
// is sanitized to identifier characters: an injection attempt cannot break out.
func TestCompileSelect_JSONPath_Sanitized(t *testing.T) {
	meta := testMeta()
	q := query.From("users").
		Filter(query.Predicate{Field: "profile", JSONPath: "x.b'; DROP TABLE users; --", Op: query.OpEq, Value: 1}).
		Build()

	out := (&wsql.Compiler{}).Select(meta, q)
	if strings.Contains(out.SQL, "DROP TABLE") || strings.Contains(out.SQL, "';") {
		t.Fatalf("unsanitized JSON path leaked into SQL: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, "$.x.bDROPTABLEusers") {
		t.Errorf("expected sanitized path, got %s", out.SQL)
	}
}
