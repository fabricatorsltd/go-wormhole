package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// A nearest-neighbor query compiles to "<op> $N::vector" with the query vector
// bound as a text literal. The explicit ::vector cast is required because an
// operator gives the planner no column to infer the bound param's type from.
func TestCompileSelect_VectorDistance(t *testing.T) {
	meta := testMeta()
	cases := []struct {
		op   query.VectorOp
		want string
	}{
		{query.VectorL2, `"embedding" <-> $1::vector`},
		{query.VectorCosine, `"embedding" <=> $1::vector`},
		{query.VectorInner, `"embedding" <#> $1::vector`},
	}
	for _, tc := range cases {
		q := query.From("doc").
			OrderByDistance(query.VectorDistance{Field: "embedding", Op: tc.op, Vector: []float32{1, 2, 3}}, query.Asc).
			Limit(5).
			Build()
		out := (&wsql.Compiler{Numbered: true}).Select(meta, q) // postgres
		if !strings.Contains(out.SQL, tc.want) {
			t.Errorf("op %d: want SQL to contain %q, got %q", tc.op, tc.want, out.SQL)
		}
		if !strings.Contains(out.SQL, "ORDER BY") || !strings.Contains(out.SQL, "LIMIT 5") {
			t.Errorf("op %d: expected ORDER BY ... LIMIT 5, got %q", tc.op, out.SQL)
		}
		if len(out.Params) != 1 || out.Params[0] != "[1,2,3]" {
			t.Errorf("op %d: want param [1,2,3], got %v", tc.op, out.Params)
		}
	}
}
