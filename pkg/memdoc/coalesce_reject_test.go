package memdoc_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/memdoc"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// COALESCE is a SQL expression; a document store rejects it rather than dropping
// it (which would change which rows match).
func TestMemDoc_RejectsCoalesce(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&sqUser{})
	expr := query.CoalesceExpr{Args: []query.CoalesceArg{{Column: "name"}, {Value: "x"}}}
	q := query.Query{
		EntityName: meta.Name,
		Where:      query.Predicate{Coalesce: &expr, Op: query.OpEq, Value: "x"},
	}
	var out []sqUser
	if err := p.Execute(t.Context(), meta, q, &out); err == nil || !strings.Contains(err.Error(), "COALESCE") {
		t.Fatalf("memdoc should reject a COALESCE expression, got %v", err)
	}
}
