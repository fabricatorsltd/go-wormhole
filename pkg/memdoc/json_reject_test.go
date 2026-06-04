package memdoc_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/memdoc"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// JSON-path predicates are a SQL construct (they extract from a JSON text
// column). A document store rejects them rather than silently dropping the
// filter, which would return every row.
func TestMemDoc_RejectsJSONPath(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&sqUser{})
	q := query.Query{
		EntityName: meta.Name,
		Where:      query.Predicate{Field: "name", JSONPath: "a.b", Op: query.OpEq, Value: "x"},
	}
	var out []sqUser
	if err := p.Execute(t.Context(), meta, q, &out); err == nil || !strings.Contains(err.Error(), "JSON") {
		t.Fatalf("memdoc should reject a JSON path query, got %v", err)
	}
}
