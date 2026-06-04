package memdoc_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/memdoc"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// Vector search is pgvector-specific; a document store rejects a distance
// order-by rather than ignoring it (which would return rows in arbitrary order).
func TestMemDoc_RejectsVectorSearch(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&sqUser{})
	q := query.Query{
		EntityName: meta.Name,
		OrderBy: []query.Sort{{
			Distance: &query.VectorDistance{Field: "embedding", Op: query.VectorL2, Vector: []float32{1, 2, 3}},
		}},
	}
	var out []sqUser
	if err := p.Execute(t.Context(), meta, q, &out); err == nil || !strings.Contains(err.Error(), "vector") {
		t.Fatalf("memdoc should reject vector search, got %v", err)
	}
}
