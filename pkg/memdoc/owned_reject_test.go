package memdoc_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/memdoc"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

type owMoney struct {
	Amount   int    `db:"column:amount"`
	Currency string `db:"column:currency"`
}

type owInvoice struct {
	ID    int     `db:"column:id;primary_key"`
	Total owMoney `db:"owned"`
}

// Column-flattened owned types are SQL-only: a document store rejects writing or
// finding such an entity rather than persisting it in a half-mapped shape.
func TestMemDoc_RejectsOwnedType(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&owInvoice{})
	if _, err := p.Insert(t.Context(), meta, &owInvoice{ID: 1}); err == nil || !strings.Contains(err.Error(), "owned") {
		t.Fatalf("Insert of an owned-type entity should be rejected, got %v", err)
	}
	var out owInvoice
	if err := p.Find(t.Context(), meta, 1, &out); err == nil || !strings.Contains(err.Error(), "owned") {
		t.Fatalf("Find of an owned-type entity should be rejected, got %v", err)
	}
}
