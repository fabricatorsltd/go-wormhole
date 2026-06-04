package memdoc_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/memdoc"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

type sqUser struct {
	ID   int    `db:"column:id;primary_key"`
	Name string `db:"column:name"`
}

// A document store does not support subquery filters, so a query carrying one
// must be rejected with a clear error rather than silently dropping the filter
// (which would return every row).
func TestMemDoc_RejectsSubquery(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&sqUser{})
	q := query.Query{
		EntityName: meta.Name,
		Where: query.Subquery{
			Field: "id",
			Op:    query.OpIn,
			Query: query.Query{EntityName: "orders", Columns: []string{"user_id"}},
		},
	}
	var out []sqUser
	err := p.Execute(t.Context(), meta, q, &out)
	if err == nil || !strings.Contains(err.Error(), "subquery") {
		t.Fatalf("memdoc should reject a subquery filter, got %v", err)
	}
}

// A subquery buried inside an OR composite is still rejected: the guard recurses
// into composites, so it cannot vanish in a backend that fails open on unknown
// nodes.
func TestMemDoc_RejectsNestedSubquery(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&sqUser{})
	q := query.Query{
		EntityName: meta.Name,
		Where: query.Composite{
			Logic: query.LogicOr,
			Children: []query.Node{
				query.Predicate{Field: "name", Op: query.OpEq, Value: "x"},
				query.Subquery{Field: "id", Op: query.OpExists, Query: query.Query{EntityName: "orders"}},
			},
		},
	}
	var out []sqUser
	if err := p.Execute(t.Context(), meta, q, &out); err == nil || !strings.Contains(err.Error(), "subquery") {
		t.Fatalf("memdoc should reject a nested subquery filter, got %v", err)
	}
}

// A document store cannot do set operations, so a query carrying one must be
// rejected rather than silently returning only the first operand.
func TestMemDoc_RejectsSetOp(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&sqUser{})
	q := query.Query{
		EntityName: meta.Name,
		SetOps:     []query.SetOp{{Kind: query.SetUnion, Query: query.Query{EntityName: meta.Name}}},
	}
	var out []sqUser
	if err := p.Execute(t.Context(), meta, q, &out); err == nil || !strings.Contains(err.Error(), "set operation") {
		t.Fatalf("memdoc should reject a set operation, got %v", err)
	}
}

// A document store cannot evaluate SQL CASE expressions, so a CASE projection
// must be rejected rather than silently dropped.
func TestMemDoc_RejectsCaseProjection(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&sqUser{})
	q := query.Query{
		EntityName: meta.Name,
		CaseSelects: []query.CaseSelect{{
			Expr:  query.CaseExpr{Branches: []query.CaseBranch{{When: query.Predicate{Field: "id", Op: query.OpGt, Value: 0}, Then: "x"}}},
			Alias: "label",
		}},
	}
	var out []sqUser
	if err := p.Execute(t.Context(), meta, q, &out); err == nil || !strings.Contains(err.Error(), "CASE") {
		t.Fatalf("memdoc should reject a CASE projection, got %v", err)
	}
}

// A CASE expression in a WHERE predicate must also be rejected on a document
// store, not silently dropped (the guard walks the WHERE tree).
func TestMemDoc_RejectsCaseInWhere(t *testing.T) {
	p := memdoc.New()
	if err := p.Open(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	meta := schema.Parse(&sqUser{})
	ce := query.CaseExpr{Branches: []query.CaseBranch{{When: query.Predicate{Field: "id", Op: query.OpGt, Value: 0}, Then: "x"}}}
	q := query.Query{
		EntityName: meta.Name,
		Where:      query.Predicate{Case: &ce, Op: query.OpEq, Value: "x"},
	}
	var out []sqUser
	if err := p.Execute(t.Context(), meta, q, &out); err == nil || !strings.Contains(err.Error(), "CASE") {
		t.Fatalf("memdoc should reject a CASE in WHERE, got %v", err)
	}
}
