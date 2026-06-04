package sql

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
)

// vectorText renders both float widths in pgvector's literal form; the scanner
// parses it back. Round-trip preserves the values.
func TestVectorText_RoundTrip(t *testing.T) {
	s, ok := vectorText([]float32{1, 2.5, -3})
	if !ok || s != "[1,2.5,-3]" {
		t.Fatalf("float32 literal: got %q ok=%v", s, ok)
	}
	if s, _ := vectorText([]float64{1, 2}); s != "[1,2]" {
		t.Errorf("float64 literal: got %q", s)
	}

	var out []float32
	if err := (vectorScanner{dst: &out}).Scan("[1,2.5,-3]"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, []float32{1, 2.5, -3}) {
		t.Errorf("scan round-trip: got %v", out)
	}
	// NULL scans to an untouched (nil) slice, not an error.
	var n []float32
	if err := (vectorScanner{dst: &n}).Scan(nil); err != nil || n != nil {
		t.Errorf("nil scan: got %v err=%v", n, err)
	}
}

// requireVectorDialect rejects a vector ORDER BY on a non-postgres dialect (the
// pgvector operators do not exist there) and passes it on postgres.
func TestRequireVectorDialect(t *testing.T) {
	q := query.From("doc").
		OrderByDistance(query.VectorDistance{Field: "embedding", Op: query.VectorL2, Vector: []float32{1}}, query.Asc).
		Build()

	if err := requireVectorDialect(&Compiler{}, q); err == nil { // sqlite
		t.Error("sqlite should reject a vector distance order-by")
	}
	if err := requireVectorDialect(&Compiler{Backtick: true}, q); err == nil { // mysql
		t.Error("mysql should reject a vector distance order-by")
	}
	if err := requireVectorDialect(&Compiler{Numbered: true}, q); err != nil { // postgres
		t.Errorf("postgres should allow vector distance, got %v", err)
	}
	// A query with no vector term is fine on any dialect.
	if err := requireVectorDialect(&Compiler{}, query.From("doc").Build()); err != nil {
		t.Errorf("non-vector query should pass: %v", err)
	}
}
