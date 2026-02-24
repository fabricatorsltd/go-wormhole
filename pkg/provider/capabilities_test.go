package provider_test

import (
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/slipstream"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func TestDetectCapabilities_SQL(t *testing.T) {
	p := wsql.New(nil)
	c := provider.DetectCapabilities(p)

	if !c.Transactions {
		t.Fatal("sql provider should support transactions")
	}
	if !c.PartialUpdate {
		t.Fatal("sql provider should support partial updates")
	}
	if !c.OffsetPagination {
		t.Fatal("sql provider should support offset pagination")
	}
	if !c.Sorting {
		t.Fatal("sql provider should support sorting")
	}
	if !c.SchemaMigrations {
		t.Fatal("sql provider should support schema migrations")
	}
	if c.SchemaEvolution {
		t.Fatal("sql provider should not declare NoSQL schema evolution")
	}
}

func TestDetectCapabilities_Slipstream(t *testing.T) {
	p, err := slipstream.New(t.TempDir())
	if err != nil {
		t.Fatalf("slipstream.New: %v", err)
	}
	defer p.Close()

	c := provider.DetectCapabilities(p)
	if !c.Transactions {
		t.Fatal("slipstream provider should support transactions")
	}
	if !c.SchemaEvolution {
		t.Fatal("slipstream provider should support schema evolution")
	}
	if c.SchemaMigrations {
		t.Fatal("slipstream provider should not declare SQL schema migrations")
	}
	if c.Sorting {
		t.Fatal("slipstream provider should not declare sorting")
	}
}

func TestValidateQueryCapabilities(t *testing.T) {
	qSorted := query.From("users").OrderBy("name", query.Asc).Build()
	err := provider.ValidateQueryCapabilities(provider.Capabilities{Sorting: false}, qSorted)
	if err == nil {
		t.Fatal("expected sorting capability error")
	}

	qInclude := query.From("users").Include("orders").Build()
	err = provider.ValidateQueryCapabilities(provider.Capabilities{Aggregations: false}, qInclude)
	if err == nil {
		t.Fatal("expected include capability error")
	}

	qOffset := query.From("users").Offset(10).Build()
	err = provider.ValidateQueryCapabilities(provider.Capabilities{OffsetPagination: false}, qOffset)
	if err == nil {
		t.Fatal("expected offset capability error")
	}

	qOK := query.From("users").OrderBy("name", query.Asc).Offset(1).Build()
	err = provider.ValidateQueryCapabilities(provider.Capabilities{
		Sorting:          true,
		OffsetPagination: true,
	}, qOK)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}
