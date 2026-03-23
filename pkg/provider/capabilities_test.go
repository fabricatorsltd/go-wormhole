package provider_test

import (
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
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
	_, err := provider.ValidateQueryCapabilities(nil, provider.Capabilities{Sorting: false}, qSorted)
	if err == nil {
		t.Fatal("expected sorting capability error")
	}

	qInclude := query.From("users").Include("orders").Build()
	_, err = provider.ValidateQueryCapabilities(nil, provider.Capabilities{Aggregations: false}, qInclude)
	if err == nil {
		t.Fatal("expected include capability error")
	}

	qOffset := query.From("users").Offset(10).Build()
	_, err = provider.ValidateQueryCapabilities(nil, provider.Capabilities{OffsetPagination: false}, qOffset)
	if err == nil {
		t.Fatal("expected offset capability error")
	}

	qOK := query.From("users").OrderBy("name", query.Asc).Offset(1).Build()
	_, err = provider.ValidateQueryCapabilities(nil, provider.Capabilities{
		Sorting:          true,
		OffsetPagination: true,
	}, qOK)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateQueryCapabilities_AggregateValidation(t *testing.T) {
	meta := &model.EntityMeta{
		Name: "users",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id"},
			{FieldName: "Age", Column: "age"},
			{FieldName: "Name", Column: "name"},
		},
	}
	meta.BuildIndex()

	caps := provider.Capabilities{Sorting: true, OffsetPagination: true, Aggregations: true}

	t.Run("normalizes valid aggregate query", func(t *testing.T) {
		q := query.From("users").
			GroupBy("Age").
			Aggregate(query.AggCount, "*", "count").
			Having(query.Predicate{Field: "Age", Op: query.OpGt, Value: 18}).
			OrderBy("Age", query.Asc).
			Build()

		normalized, err := provider.ValidateQueryCapabilities(meta, caps, q)
		if err != nil {
			t.Fatalf("unexpected validation error: %v", err)
		}
		if normalized.GroupBy[0] != "age" {
			t.Fatalf("group by not normalized: %#v", normalized.GroupBy)
		}
		having, ok := normalized.Having.(query.Predicate)
		if !ok || having.Field != "age" {
			t.Fatalf("having not normalized: %#v", normalized.Having)
		}
		if normalized.OrderBy[0].Field != "age" {
			t.Fatalf("order by not normalized: %#v", normalized.OrderBy)
		}
	})

	t.Run("rejects invalid group by field", func(t *testing.T) {
		q := query.From("users").
			GroupBy("missing").
			Aggregate(query.AggCount, "*", "count").
			Build()

		if _, err := provider.ValidateQueryCapabilities(meta, caps, q); err == nil {
			t.Fatal("expected invalid GROUP BY field error")
		}
	})

	t.Run("rejects invalid aggregate field", func(t *testing.T) {
		q := query.From("users").
			Aggregate(query.AggSum, "missing", "total").
			Build()

		if _, err := provider.ValidateQueryCapabilities(meta, caps, q); err == nil {
			t.Fatal("expected invalid aggregate field error")
		}
	})

	t.Run("rejects undefined having alias", func(t *testing.T) {
		q := query.From("users").
			GroupBy("age").
			Aggregate(query.AggCount, "*", "count").
			Having(query.Predicate{Field: "missing", Op: query.OpGt, Value: 1}).
			Build()

		if _, err := provider.ValidateQueryCapabilities(meta, caps, q); err == nil {
			t.Fatal("expected invalid HAVING reference error")
		}
	})
}
