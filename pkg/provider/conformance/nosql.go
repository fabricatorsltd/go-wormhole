package conformance

import (
	"context"
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

type NoSQLFactory func(t *testing.T) provider.Provider

// RunNoSQLProvider executes a reusable contract suite against a provider.
func RunNoSQLProvider(t *testing.T, name string, newProvider NoSQLFactory) {
	t.Helper()

	t.Run(name+"/crud", func(t *testing.T) {
		p := newProvider(t)
		meta := schema.Parse(&testUser{})
		ctx := context.Background()

		u := &testUser{ID: 1, Name: "Alice", Age: 30}
		id, err := p.Insert(ctx, meta, u)
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}

		var found testUser
		if err := p.Find(ctx, meta, id, &found); err != nil {
			t.Fatalf("Find: %v", err)
		}
		if found.Name != "Alice" || found.Age != 30 {
			t.Fatalf("unexpected found: %+v", found)
		}

		u.Age = 31
		if err := p.Update(ctx, meta, u, []string{"Age"}); err != nil {
			t.Fatalf("Update(partial): %v", err)
		}

		var after testUser
		if err := p.Find(ctx, meta, id, &after); err != nil {
			t.Fatalf("Find(after update): %v", err)
		}
		if after.Age != 31 {
			t.Fatalf("expected age=31, got %d", after.Age)
		}
		if after.Name != "Alice" {
			t.Fatalf("expected name unchanged, got %q", after.Name)
		}

		_ = p.Delete(ctx, meta, id)
		var gone testUser
		if err := p.Find(ctx, meta, id, &gone); err == nil {
			t.Fatalf("expected not found after delete")
		}
	})

	t.Run(name+"/is-null-filter", func(t *testing.T) {
		p := newProvider(t)
		meta := schema.Parse(&testUser{})
		ctx := context.Background()

		for i := 1; i <= 5; i++ {
			if _, err := p.Insert(ctx, meta, &testUser{ID: i, Name: "u", Age: 10 + i}); err != nil {
				t.Fatalf("seed insert: %v", err)
			}
		}

		// age IS NOT NULL → all 5 records (age is always set)
		qNotNull := query.From(meta.Name).
			Filter(query.Predicate{Field: "age", Op: query.OpIsNotNil}).
			Build()
		var notNullOut []testUser
		if err := p.Execute(ctx, meta, qNotNull, &notNullOut); err != nil {
			t.Fatalf("Execute IS NOT NULL: %v", err)
		}
		if len(notNullOut) != 5 {
			t.Fatalf("IS NOT NULL: expected 5 results, got %d", len(notNullOut))
		}

		// age IS NULL → 0 records (age is always set)
		qNull := query.From(meta.Name).
			Filter(query.Predicate{Field: "age", Op: query.OpIsNil}).
			Build()
		var nullOut []testUser
		if err := p.Execute(ctx, meta, qNull, &nullOut); err != nil {
			t.Fatalf("Execute IS NULL: %v", err)
		}
		if len(nullOut) != 0 {
			t.Fatalf("IS NULL: expected 0 results, got %d", len(nullOut))
		}
	})

	t.Run(name+"/query", func(t *testing.T) {
		p := newProvider(t)
		meta := schema.Parse(&testUser{})
		ctx := context.Background()

		for i := 0; i < 20; i++ {
			_, err := p.Insert(ctx, meta, &testUser{ID: i + 1, Name: "u", Age: 10 + i})
			if err != nil {
				t.Fatalf("seed insert: %v", err)
			}
		}

		q := query.From(meta.Name).
			Filter(query.Predicate{Field: "age", Op: query.OpGt, Value: 20}).
			Offset(2).
			Limit(5).
			Build()

		var out []testUser
		if err := p.Execute(ctx, meta, q, &out); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if len(out) != 5 {
			t.Fatalf("expected 5 results, got %d", len(out))
		}
		for _, u := range out {
			if u.Age <= 20 {
				t.Fatalf("filter failed: %+v", u)
			}
		}
	})

	t.Run(name+"/transactions", func(t *testing.T) {
		p := newProvider(t)
		meta := schema.Parse(&testUser{})
		ctx := context.Background()

		caps := provider.DetectCapabilities(p)
		tx, err := p.Begin(ctx)
		if !caps.Transactions {
			if err == nil {
				t.Fatalf("expected Begin to fail when Transactions=false")
			}
			return
		}
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}

		u := &testUser{ID: 10, Name: "T", Age: 1}
		id, err := tx.Insert(ctx, meta, u)
		if err != nil {
			t.Fatalf("tx.Insert: %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}

		var found testUser
		if err := p.Find(ctx, meta, id, &found); err == nil {
			t.Fatalf("expected rollback to discard inserted entity")
		}

		tx2, err := p.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin2: %v", err)
		}
		id2, err := tx2.Insert(ctx, meta, &testUser{ID: 11, Name: "C", Age: 2})
		if err != nil {
			t.Fatalf("tx2.Insert: %v", err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if err := p.Find(ctx, meta, id2, &found); err != nil {
			t.Fatalf("expected committed entity to be visible: %v", err)
		}
	})

	t.Run(name+"/capability-guards", func(t *testing.T) {
		p := newProvider(t)
		meta := schema.Parse(&testUser{})
		ctx := context.Background()

		caps := provider.DetectCapabilities(p)

		qSorted := query.From(meta.Name).OrderBy("age", query.Desc).Build()
		err := p.Execute(ctx, meta, qSorted, &[]testUser{})
		if caps.Sorting {
			if err != nil {
				t.Fatalf("expected sorting supported, got %v", err)
			}
		} else {
			if err == nil || !strings.Contains(err.Error(), "sorting") {
				t.Fatalf("expected explicit sorting error, got %v", err)
			}
		}

		qInc := query.From(meta.Name).Include("orders").Build()
		err = p.Execute(ctx, meta, qInc, &[]testUser{})
		if caps.Aggregations {
			if err != nil {
				t.Fatalf("expected includes supported, got %v", err)
			}
		} else {
			if err == nil || !strings.Contains(err.Error(), "includes") {
				t.Fatalf("expected explicit includes error, got %v", err)
			}
		}

		qOff := query.From(meta.Name).Offset(1).Build()
		err = p.Execute(ctx, meta, qOff, &[]testUser{})
		if caps.OffsetPagination {
			if err != nil {
				t.Fatalf("expected offset supported, got %v", err)
			}
		} else {
			if err == nil || !strings.Contains(err.Error(), "offset pagination") {
				t.Fatalf("expected explicit offset pagination error, got %v", err)
			}
		}

		var u testUser
		err = p.Find(ctx, meta, -123, &u)
		if err == nil {
			t.Fatalf("expected not-found error")
		}
	})
}

type testUser struct {
	ID   int    `db:"column:id; primary_key"`
	Name string `db:"column:name"`
	Age  int    `db:"column:age"`
}
