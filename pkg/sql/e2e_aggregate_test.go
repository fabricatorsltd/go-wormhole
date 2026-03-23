package sql_test

import (
	"context"
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// AgeCount is a custom result struct used for COUNT + GROUP BY queries.
type AgeCount struct {
	Age   int `db:"column:age"`
	Count int `db:"column:count"`
}

// TotalCount holds a single COUNT(*) result.
type TotalCount struct {
	Total int `db:"column:total"`
}

// AgeStats holds aggregate stats for age.
type AgeStats struct {
	MinAge int `db:"column:min_age"`
	MaxAge int `db:"column:max_age"`
}

func TestE2E_CountStar(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	userMeta := schema.Parse(&User{})

	for i := 0; i < 5; i++ {
		p.Insert(ctx, userMeta, &User{Name: "u", Age: i})
	}

	q := query.From("user").
		Aggregate(query.AggCount, "*", "total").
		Build()

	resultMeta := schema.Parse(&TotalCount{})
	var results []TotalCount
	if err := p.Execute(ctx, resultMeta, q, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}
	if results[0].Total != 5 {
		t.Fatalf("expected total=5, got %d", results[0].Total)
	}
}

func TestE2E_CountGroupBy(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	userMeta := schema.Parse(&User{})

	p.Insert(ctx, userMeta, &User{Name: "Alice", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "Bob", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "Charlie", Age: 25})
	p.Insert(ctx, userMeta, &User{Name: "Dave", Age: 25})
	p.Insert(ctx, userMeta, &User{Name: "Eve", Age: 30})

	q := query.From("user").
		GroupBy("age").
		Aggregate(query.AggCount, "*", "count").
		OrderBy("age", query.Asc).
		Build()

	resultMeta := schema.Parse(&AgeCount{})
	var results []AgeCount
	if err := p.Execute(ctx, resultMeta, q, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 age groups, got %d", len(results))
	}
	if results[0].Age != 20 || results[0].Count != 2 {
		t.Fatalf("age=20: want count=2, got age=%d count=%d", results[0].Age, results[0].Count)
	}
	if results[1].Age != 25 || results[1].Count != 2 {
		t.Fatalf("age=25: want count=2, got age=%d count=%d", results[1].Age, results[1].Count)
	}
	if results[2].Age != 30 || results[2].Count != 1 {
		t.Fatalf("age=30: want count=1, got age=%d count=%d", results[2].Age, results[2].Count)
	}
}

func TestE2E_CountGroupByHaving(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	userMeta := schema.Parse(&User{})

	// Insert: age 20 x3, age 25 x1, age 30 x1
	p.Insert(ctx, userMeta, &User{Name: "A", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "B", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "C", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "D", Age: 25})
	p.Insert(ctx, userMeta, &User{Name: "E", Age: 30})

	// Only return groups with count >= 2
	q := query.From("user").
		GroupBy("age").
		Aggregate(query.AggCount, "*", "count").
		Having(query.Predicate{Field: "count", Op: query.OpGte, Value: 2}).
		Build()

	resultMeta := schema.Parse(&AgeCount{})
	var results []AgeCount
	if err := p.Execute(ctx, resultMeta, q, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 group (age=20 with count=3), got %d", len(results))
	}
	if results[0].Age != 20 || results[0].Count != 3 {
		t.Fatalf("expected age=20 count=3, got age=%d count=%d", results[0].Age, results[0].Count)
	}
}

func TestE2E_CountWhereGroupBy(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	userMeta := schema.Parse(&User{})

	p.Insert(ctx, userMeta, &User{Name: "A", Age: 10})
	p.Insert(ctx, userMeta, &User{Name: "B", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "C", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "D", Age: 30})

	// WHERE age > 15, GROUP BY age — should exclude age=10 group
	q := query.From("user").
		Filter(query.Predicate{Field: "age", Op: query.OpGt, Value: 15}).
		GroupBy("age").
		Aggregate(query.AggCount, "*", "count").
		OrderBy("age", query.Asc).
		Build()

	resultMeta := schema.Parse(&AgeCount{})
	var results []AgeCount
	if err := p.Execute(ctx, resultMeta, q, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 groups (20, 30), got %d", len(results))
	}
	if results[0].Age != 20 || results[0].Count != 2 {
		t.Fatalf("age=20: want count=2, got %+v", results[0])
	}
	if results[1].Age != 30 || results[1].Count != 1 {
		t.Fatalf("age=30: want count=1, got %+v", results[1])
	}
}

func TestE2E_SumAndMaxAggregates(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	userMeta := schema.Parse(&User{})

	p.Insert(ctx, userMeta, &User{Name: "A", Age: 10})
	p.Insert(ctx, userMeta, &User{Name: "B", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "C", Age: 30})

	q := query.From("user").
		Aggregate(query.AggMin, "age", "min_age").
		Aggregate(query.AggMax, "age", "max_age").
		Build()

	resultMeta := schema.Parse(&AgeStats{})
	var results []AgeStats
	if err := p.Execute(ctx, resultMeta, q, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}
	if results[0].MinAge != 10 {
		t.Fatalf("expected min_age=10, got %d", results[0].MinAge)
	}
	if results[0].MaxAge != 30 {
		t.Fatalf("expected max_age=30, got %d", results[0].MaxAge)
	}
}

func TestE2E_CountGroupByUsingGoFieldNames(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	userMeta := schema.Parse(&User{})

	p.Insert(ctx, userMeta, &User{Name: "Alice", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "Bob", Age: 20})
	p.Insert(ctx, userMeta, &User{Name: "Charlie", Age: 25})

	q := query.From("user").
		GroupBy("Age").
		Aggregate(query.AggCount, "*", "count").
		OrderBy("Age", query.Asc).
		Build()

	resultMeta := schema.Parse(&AgeCount{})
	var results []AgeCount
	if err := p.Execute(ctx, resultMeta, q, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 age groups, got %d", len(results))
	}
}

func TestE2E_AggregateValidationErrors(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	userMeta := schema.Parse(&User{})
	resultMeta := schema.Parse(&AgeCount{})

	p.Insert(ctx, userMeta, &User{Name: "Alice", Age: 20})

	tests := []struct {
		name string
		q    query.Query
		want string
	}{
		{
			name: "group by missing field",
			q: query.From("user").
				GroupBy("missing").
				Aggregate(query.AggCount, "*", "count").
				Build(),
			want: `GROUP BY field "missing"`,
		},
		{
			name: "aggregate missing field",
			q: query.From("user").
				Aggregate(query.AggSum, "missing", "sum_age").
				Build(),
			want: `aggregate field "missing"`,
		},
		{
			name: "having missing alias",
			q: query.From("user").
				GroupBy("age").
				Aggregate(query.AggCount, "*", "count").
				Having(query.Predicate{Field: "missing_alias", Op: query.OpGt, Value: 1}).
				Build(),
			want: `HAVING field "missing_alias"`,
		},
		{
			name: "group by without aggregate",
			q: query.From("user").
				GroupBy("age").
				Build(),
			want: "GROUP BY requires at least one aggregate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var results []AgeCount
			err := p.Execute(ctx, resultMeta, tt.q, &results)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.want)
			}
		})
	}
}
