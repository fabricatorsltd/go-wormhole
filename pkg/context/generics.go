package context

import (
	stdctx "context"

	"github.com/mirkobrombin/go-wormhole/pkg/query"
	"github.com/mirkobrombin/go-wormhole/pkg/schema"
)

// Find retrieves a single entity of type T by primary key.
// The result is automatically tracked as Unchanged.
//
//	u, err := context.Find[User](ctx, dbCtx, 42)
func Find[T any](ctx stdctx.Context, c *DbContext, pk any) (*T, error) {
	var zero T
	meta := schema.Parse(&zero)

	dest := new(T)
	err := c.withReadResilience(ctx, func() error {
		return c.provider.Find(ctx, meta, pk, dest)
	})
	if err != nil {
		return nil, err
	}
	c.tracker.Attach(dest)
	return dest, nil
}

// QueryResult holds the results of a generic query and allows
// fluent chaining before execution.
type QueryResult[T any] struct {
	ctx     *DbContext
	builder *query.Builder
}

// Query starts a fluent, type-safe query for entities of type T.
//
//	results, err := context.Query[User](dbCtx).
//	    Where(dsl.Gt(&u, &u.Age, 18)).
//	    Limit(10).
//	    Exec(ctx)
func Query[T any](c *DbContext) *QueryResult[T] {
	var zero T
	meta := schema.Parse(&zero)
	return &QueryResult[T]{
		ctx:     c,
		builder: query.From(meta.Name),
	}
}

// Where appends predicates (AND logic).
func (qr *QueryResult[T]) Where(preds ...query.Predicate) *QueryResult[T] {
	qr.builder.Filter(preds...)
	return qr
}

// OrderBy appends a sort clause.
func (qr *QueryResult[T]) OrderBy(field string, dir query.SortDir) *QueryResult[T] {
	qr.builder.OrderBy(field, dir)
	return qr
}

// Limit sets the maximum number of results.
func (qr *QueryResult[T]) Limit(n int) *QueryResult[T] {
	qr.builder.Limit(n)
	return qr
}

// Offset sets the number of results to skip.
func (qr *QueryResult[T]) Offset(n int) *QueryResult[T] {
	qr.builder.Offset(n)
	return qr
}

// Exec runs the query and returns a typed slice.
func (qr *QueryResult[T]) Exec(ctx stdctx.Context) ([]T, error) {
	var zero T
	meta := schema.Parse(&zero)
	q := qr.builder.Build()

	var results []T
	err := qr.ctx.withReadResilience(ctx, func() error {
		return qr.ctx.provider.Execute(ctx, meta, q, &results)
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}
