package context

import (
	"fmt"
	"reflect"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// EntitySet provides a fluent API for querying and managing entities
// of a single type through the DbContext.
type EntitySet struct {
	ctx           *DbContext
	dest          any
	meta          *model.EntityMeta
	tableOverride string
	preds         []query.Node
	sorts         []query.Sort
	lim           int
	off           int
	groupBy       []string
	havingPreds   []query.Node
	aggregates    []query.Aggregate
	joins         []query.JoinSpec
}

// Set creates an EntitySet bound to the given destination.
// dest can be a *Struct (for Find) or *[]Struct / *[]*Struct (for queries).
func (c *DbContext) Set(dest any) *EntitySet {
	t := reflect.TypeOf(dest)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	meta := schema.ParseType(t)
	return &EntitySet{
		ctx:  c,
		dest: dest,
		meta: meta,
	}
}

// Find retrieves a single entity by primary key, populates dest,
// and auto-tracks it as Unchanged.
func (s *EntitySet) Find(pk any) error {
	ctx := s.ctx.opCtx()
	err := s.ctx.withReadResilience(ctx, func() error {
		return s.ctx.provider.Find(ctx, s.meta, pk, s.dest)
	})
	if err != nil {
		return err
	}
	s.ctx.tracker.Attach(s.dest)
	return nil
}

// From overrides the table name used for the FROM clause.
//
// This is useful when the destination is a custom DTO struct (e.g. an
// aggregate result holder) whose Go type name doesn't match the source
// table. Without an override, the FROM clause derives from the dest meta
// (snake_case of the struct name) which is wrong for cross-table queries.
//
//	type CountRow struct {
//	    PostID int64 `db:"post_id"`
//	    Count  int   `db:"count"`
//	}
//	var rows []CountRow
//	db.Set(&rows).
//	    From("social_post").                            // ← required
//	    Where(...).
//	    GroupBy("post_id").
//	    Aggregate(query.AggCount, "*", "count").
//	    All()
func (s *EntitySet) From(table string) *EntitySet {
	s.tableOverride = table
	return s
}

// Where appends filter predicates (top-level AND logic).
// Accepts any query.Node, so composite dsl.And / dsl.Or trees work too.
// Chainable.
func (s *EntitySet) Where(nodes ...query.Node) *EntitySet {
	s.preds = append(s.preds, nodes...)
	return s
}

// OrderBy appends a sort clause. Chainable.
func (s *EntitySet) OrderBy(field string, dir query.SortDir) *EntitySet {
	s.sorts = append(s.sorts, query.Sort{Field: field, Dir: dir})
	return s
}

// OrderByCase appends a sort clause that uses a CASE WHEN expression
// instead of a column reference, enabling sort-priority patterns such as
// "pinned items first, then everything else."
func (s *EntitySet) OrderByCase(c query.CaseExpr, dir query.SortDir) *EntitySet {
	s.sorts = append(s.sorts, query.Sort{Case: &c, Dir: dir})
	return s
}

// Limit sets the maximum number of results.
func (s *EntitySet) Limit(n int) *EntitySet {
	s.lim = n
	return s
}

// Offset sets the number of results to skip.
func (s *EntitySet) Offset(n int) *EntitySet {
	s.off = n
	return s
}

// GroupBy appends fields to the GROUP BY clause. Chainable.
func (s *EntitySet) GroupBy(fields ...string) *EntitySet {
	s.groupBy = append(s.groupBy, fields...)
	return s
}

// Having appends predicate(s) for the HAVING clause (AND logic). Chainable.
// Accepts any query.Node.
func (s *EntitySet) Having(nodes ...query.Node) *EntitySet {
	s.havingPreds = append(s.havingPreds, nodes...)
	return s
}

// Aggregate appends an aggregate expression (COUNT, SUM, AVG, MIN, MAX)
// to the SELECT clause. field may be "*" or empty for COUNT(*).
func (s *EntitySet) Aggregate(fn query.AggFunc, field, alias string) *EntitySet {
	s.aggregates = append(s.aggregates, query.Aggregate{Func: fn, Field: field, Alias: alias})
	return s
}

// Join attaches an INNER JOIN to the underlying query. The on predicate is
// typically produced by dsl.JoinEq for type-safe joins, but any Predicate
// or Composite is accepted.
//
// Result rows are still scanned into the primary entity (s.dest) — joined
// tables are visible only in WHERE/ORDER BY at this point. DTO-typed result
// scanning is planned as a follow-up.
func (s *EntitySet) Join(entity string, on query.Node) *EntitySet {
	s.joins = append(s.joins, query.JoinSpec{Type: query.JoinInner, Entity: entity, On: on})
	return s
}

// LeftJoin attaches a LEFT JOIN to the underlying query.
func (s *EntitySet) LeftJoin(entity string, on query.Node) *EntitySet {
	s.joins = append(s.joins, query.JoinSpec{Type: query.JoinLeft, Entity: entity, On: on})
	return s
}

// All executes the built query and scans results into dest
// (must be *[]T or *[]*T).
func (s *EntitySet) All() error {
	q := s.buildQuery()

	ctx := s.ctx.opCtx()
	return s.ctx.withReadResilience(ctx, func() error {
		return s.ctx.provider.Execute(ctx, s.meta, q, s.dest)
	})
}

// Delete performs a bulk DELETE against the underlying provider matching the
// current Where chain. Returns the number of rows affected (-1 if the driver
// cannot report it).
//
// An EntitySet with no Where predicates deletes every row in the entity table.
// OrderBy/Limit/Offset/GroupBy/Having/Aggregate are intentionally ignored —
// portable SQL DELETE does not support them.
//
// Returns an error if the provider does not implement provider.BulkDeleter.
// Callers can fall back to loading rows + Remove + Save in that case.
func (s *EntitySet) Delete() (int64, error) {
	bd, ok := s.ctx.provider.(provider.BulkDeleter)
	if !ok {
		return 0, fmt.Errorf("provider %q does not support bulk delete (BulkDeleter)", s.ctx.provider.Name())
	}
	q := s.buildQuery()
	ctx := s.ctx.opCtx()
	return bd.DeleteWhere(ctx, s.meta, q)
}

// Add marks entities for insertion.
func (s *EntitySet) Add(entities ...any) {
	for _, e := range entities {
		s.ctx.tracker.Add(e)
	}
}

// Remove marks entities for deletion.
func (s *EntitySet) Remove(entities ...any) {
	for _, e := range entities {
		s.ctx.tracker.Remove(e)
	}
}

// ToSQL compiles the current query chain and returns the SQL string and
// parameters without executing anything. Returns an error if the
// provider does not implement QueryExplainer.
func (s *EntitySet) ToSQL() (string, []any, error) {
	exp, ok := s.ctx.provider.(provider.QueryExplainer)
	if !ok {
		return "", nil, fmt.Errorf("provider %q does not support ToSQL (QueryExplainer)", s.ctx.provider.Name())
	}

	q := s.buildQuery()
	c, err := exp.ExplainSelect(s.meta, q)
	if err != nil {
		return "", nil, err
	}
	return c.SQL, c.Params, nil
}

func (s *EntitySet) buildQuery() query.Query {
	tableName := s.meta.Name
	if s.tableOverride != "" {
		tableName = s.tableOverride
	}
	b := query.From(tableName)
	if len(s.preds) > 0 {
		b.Filter(s.preds...)
	}
	for _, sort := range s.sorts {
		if sort.Case != nil {
			b.OrderByCase(*sort.Case, sort.Dir)
		} else {
			b.OrderBy(sort.Field, sort.Dir)
		}
	}
	if s.lim > 0 {
		b.Limit(s.lim)
	}
	if s.off > 0 {
		b.Offset(s.off)
	}
	if len(s.groupBy) > 0 {
		b.GroupBy(s.groupBy...)
	}
	if len(s.havingPreds) > 0 {
		b.Having(s.havingPreds...)
	}
	for _, agg := range s.aggregates {
		b.Aggregate(agg.Func, agg.Field, agg.Alias)
	}
	for _, j := range s.joins {
		switch j.Type {
		case query.JoinLeft:
			b.LeftJoin(j.Entity, j.On)
		default:
			b.Join(j.Entity, j.On)
		}
	}
	return b.Build()
}
