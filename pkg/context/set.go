package context

import (
	stdctx "context"
	"database/sql"
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
	includes      []string
	ignoreFilters bool
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

// IgnoreFilters runs this query without the DbContext's registered query
// filters (the equivalent of EF Core's IgnoreQueryFilters). It applies to the
// whole query, including eager-loaded relations. Chainable.
func (s *EntitySet) IgnoreFilters() *EntitySet {
	s.ignoreFilters = true
	return s
}

// activeFilters returns the registered query filters that apply to this set,
// or nil when filters are ignored.
func (s *EntitySet) activeFilters() []query.Node {
	if s.ignoreFilters {
		return nil
	}
	return s.ctx.filtersFor(s.meta.Name)
}

// Find retrieves a single entity by primary key, populates dest,
// and auto-tracks it as Unchanged.
//
// When query filters are registered for the entity, Find applies them too, so
// a primary-key lookup cannot reach a row excluded by a filter (e.g. another
// tenant's row, or a soft-deleted one). Use IgnoreFilters to bypass.
func (s *EntitySet) Find(pk any) error {
	ctx := s.ctx.opCtx()

	filters := s.activeFilters()
	if len(filters) == 0 {
		err := s.ctx.withReadResilience(ctx, func() error {
			return s.ctx.provider.Find(ctx, s.meta, pk, s.dest)
		})
		if err != nil {
			return err
		}
		s.ctx.tracker.Attach(s.dest)
		return nil
	}
	return s.findFiltered(ctx, pk, filters)
}

// findFiltered loads a single entity by primary key through the query path so
// the registered filters apply. Returns sql.ErrNoRows when no row matches both
// the key and the filters, matching the keyed-lookup Find.
func (s *EntitySet) findFiltered(ctx stdctx.Context, pk any, filters []query.Node) error {
	pkCol := "id"
	if s.meta.PrimaryKey != nil {
		pkCol = s.meta.PrimaryKey.Column
	}

	b := query.From(s.meta.Name)
	preds := append([]query.Node{query.Predicate{Field: pkCol, Op: query.OpEq, Value: pk}}, filters...)
	b.Filter(preds...)
	b.Limit(1)
	q := b.Build()

	elem := reflect.TypeOf(s.dest).Elem()
	slicePtr := reflect.New(reflect.SliceOf(reflect.PtrTo(elem))) // *[]*T
	if err := s.ctx.withReadResilience(ctx, func() error {
		return s.ctx.provider.Execute(ctx, s.meta, q, slicePtr.Interface())
	}); err != nil {
		return err
	}

	slice := slicePtr.Elem()
	if slice.Len() == 0 {
		return sql.ErrNoRows
	}
	reflect.ValueOf(s.dest).Elem().Set(slice.Index(0).Elem())
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

// Include eager-loads the named navigation relations after the main query
// runs, using a separate batched query per relation (no cartesian JOIN).
// Names are the Go navigation field names (e.g. "Orders", "Profile"); the
// type-safe dsl.FieldName(&u, &u.Orders) resolves one at compile time.
// Chainable.
func (s *EntitySet) Include(relations ...string) *EntitySet {
	s.includes = append(s.includes, relations...)
	return s
}

// All executes the built query and scans results into dest
// (must be *[]T or *[]*T). When Include was used, each named relation is
// loaded and stitched onto the results before returning.
func (s *EntitySet) All() error {
	q := s.buildQuery()

	ctx := s.ctx.opCtx()
	if err := s.ctx.withReadResilience(ctx, func() error {
		return s.ctx.provider.Execute(ctx, s.meta, q, s.dest)
	}); err != nil {
		return err
	}

	for _, name := range s.includes {
		if err := s.loadInclude(ctx, name); err != nil {
			return err
		}
	}
	return nil
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

// Update performs a bulk UPDATE against the underlying provider, applying the
// given assignments to every row matching the current Where chain. Returns the
// number of rows affected (-1 if the driver cannot report it).
//
// Assignments are typically produced by dsl.Set for type safety:
//
//	u := &User{}
//	n, err := db.Set(&User{}).
//	    Where(dsl.Eq(u, &u.Status, "pending")).
//	    Update(dsl.Set(u, &u.Status, "active"))
//
// An EntitySet with no Where predicates updates every row in the table.
// OrderBy/Limit/Offset/GroupBy/Having are intentionally ignored, like Delete.
//
// This is a direct set-based statement: it does not load, change-track, or run
// optimistic-concurrency checks on the affected rows, and it does not refresh
// any already-tracked entities (a tracked row updated here keeps its stale
// snapshot, so a later SaveChanges on it can overwrite these changes). Returns
// an error if no assignments are given or the provider does not implement
// provider.BulkUpdater.
func (s *EntitySet) Update(sets ...query.Assignment) (int64, error) {
	if len(sets) == 0 {
		return 0, fmt.Errorf("Update requires at least one assignment")
	}
	bu, ok := s.ctx.provider.(provider.BulkUpdater)
	if !ok {
		return 0, fmt.Errorf("provider %q does not support bulk update (BulkUpdater)", s.ctx.provider.Name())
	}
	q := s.buildQuery()
	ctx := s.ctx.opCtx()
	return bu.UpdateWhere(ctx, s.meta, q, sets)
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
	// Apply the caller's predicates plus any registered query filters, ANDed
	// together. Filters are keyed on the table actually queried (tableName), so
	// a From() override onto a registered, filtered table is still scoped rather
	// than reading across the filter. A local copy avoids mutating s.preds
	// (buildQuery runs more than once, e.g. All then ToSQL).
	preds := s.preds
	if !s.ignoreFilters {
		if f := s.ctx.filtersFor(tableName); len(f) > 0 {
			preds = append(append([]query.Node{}, s.preds...), f...)
		}
	}
	if len(preds) > 0 {
		b.Filter(preds...)
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
