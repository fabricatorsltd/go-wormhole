package context

import (
	stdctx "context"
	"database/sql"
	"fmt"
	"iter"
	"reflect"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// trackMode is an EntitySet's per-query change-tracking choice. The zero value
// defers to the path default and the DbContext setting.
type trackMode int

const (
	trackDefault trackMode = iota // path default (Find tracks, All does not)
	trackOff                      // NoTracking: never attach results
	trackOn                       // AsTracking: attach results
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
	tracking      trackMode
	distinct      bool
	selectCols    []string
	setOps        []setOpSpec
	caseSelects   []query.CaseSelect
}

// setOpSpec pairs a set operation with the EntitySet whose query is the operand.
type setOpSpec struct {
	kind  query.SetOpKind
	other *EntitySet
}

// NoTracking runs this query without attaching the result to the change
// tracker, the equivalent of EF Core's AsNoTracking. The returned entities are
// detached: Save will not persist later mutations to them, and they are not
// retained in the identity map. Useful for read-only lookups. Chainable.
//
// Collection queries (All) are non-tracking by default, so this only changes
// the single-entity Find paths; it also overrides a context-level AsTracking.
func (s *EntitySet) NoTracking() *EntitySet {
	s.tracking = trackOff
	return s
}

// AsTracking attaches the query result to the change tracker so a later Save
// persists mutations to it, the equivalent of EF Core's AsTracking. It is the
// way to opt a collection query (All) into load-mutate-save, and to force
// tracking on a Find when the context defaults to no-tracking. Chainable.
//
// For collection queries the destination must be *[]*T (a slice of pointers):
// tracked entities need stable addresses, which a *[]T value slice cannot give.
// All returns an error otherwise. Only the top-level results are tracked;
// eager-loaded relations from Include are not.
func (s *EntitySet) AsTracking() *EntitySet {
	s.tracking = trackOn
	return s
}

// trackFind reports whether a single-entity read should attach its result.
// Find tracks by default unless the context opts out; explicit per-query modes
// win over the context setting.
func (s *EntitySet) trackFind() bool {
	switch s.tracking {
	case trackOn:
		return true
	case trackOff:
		return false
	default:
		return !s.ctx.noTracking
	}
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
func (s *EntitySet) Find(pk ...any) error {
	if err := s.ctx.requireKeySupport(s.meta); err != nil {
		return err
	}
	if n := len(keyFields(s.meta)); n > 0 && len(pk) != n {
		return fmt.Errorf("Find: %q has %d primary key column(s), got %d key value(s)", s.meta.Name, n, len(pk))
	}

	ctx := s.ctx.opCtx()
	key := keyArg(pk)

	filters := s.activeFilters()
	if len(filters) == 0 {
		err := s.ctx.withReadResilience(ctx, func() error {
			return s.ctx.provider.Find(ctx, s.meta, key, s.dest)
		})
		if err != nil {
			return err
		}
		if s.trackFind() {
			s.ctx.tracker.Attach(s.dest)
		}
		return nil
	}
	return s.findFiltered(ctx, pk, filters)
}

// keyArg packs Find's key values into the provider argument: a scalar for a
// single-column key, the []any tuple for a composite key.
func keyArg(pk []any) any {
	if len(pk) == 1 {
		return pk[0]
	}
	return pk
}

// findFiltered loads a single entity by primary key through the query path so
// the registered filters apply. Returns sql.ErrNoRows when no row matches both
// the key and the filters, matching the keyed-lookup Find.
func (s *EntitySet) findFiltered(ctx stdctx.Context, pk []any, filters []query.Node) error {
	keys := keyFields(s.meta)
	preds := make([]query.Node, 0, len(keys)+len(filters))
	if len(keys) == 0 && len(pk) > 0 {
		// No declared PK: match on "id" by convention, mirroring the keyed path.
		preds = append(preds, query.Predicate{Field: "id", Op: query.OpEq, Value: pk[0]})
	}
	for i, k := range keys {
		preds = append(preds, query.Predicate{Field: k.Column, Op: query.OpEq, Value: pk[i]})
	}
	preds = append(preds, filters...)

	b := query.From(s.meta.Name)
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
	if s.trackFind() {
		s.ctx.tracker.Attach(s.dest)
	}
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

// Union combines this query with another EntitySet's via UNION (duplicate rows
// removed). Both sets should produce the same columns. Sort and limit the
// combined result on this (outer) set; ORDER BY / Limit on the operand are
// ignored. Chainable.
func (s *EntitySet) Union(other *EntitySet) *EntitySet {
	s.setOps = append(s.setOps, setOpSpec{kind: query.SetUnion, other: other})
	return s
}

// UnionAll is Union without duplicate removal. Chainable.
func (s *EntitySet) UnionAll(other *EntitySet) *EntitySet {
	s.setOps = append(s.setOps, setOpSpec{kind: query.SetUnionAll, other: other})
	return s
}

// Intersect keeps only rows present in both queries. Chainable.
func (s *EntitySet) Intersect(other *EntitySet) *EntitySet {
	s.setOps = append(s.setOps, setOpSpec{kind: query.SetIntersect, other: other})
	return s
}

// Except keeps rows in this query that are not in the other. Chainable.
func (s *EntitySet) Except(other *EntitySet) *EntitySet {
	s.setOps = append(s.setOps, setOpSpec{kind: query.SetExcept, other: other})
	return s
}

// Distinct collapses duplicate rows in the result (SELECT DISTINCT). Most
// useful with Select, to dedupe a projected subset of columns. Chainable.
func (s *EntitySet) Distinct() *EntitySet {
	s.distinct = true
	return s
}

// Select restricts the query to a subset of columns instead of every mapped
// field, the equivalent of a projection. Names are field or column names; the
// type-safe dsl.FieldName(&u, &u.Email) resolves one at compile time.
// Chainable.
//
// Unselected fields stay at their zero value when scanned into the entity, so
// Select pairs naturally with a DTO destination (Set(&dtos).From("table")).
func (s *EntitySet) Select(fields ...string) *EntitySet {
	s.selectCols = append(s.selectCols, fields...)
	return s
}

// projectedColumns returns the Select column set augmented with columns a later
// step needs even if the caller projected them away: the primary key when the
// results are tracked (the identity map keys on it), and the primary key plus
// each eager-loaded relation's local key so Include can stitch children.
// Without this, projecting away a key silently yields untracked rows or empty
// relations. EF Core augments a projection that feeds tracking or Include the
// same way. Returns nil when there is no projection (every column is selected).
func (s *EntitySet) projectedColumns() []string {
	if len(s.selectCols) == 0 {
		return nil
	}
	cols := append([]string(nil), s.selectCols...)
	have := make(map[string]bool, len(cols))
	for _, c := range cols {
		have[canonicalColumn(s.meta, c)] = true
	}
	add := func(col string) {
		if col == "" || have[col] {
			return
		}
		cols = append(cols, col)
		have[col] = true
	}
	if s.tracking == trackOn || len(s.includes) > 0 {
		for _, k := range keyFields(s.meta) {
			add(k.Column)
		}
	}
	for _, name := range s.includes {
		if rel := s.meta.Relation(name); rel != nil {
			add(rel.LocalKey)
		}
	}
	return cols
}

// canonicalColumn resolves a field or column name to its storage column,
// matching how the compiler maps SELECT/ORDER BY names.
func canonicalColumn(meta *model.EntityMeta, name string) string {
	if f := meta.Field(name); f != nil {
		return f.Column
	}
	if f := meta.FieldByColumn(name); f != nil {
		return f.Column
	}
	return name
}

// SelectCase adds a CASE expression to the projection under an alias; the
// result is scanned into the destination field mapped to that column name
// (typically a DTO field). Build the expression with dsl.Case(). Chainable.
func (s *EntitySet) SelectCase(expr query.CaseExpr, alias string) *EntitySet {
	s.caseSelects = append(s.caseSelects, query.CaseSelect{Expr: expr, Alias: alias})
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

// RightJoin attaches a RIGHT JOIN to the underlying query. Supported on
// PostgreSQL, MySQL, SQL Server, and SQLite 3.39+.
func (s *EntitySet) RightJoin(entity string, on query.Node) *EntitySet {
	s.joins = append(s.joins, query.JoinSpec{Type: query.JoinRight, Entity: entity, On: on})
	return s
}

// FullJoin attaches a FULL OUTER JOIN to the underlying query. Supported on
// PostgreSQL, SQL Server, and SQLite 3.39+; MySQL has no FULL JOIN.
func (s *EntitySet) FullJoin(entity string, on query.Node) *EntitySet {
	s.joins = append(s.joins, query.JoinSpec{Type: query.JoinFull, Entity: entity, On: on})
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
	// Validate the destination shape before any query runs, so an AsTracking
	// misuse fails fast instead of after the main query and every Include.
	if s.tracking == trackOn {
		if err := s.checkTrackableDest(); err != nil {
			return err
		}
	}

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

	if s.tracking == trackOn {
		s.attachResults()
	}
	return nil
}

// checkTrackableDest verifies AsTracking has a *[]*T destination. The tracker
// keys entities by address, and only a slice of pointers gives each element a
// stable one (a *[]T backing array can move on append/reslice).
func (s *EntitySet) checkTrackableDest() error {
	v := reflect.ValueOf(s.dest)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("AsTracking requires a slice destination, got %s", v.Type())
	}
	if v.Elem().Type().Elem().Kind() != reflect.Ptr {
		return fmt.Errorf("AsTracking requires *[]*T (a slice of pointers) so tracked entities have stable addresses, got %s", v.Type())
	}
	return nil
}

// attachResults tracks each element of a collection result as Unchanged, so a
// later Save persists mutations (AsTracking on All). The destination shape was
// already validated by checkTrackableDest.
//
// When a row is already tracked (same primary key, e.g. loaded by an earlier
// Find), the identity map wins: the slice element is replaced with the tracked
// instance rather than re-attached. That mirrors EF Core and avoids clobbering a
// pending change with a fresh snapshot, which would silently drop the mutation.
func (s *EntitySet) attachResults() {
	slice := reflect.ValueOf(s.dest).Elem()
	for i := 0; i < slice.Len(); i++ {
		elem := slice.Index(i)
		if elem.IsNil() {
			continue
		}
		ptr := elem.Interface()
		if existing, ok := s.ctx.tracker.Entry(ptr); ok {
			elem.Set(reflect.ValueOf(existing.Entity))
			continue
		}
		s.ctx.tracker.Attach(ptr)
	}
}

// Stream executes the query and yields one entity at a time instead of loading
// the whole result set, for iterating large reads without buffering every row.
// Each yielded value is a *T (pointer to the entity type); range with the
// standard two-value form and stop by breaking:
//
//	for v, err := range ctx.Set(&User{}).Where(...).Stream(ctx) {
//	    if err != nil {
//	        return err
//	    }
//	    u := v.(*User)
//	    // ...
//	}
//
// Use the generic Stream[T] helper to get *T directly without the assertion.
//
// Registered query filters apply, exactly as for All. Streamed entities are not
// change-tracked (tracking would rebuild the buffer streaming exists to avoid).
// The read is not retried and does not pass through the circuit breaker: both
// wrap a single buffered call, but a stream is paced by the consumer, so
// retrying or breaker-counting a half-consumed iteration is meaningless.
// Include, GroupBy, and Aggregate are not supported with streaming and yield an
// error. Requires a provider that implements provider.StreamExecutor.
func (s *EntitySet) Stream(ctx stdctx.Context) iter.Seq2[any, error] {
	if ctx == nil {
		ctx = s.ctx.opCtx()
	}
	return func(yield func(any, error) bool) {
		if err := s.checkStreamable(); err != nil {
			yield(nil, err)
			return
		}
		se, ok := s.ctx.provider.(provider.StreamExecutor)
		if !ok {
			yield(nil, fmt.Errorf("provider %q does not support streaming (StreamExecutor)", s.ctx.provider.Name()))
			return
		}

		q := s.buildQuery()
		// ExecuteStream returns nil the moment yield reports stop, so a consumer
		// break never reaches the post-loop error yield below.
		err := se.ExecuteStream(ctx, s.meta, q, s.meta.GoType, func(e any) bool {
			return yield(e, nil)
		})
		if err != nil {
			yield(nil, err)
		}
	}
}

// checkStreamable rejects query shapes that cannot stream row by row: Include
// batches a separate query over the full result set, and GroupBy/Aggregate
// collapse rows server-side into a shape that is not the entity type.
func (s *EntitySet) checkStreamable() error {
	switch {
	case len(s.includes) > 0:
		return fmt.Errorf("Stream does not support Include (it batches a second query over all results); use All")
	case len(s.groupBy) > 0 || len(s.aggregates) > 0:
		return fmt.Errorf("Stream does not support GroupBy/Aggregate; use All")
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
	if s.distinct {
		b.Distinct()
	}
	if cols := s.projectedColumns(); len(cols) > 0 {
		b.Select(cols...)
	}
	for _, cs := range s.caseSelects {
		b.SelectCase(cs.Expr, cs.Alias)
	}
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
		case query.JoinRight:
			b.RightJoin(j.Entity, j.On)
		case query.JoinFull:
			b.FullJoin(j.Entity, j.On)
		default:
			b.Join(j.Entity, j.On)
		}
	}
	for _, so := range s.setOps {
		operand := so.other.buildQuery()
		switch so.kind {
		case query.SetUnionAll:
			b.UnionAll(operand)
		case query.SetIntersect:
			b.Intersect(operand)
		case query.SetExcept:
			b.Except(operand)
		default:
			b.Union(operand)
		}
	}
	return b.Build()
}
