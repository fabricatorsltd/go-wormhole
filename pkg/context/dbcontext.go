package context

import (
	stdctx "context"
	"fmt"
	"reflect"
	"time"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	"github.com/fabricatorsltd/go-wormhole/pkg/tracker"
	"github.com/mirkobrombin/go-foundation/pkg/errutil"
	"github.com/mirkobrombin/go-foundation/pkg/hooks"
	"github.com/mirkobrombin/go-foundation/pkg/resiliency"
)

// DbContext is the Unit of Work entry point. It manages the lifecycle
// of a session: tracking entities, detecting changes, and flushing
// them through the underlying Provider inside a transaction.
type DbContext struct {
	provider     provider.Provider
	tracker      *tracker.Tracker
	hooks        *hooks.Runner
	retry        []func(*resiliency.RetryOptions)
	readRetry    []func(*resiliency.RetryOptions)
	breaker      *resiliency.CircuitBreaker
	stdCtx       stdctx.Context
	queryFilters map[string][]query.Node
	noTracking   bool
}

// Option configures a DbContext.
type Option func(*DbContext)

// WithRetry adds retry options for transactional commits.
func WithRetry(opts ...func(*resiliency.RetryOptions)) Option {
	return func(c *DbContext) {
		c.retry = opts
	}
}

// WithReadRetry adds retry options for read operations (Find, Execute).
func WithReadRetry(opts ...func(*resiliency.RetryOptions)) Option {
	return func(c *DbContext) {
		c.readRetry = opts
	}
}

// WithCircuitBreaker enables a circuit breaker for all provider calls.
// The breaker opens after `threshold` consecutive failures and stays
// open for `timeout` before entering half-open state.
func WithCircuitBreaker(threshold int, timeout time.Duration) Option {
	return func(c *DbContext) {
		c.breaker = resiliency.NewCircuitBreaker(threshold, timeout)
	}
}

// WithContext sets a default context for operations that don't receive
// an explicit context (e.g. EntitySet.Find, Save).
func WithContext(ctx stdctx.Context) Option {
	return func(c *DbContext) {
		c.stdCtx = ctx
	}
}

// WithNoTracking makes single-entity reads (Find) skip the change tracker by
// default, the equivalent of EF Core's QueryTrackingBehavior.NoTracking. Reads
// then return detached entities that Save will not persist; a query can opt back
// in per call with EntitySet.AsTracking. Collection queries (All) are already
// non-tracking regardless of this setting; use AsTracking to track them.
func WithNoTracking() Option {
	return func(c *DbContext) {
		c.noTracking = true
	}
}

// New creates a DbContext bound to the given Provider.
// When built with -tags wormhole_cli, execution is intercepted here and the
// wormhole CLI runs against the current project before this function returns.
func New(p provider.Provider, opts ...Option) *DbContext {
	c := &DbContext{
		provider: p,
		tracker:  tracker.New(),
		hooks:    hooks.NewRunner(),
	}
	for _, o := range opts {
		o(c)
	}
	c.runCLIIfEnabled()
	return c
}

// NewDefault creates a DbContext using the default registered provider.
func NewDefault(opts ...Option) *DbContext {
	return New(provider.Default(), opts...)
}

// --- Entity operations ---

// Add marks entities for insertion.
func (c *DbContext) Add(entities ...any) {
	for _, e := range entities {
		c.tracker.Add(e)
	}
}

// Attach starts tracking entities as Unchanged.
func (c *DbContext) Attach(entities ...any) {
	for _, e := range entities {
		c.tracker.Attach(e)
	}
}

// Remove marks entities for deletion.
func (c *DbContext) Remove(entities ...any) {
	for _, e := range entities {
		c.tracker.Remove(e)
	}
}

// Detach stops tracking entities.
func (c *DbContext) Detach(entities ...any) {
	for _, e := range entities {
		c.tracker.Detach(e)
	}
}

// Entry returns the tracking entry for an entity.
func (c *DbContext) Entry(entity any) (*model.Entry, bool) {
	return c.tracker.Entry(entity)
}

// AddQueryFilter registers a predicate that is automatically applied (ANDed
// into the WHERE clause) to every query of the sample's entity type: All,
// Find, bulk Update/Delete, and eager-loaded relations of that type. The
// entity is identified from sample (a zero-value struct pointer is fine).
//
// This is the basis for soft-delete and multi-tenant isolation. Build the
// predicate with the dsl for type safety; the value is captured at
// registration, which fits the per-unit-of-work lifetime of a DbContext:
//
//	u := &User{}
//	ctx.AddQueryFilter(&User{}, dsl.Eq(u, &u.TenantID, currentTenant))
//
// Use EntitySet.IgnoreFilters to run a query without the registered filters.
//
// Filters are keyed by entity name (snake_case of the Go type, ignoring the
// package path), the same convention the rest of the ORM uses, so two distinct
// types that share a name also share filters.
func (c *DbContext) AddQueryFilter(sample any, filter query.Node) {
	if filter == nil {
		return
	}
	meta := schema.Parse(sample)
	if c.queryFilters == nil {
		c.queryFilters = make(map[string][]query.Node)
	}
	c.queryFilters[meta.Name] = append(c.queryFilters[meta.Name], filter)
}

// filtersFor returns the registered query filters for an entity name.
func (c *DbContext) filtersFor(entity string) []query.Node {
	return c.queryFilters[entity]
}

// Transaction runs fn inside a single provider transaction, committing on
// success and rolling back if fn returns an error. The provider.Tx passed
// to fn exposes the entity operations (Insert/Update/Delete/Find/Execute);
// a Tx from a SQL provider additionally satisfies the raw-SQL escape hatch
// (type-assert to wormholesql.TxRunner) so hand-written SQL — e.g.
// SELECT ... FOR UPDATE — runs atomically alongside ORM operations within
// the same transaction. Changes tracked on the DbContext via Add/Save are
// independent of this call and are not flushed here.
func (c *DbContext) Transaction(ctx stdctx.Context, fn func(tx provider.Tx) error) error {
	if ctx == nil {
		ctx = c.opCtx()
	}
	tx, err := c.provider.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// Upsert performs an insert-or-update for entity in a single statement
// using the provider's ON CONFLICT support. It requires the provider to
// implement provider.Upserter (the SQL provider does); otherwise it errors.
// conflict.Columns is the unique/PK target; an empty conflict.Update means
// "leave the existing row" (DO NOTHING).
func (c *DbContext) Upsert(ctx stdctx.Context, entity any, conflict provider.ConflictClause) error {
	if ctx == nil {
		ctx = c.opCtx()
	}
	up, ok := c.provider.(provider.Upserter)
	if !ok {
		return fmt.Errorf("provider %q does not support upsert", c.provider.Name())
	}
	meta := schema.Parse(entity)
	_, err := up.Upsert(ctx, meta, entity, conflict)
	return err
}

// --- Query ---

// Query returns a new QueryBuilder targeting the named entity.
func (c *DbContext) Query(entity string) *query.Builder {
	return query.From(entity)
}

// --- Hooks ---

// Before registers a pre-event hook (e.g. "save", "insert", "delete").
func (c *DbContext) Before(event string, fn hooks.HookFunc) {
	c.hooks.Before(event, fn)
}

// After registers a post-event hook.
func (c *DbContext) After(event string, fn hooks.HookFunc) {
	c.hooks.After(event, fn)
}

// Save is a convenience wrapper for SaveChanges using the stored context.
func (c *DbContext) Save() error {
	return c.SaveChanges(c.opCtx())
}

// --- Persistence ---

// SaveChanges detects modifications, runs lifecycle hooks, and
// flushes all pending changes through the Provider in a single
// transaction. Returns a MultiError if multiple entities fail
// validation or persistence.
func (c *DbContext) SaveChanges(ctx stdctx.Context) error {
	c.tracker.DetectChanges()

	pending := c.tracker.Pending()
	if len(pending) == 0 {
		return nil
	}

	// Run "before" lifecycle methods discovered on entities.
	disc := hooks.NewDiscovery()
	var me errutil.MultiError
	for _, e := range pending {
		for _, m := range disc.Discover(e.Entity, "Before") {
			results, err := disc.CallWithContext(ctx, e.Entity, m.Name)
			if err != nil {
				me.Append(fmt.Errorf("%s.%s: %w", e.Meta.Name, m.Name, err))
			}
			for _, r := range results {
				if rerr, ok := r.(error); ok && rerr != nil {
					me.Append(fmt.Errorf("%s.%s: %w", e.Meta.Name, m.Name, rerr))
				}
			}
		}
	}
	if me.HasErrors() {
		return me.ErrorOrNil()
	}

	// Run registered "save" hook.
	hookErr := c.hooks.Run(ctx, "save", func() error {
		return c.flush(ctx, pending)
	})
	if hookErr != nil {
		return hookErr
	}

	// Run "after" lifecycle methods.
	for _, e := range pending {
		for _, m := range disc.Discover(e.Entity, "After") {
			if _, err := disc.CallWithContext(ctx, e.Entity, m.Name); err != nil {
				me.Append(err)
			}
		}
	}

	c.tracker.AcceptAll()
	return me.ErrorOrNil()
}

// flush executes the pending operations inside a transaction,
// optionally wrapped in a retry policy and circuit breaker.
func (c *DbContext) flush(ctx stdctx.Context, pending []*model.Entry) error {
	// Reject composite-PK entities on a provider that cannot key on them before
	// opening a transaction.
	for _, e := range pending {
		if err := c.requireKeySupport(e.Meta); err != nil {
			return err
		}
		if err := c.requireRelationKeySupport(e.Meta); err != nil {
			return err
		}
	}

	// Order inserts so that a new parent is written before any new child that
	// references it, and fix up foreign-key values from the in-memory object
	// graph. With no relationships this is the original order, untouched.
	order, err := planInsertOrder(pending)
	if err != nil {
		return err
	}

	commit := func() error {
		tx, err := c.provider.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}

		bi, batchable := tx.(provider.BatchInserter)

		for i := 0; i < len(order); {
			e := pending[order[i]]

			// Group a contiguous run of same-type client-PK inserts into one
			// multi-row INSERT when the provider supports it. Auto-increment PKs
			// are excluded (isBatchableInsert): they need per-row generated-key
			// write-back, which a multi-row INSERT cannot deliver reliably.
			if batchable && isBatchableInsert(e) {
				j := i + 1
				for j < len(order) {
					ej := pending[order[j]]
					if ej.Meta != e.Meta || !isBatchableInsert(ej) {
						break
					}
					j++
				}
				if j-i > 1 {
					run := order[i:j]
					// Both fixup passes run BEFORE the batch: every batched entity
					// has a client-assigned PK (isBatchableInsert excludes
					// auto-increment), so a parent's PK is already known and its
					// child's FK column can be filled before the rows are written.
					// The per-row path only defers fixupChildFKs to capture a
					// generated key, which by construction never exists here.
					for _, idx := range run {
						c.fixupBelongsToFKs(pending[idx])
						c.fixupChildFKs(pending[idx])
					}
					entities := make([]any, len(run))
					for k, idx := range run {
						entities[k] = pending[idx].Entity
					}
					if err := bi.InsertBatch(ctx, e.Meta, entities); err != nil {
						_ = tx.Rollback()
						return err
					}
					i = j
					continue
				}
			}

			if e.State == model.Added {
				c.fixupBelongsToFKs(e)
			}
			if err := c.applyEntry(ctx, tx, e); err != nil {
				_ = tx.Rollback()
				return err
			}
			if e.State == model.Added {
				c.fixupChildFKs(e)
			}
			i++
		}

		return tx.Commit()
	}

	if c.breaker != nil {
		orig := commit
		commit = func() error {
			return c.breaker.Execute(orig)
		}
	}

	var runErr error
	if len(c.retry) > 0 {
		runErr = resiliency.Retry(ctx, commit, c.retry...)
	} else {
		runErr = commit()
	}
	if runErr != nil {
		return runErr
	}

	// The transaction committed: advance in-memory version tokens to match the
	// server-side bump, so the upcoming AcceptAll re-snapshots the new value.
	// Done here (post-commit) rather than inside the provider so a rollback can
	// never leave an entity ahead of the database.
	bumpVersionTokens(pending)
	return nil
}

// isBatchableInsert reports whether an entry can join a multi-row INSERT
// batch. It must be a pending insert with no auto-increment primary key, so no
// per-row generated-key write-back is needed (a multi-row INSERT cannot map
// generated keys back to individual entities reliably). Client-assigned PKs and
// keyless rows (e.g. join tables) qualify.
func isBatchableInsert(e *model.Entry) bool {
	return e.State == model.Added &&
		(e.Meta.PrimaryKey == nil || !e.Meta.PrimaryKey.AutoIncr)
}

func (c *DbContext) applyEntry(ctx stdctx.Context, tx provider.Tx, e *model.Entry) error {
	switch e.State {
	case model.Added:
		id, err := tx.Insert(ctx, e.Meta, e.Entity)
		if err == nil {
			// Write the generated PK back onto the in-memory entity so
			// callers can use it immediately after Save(). Without this
			// the auto-increment value goes into the database but the
			// caller still sees the zero value, which breaks any flow
			// that needs to reference the just-inserted row in the same
			// request (link tables, return-id-to-FE, cascading writes).
			assignAutoIncrPK(e, id)
		}
		return err
	case model.Modified:
		changed := tracker.ChangedFields(e)
		return tx.Update(ctx, e.Meta, e.Entity, changed)
	case model.Deleted:
		pk := c.pkValue(e)
		// Optimistic concurrency: guard the delete on the loaded version so a
		// row changed by another transaction is not silently removed.
		if e.Meta.Version != nil {
			if vd, ok := tx.(provider.VersionedDeleter); ok {
				rows, err := vd.DeleteVersioned(ctx, e.Meta, pk, c.versionValue(e))
				if err != nil {
					return err
				}
				if rows == 0 {
					return provider.ErrConcurrencyConflict
				}
				return nil
			}
		}
		return tx.Delete(ctx, e.Meta, pk)
	default:
		return nil
	}
}

// assignAutoIncrPK writes `id` onto the entity's auto-increment PK
// field, using reflection so we can adapt to whatever integer width the
// model declared (int16 / int32 / int64 / uint*). It is a no-op when
// the model has no auto-increment PK, when the id is nil, or when the
// PK field is not addressable (e.g. caller passed a non-pointer).
//
// Driver providers return `id` as any — typically int64 for SQL drivers
// that surface LastInsertId, or whatever scan target the RETURNING path
// chose. We funnel everything through reflect.Value.Convert so callers
// don't have to care.
func assignAutoIncrPK(e *model.Entry, id any) {
	if id == nil || e.Meta.PrimaryKey == nil || !e.Meta.PrimaryKey.AutoIncr {
		return
	}
	val := reflect.ValueOf(e.Entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if !val.IsValid() || val.Kind() != reflect.Struct {
		return
	}
	field := val.FieldByName(e.Meta.PrimaryKey.FieldName)
	if !field.IsValid() || !field.CanSet() {
		return
	}
	src := reflect.ValueOf(id)
	if !src.IsValid() {
		return
	}
	if src.Type().ConvertibleTo(field.Type()) {
		field.Set(src.Convert(field.Type()))
	}
}

// pkValue returns the entity's loaded primary key from its snapshot: a scalar
// for a single-column key, or a []any tuple (in column order) for a composite
// key. The composite form is understood by the compiler's key WHERE builder.
func (c *DbContext) pkValue(e *model.Entry) any {
	keys := keyFields(e.Meta)
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		return e.Snapshot[keys[0].FieldName]
	}
	vals := make([]any, len(keys))
	for i, k := range keys {
		vals[i] = e.Snapshot[k.FieldName]
	}
	return vals
}

// keyFields returns the entity's primary-key fields, preferring the composite
// PrimaryKeys list and falling back to the singular PrimaryKey shortcut.
func keyFields(meta *model.EntityMeta) []*model.FieldMeta {
	if len(meta.PrimaryKeys) > 0 {
		return meta.PrimaryKeys
	}
	if meta.PrimaryKey != nil {
		return []*model.FieldMeta{meta.PrimaryKey}
	}
	return nil
}

// requireKeySupport rejects a composite-PK entity on a provider that cannot key
// on multiple columns, so it fails with a clear error instead of silently
// matching on the first column.
func (c *DbContext) requireKeySupport(meta *model.EntityMeta) error {
	if len(keyFields(meta)) <= 1 {
		return nil
	}
	if ck, ok := c.provider.(provider.CompositeKeyer); ok && ck.CompositeKeysSupported() {
		return nil
	}
	return fmt.Errorf("entity %q has a composite primary key, which provider %q does not support", meta.Name, c.provider.Name())
}

// requireRelationKeySupport rejects a relation that targets a composite-PK
// entity. Relation foreign keys are single columns (Relation.LocalKey /
// ForeignKey), so a composite target cannot yet be linked without silently
// writing only its first key column. Composite-PK entities support standalone
// CRUD; being the target of a navigation is a later slice.
func (c *DbContext) requireRelationKeySupport(meta *model.EntityMeta) error {
	for _, rel := range meta.Relations {
		if rel.Target == nil {
			continue
		}
		target := schema.ParseType(rel.Target)
		if len(keyFields(target)) > 1 {
			return fmt.Errorf("entity %q relation %q targets %q, which has a composite primary key; relations to composite-key entities are not yet supported", meta.Name, rel.Field, target.Name)
		}
	}
	return nil
}

// versionValue returns the entity's loaded optimistic-concurrency version from
// the snapshot, used to guard a delete.
func (c *DbContext) versionValue(e *model.Entry) any {
	if e.Meta.Version == nil {
		return nil
	}
	if v, ok := e.Snapshot[e.Meta.Version.FieldName]; ok {
		return v
	}
	return nil
}

// Close releases the underlying provider connection.
func (c *DbContext) Close() error {
	c.tracker.Clear()
	return c.provider.Close()
}

// PendingChanges describes a single pending operation for preview.
type PendingChange struct {
	Table     string
	Operation string // "INSERT", "UPDATE", "DELETE"
	SQL       string
	Params    []any
}

// PendingSQL detects changes exactly like SaveChanges but returns the
// compiled SQL statements without executing them. Useful for debugging
// what SaveChanges would do. Returns an error if the provider does not
// implement QueryExplainer.
func (c *DbContext) PendingSQL() ([]PendingChange, error) {
	exp, ok := c.provider.(provider.QueryExplainer)
	if !ok {
		return nil, fmt.Errorf("provider %q does not support PendingSQL (QueryExplainer)", c.provider.Name())
	}

	c.tracker.DetectChanges()
	pending := c.tracker.Pending()
	if len(pending) == 0 {
		return nil, nil
	}

	out := make([]PendingChange, 0, len(pending))
	for _, e := range pending {
		var pc PendingChange
		pc.Table = e.Meta.Name

		switch e.State {
		case model.Added:
			pc.Operation = "INSERT"
			cq, err := exp.ExplainInsert(e.Meta, e.Entity)
			if err != nil {
				return nil, err
			}
			pc.SQL = cq.SQL
			pc.Params = cq.Params

		case model.Modified:
			pc.Operation = "UPDATE"
			changed := tracker.ChangedFields(e)
			cq, err := exp.ExplainUpdate(e.Meta, e.Entity, changed)
			if err != nil {
				return nil, err
			}
			pc.SQL = cq.SQL
			pc.Params = cq.Params

		case model.Deleted:
			pc.Operation = "DELETE"
			pk := c.pkValue(e)
			cq, err := exp.ExplainDelete(e.Meta, pk)
			if err != nil {
				return nil, err
			}
			pc.SQL = cq.SQL
			pc.Params = cq.Params

		default:
			continue
		}

		out = append(out, pc)
	}
	return out, nil
}

// --- internal helpers ---

// opCtx returns the stored context or Background if none was set.
func (c *DbContext) opCtx() stdctx.Context {
	if c.stdCtx != nil {
		return c.stdCtx
	}
	return stdctx.Background()
}

// withReadResilience wraps a read operation with circuit breaker and retry.
func (c *DbContext) withReadResilience(ctx stdctx.Context, fn func() error) error {
	wrapped := fn
	if c.breaker != nil {
		orig := wrapped
		wrapped = func() error {
			return c.breaker.Execute(orig)
		}
	}
	if len(c.readRetry) > 0 {
		return resiliency.Retry(ctx, wrapped, c.readRetry...)
	}
	return wrapped()
}
