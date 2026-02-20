package context

import (
	stdctx "context"
	"fmt"

	"github.com/mirkobrombin/go-foundation/pkg/errors"
	"github.com/mirkobrombin/go-foundation/pkg/hooks"
	"github.com/mirkobrombin/go-foundation/pkg/resiliency"
	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/provider"
	"github.com/mirkobrombin/go-wormhole/pkg/query"
	"github.com/mirkobrombin/go-wormhole/pkg/tracker"
)

// DbContext is the Unit of Work entry point. It manages the lifecycle
// of a session: tracking entities, detecting changes, and flushing
// them through the underlying Provider inside a transaction.
type DbContext struct {
	provider provider.Provider
	tracker  *tracker.Tracker
	hooks    *hooks.Runner
	retry    []func(*resiliency.RetryOptions)
}

// Option configures a DbContext.
type Option func(*DbContext)

// WithRetry adds retry options for transactional commits.
func WithRetry(opts ...func(*resiliency.RetryOptions)) Option {
	return func(c *DbContext) {
		c.retry = opts
	}
}

// New creates a DbContext bound to the given Provider.
func New(p provider.Provider, opts ...Option) *DbContext {
	c := &DbContext{
		provider: p,
		tracker:  tracker.New(),
		hooks:    hooks.NewRunner(),
	}
	for _, o := range opts {
		o(c)
	}
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
	var me errors.MultiError
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
// optionally wrapped in a retry policy.
func (c *DbContext) flush(ctx stdctx.Context, pending []*model.Entry) error {
	commit := func() error {
		tx, err := c.provider.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}

		for _, e := range pending {
			if err := c.applyEntry(ctx, tx, e); err != nil {
				_ = tx.Rollback()
				return err
			}
		}

		return tx.Commit()
	}

	if len(c.retry) > 0 {
		return resiliency.Retry(ctx, commit, c.retry...)
	}
	return commit()
}

func (c *DbContext) applyEntry(ctx stdctx.Context, tx provider.Tx, e *model.Entry) error {
	switch e.State {
	case model.Added:
		_, err := tx.Insert(ctx, e.Meta, e.Entity)
		return err
	case model.Modified:
		changed := tracker.ChangedFields(e)
		return tx.Update(ctx, e.Meta, e.Entity, changed)
	case model.Deleted:
		pk := c.pkValue(e)
		return tx.Delete(ctx, e.Meta, pk)
	default:
		return nil
	}
}

func (c *DbContext) pkValue(e *model.Entry) any {
	if e.Meta.PrimaryKey == nil {
		return nil
	}
	if snap, ok := e.Snapshot[e.Meta.PrimaryKey.FieldName]; ok {
		return snap
	}
	return nil
}

// Close releases the underlying provider connection.
func (c *DbContext) Close() error {
	c.tracker.Clear()
	return c.provider.Close()
}
