package provider

import (
	"context"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
)

// Provider is the contract every storage backend must implement.
// It translates the provider-neutral AST and entity operations
// into native calls (SQL statements, key-value puts, etc.).
type Provider interface {
	// Name returns a unique identifier (e.g. "postgres", "slipstream").
	Name() string

	// Open initialises the connection / engine.
	Open(ctx context.Context, dsn string) error

	// Close releases resources.
	Close() error

	// --- CRUD ---

	// Insert persists a new entity and returns the generated key (if any).
	Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error)

	// Update applies partial changes to an existing entity.
	// Only the fields listed in `changed` are written.
	Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error

	// Delete removes an entity identified by its primary key value.
	Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error

	// Find retrieves a single entity by primary key.
	Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error

	// --- Query ---

	// Execute runs a query AST and scans results into dest (pointer to slice).
	Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error

	// --- Transactions ---

	// Begin starts a provider-level transaction.
	Begin(ctx context.Context) (Tx, error)
}

// Tx represents an in-flight transaction on a Provider.
type Tx interface {
	Commit() error
	Rollback() error

	Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error)
	Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error
	Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error
	Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error
	Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error
}

// CompiledQuery holds a pre-compiled query for inspection without execution.
type CompiledQuery struct {
	SQL    string
	Params []any
}

// QueryExplainer is an optional interface a Provider may implement to
// expose compiled queries without executing them. Useful for debugging.
type QueryExplainer interface {
	ExplainSelect(meta *model.EntityMeta, q query.Query) CompiledQuery
	ExplainFindByPK(meta *model.EntityMeta, pkValue any) CompiledQuery
	ExplainInsert(meta *model.EntityMeta, entity any) CompiledQuery
	ExplainUpdate(meta *model.EntityMeta, entity any, changed []string) CompiledQuery
	ExplainDelete(meta *model.EntityMeta, pkValue any) CompiledQuery
}
