package provider

import (
	"context"
	"fmt"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
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
	ExplainSelect(meta *model.EntityMeta, q query.Query) (CompiledQuery, error)
	ExplainFindByPK(meta *model.EntityMeta, pkValue any) (CompiledQuery, error)
	ExplainInsert(meta *model.EntityMeta, entity any) (CompiledQuery, error)
	ExplainUpdate(meta *model.EntityMeta, entity any, changed []string) (CompiledQuery, error)
	ExplainDelete(meta *model.EntityMeta, pkValue any) (CompiledQuery, error)
}

// Capabilities describes what a backend can do natively.
// Providers can expose these flags via CapabilityReporter.
type Capabilities struct {
	Transactions     bool
	Aggregations     bool
	NestedFilters    bool
	PartialUpdate    bool
	Sorting          bool
	OffsetPagination bool
	CursorPagination bool
	SchemaMigrations bool
	SchemaEvolution  bool
}

// CapabilityReporter is an optional interface for providers that
// publish backend capabilities.
type CapabilityReporter interface {
	Capabilities() Capabilities
}

// DetectCapabilities returns provider capabilities when exposed.
// If unsupported, it returns the zero-value capability set.
func DetectCapabilities(p Provider) Capabilities {
	if c, ok := p.(CapabilityReporter); ok {
		return c.Capabilities()
	}
	return Capabilities{}
}

// ValidateQueryCapabilities verifies whether the query shape is supported
// by the provided capability set and returns a normalized query shape.
func ValidateQueryCapabilities(meta *model.EntityMeta, c Capabilities, q query.Query) (query.Query, error) {
	if len(q.OrderBy) > 0 && !c.Sorting {
		return q, fmt.Errorf("provider does not support sorting")
	}
	if q.Offset > 0 && !c.OffsetPagination {
		return q, fmt.Errorf("provider does not support offset pagination")
	}
	if len(q.Includes) > 0 && !c.Aggregations {
		return q, fmt.Errorf("provider does not support relation includes")
	}
	if (len(q.GroupBy) > 0 || len(q.Aggregates) > 0 || q.Having != nil) && !c.Aggregations {
		return q, fmt.Errorf("provider does not support aggregations")
	}
	if len(q.GroupBy) == 0 && len(q.Aggregates) == 0 && q.Having == nil {
		return q, nil
	}
	return normalizeAggregateQuery(meta, q)
}

func normalizeAggregateQuery(meta *model.EntityMeta, q query.Query) (query.Query, error) {
	if len(q.GroupBy) > 0 && len(q.Aggregates) == 0 {
		return q, fmt.Errorf("GROUP BY requires at least one aggregate")
	}
	if q.Having != nil && len(q.Aggregates) == 0 {
		return q, fmt.Errorf("HAVING requires at least one aggregate")
	}

	sourceMeta, err := aggregateSourceMeta(meta, q)
	if err != nil {
		return q, err
	}

	groupedColumns := make(map[string]struct{}, len(q.GroupBy))
	for i, field := range q.GroupBy {
		col, ok := resolveFieldColumn(sourceMeta, field)
		if !ok {
			return q, fmt.Errorf("GROUP BY field %q does not exist on %q", field, sourceMeta.Name)
		}
		q.GroupBy[i] = col
		groupedColumns[col] = struct{}{}
	}

	aggregateAliases := make(map[string]struct{}, len(q.Aggregates))
	for i, agg := range q.Aggregates {
		if agg.Field == "" || agg.Field == "*" {
			if agg.Func != query.AggCount {
				return q, fmt.Errorf("%s requires a field reference", aggregateFuncName(agg.Func))
			}
			q.Aggregates[i].Field = "*"
		} else {
			col, ok := resolveFieldColumn(sourceMeta, agg.Field)
			if !ok {
				return q, fmt.Errorf("aggregate field %q does not exist on %q", agg.Field, sourceMeta.Name)
			}
			q.Aggregates[i].Field = col
		}

		if agg.Alias != "" {
			if _, exists := aggregateAliases[agg.Alias]; exists {
				return q, fmt.Errorf("duplicate aggregate alias %q", agg.Alias)
			}
			aggregateAliases[agg.Alias] = struct{}{}
		}
	}

	if q.Having != nil {
		q.Having, err = normalizeAggregateNode(q.Having, sourceMeta, groupedColumns, aggregateAliases)
		if err != nil {
			return q, err
		}
	}

	for i, sort := range q.OrderBy {
		if _, ok := aggregateAliases[sort.Field]; ok {
			continue
		}
		col, ok := resolveFieldColumn(sourceMeta, sort.Field)
		if !ok {
			return q, fmt.Errorf("ORDER BY field %q does not exist on %q and is not an aggregate alias", sort.Field, sourceMeta.Name)
		}
		if _, grouped := groupedColumns[col]; !grouped {
			return q, fmt.Errorf("ORDER BY field %q must reference a GROUP BY field or aggregate alias", sort.Field)
		}
		q.OrderBy[i].Field = col
	}

	return q, nil
}

func aggregateSourceMeta(meta *model.EntityMeta, q query.Query) (*model.EntityMeta, error) {
	if q.EntityName != "" {
		if sourceMeta := schema.LookupEntity(q.EntityName); sourceMeta != nil {
			return sourceMeta, nil
		}
		if meta == nil || meta.Name != q.EntityName {
			return nil, fmt.Errorf("aggregate query entity %q is not registered in schema metadata", q.EntityName)
		}
	}
	if meta == nil {
		return nil, fmt.Errorf("aggregate query requires entity metadata")
	}
	return meta, nil
}

func normalizeAggregateNode(
	node query.Node,
	sourceMeta *model.EntityMeta,
	groupedColumns map[string]struct{},
	aggregateAliases map[string]struct{},
) (query.Node, error) {
	switch n := node.(type) {
	case query.Predicate:
		if _, ok := aggregateAliases[n.Field]; ok {
			return n, nil
		}
		col, ok := resolveFieldColumn(sourceMeta, n.Field)
		if !ok {
			return nil, fmt.Errorf("HAVING field %q does not exist on %q and is not an aggregate alias", n.Field, sourceMeta.Name)
		}
		if _, grouped := groupedColumns[col]; !grouped {
			return nil, fmt.Errorf("HAVING field %q must reference a GROUP BY field or aggregate alias", n.Field)
		}
		n.Field = col
		return n, nil
	case query.Composite:
		children := make([]query.Node, len(n.Children))
		for i, child := range n.Children {
			normalized, err := normalizeAggregateNode(child, sourceMeta, groupedColumns, aggregateAliases)
			if err != nil {
				return nil, err
			}
			children[i] = normalized
		}
		n.Children = children
		return n, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate predicate type %T", node)
	}
}

func resolveFieldColumn(meta *model.EntityMeta, name string) (string, bool) {
	if meta == nil {
		return "", false
	}
	if field := meta.Field(name); field != nil {
		return field.Column, true
	}
	if field := meta.FieldByColumn(name); field != nil {
		return field.Column, true
	}
	return "", false
}

func aggregateFuncName(fn query.AggFunc) string {
	switch fn {
	case query.AggSum:
		return "SUM"
	case query.AggAvg:
		return "AVG"
	case query.AggMin:
		return "MIN"
	case query.AggMax:
		return "MAX"
	default:
		return "COUNT"
	}
}
