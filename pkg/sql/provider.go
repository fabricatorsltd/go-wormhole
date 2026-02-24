package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/mirkobrombin/go-foundation/pkg/resiliency"
)

// QueryLogger is called before every SQL execution with the compiled
// query and its parameters.
type QueryLogger func(sql string, params []any)

// Provider implements provider.Provider for any database/sql-compatible driver.
type Provider struct {
	db       *sql.DB
	compiler *Compiler
	name     string
	retry    []func(*resiliency.RetryOptions)
	logger   QueryLogger
	rawLog   bool
}

var _ provider.Provider = (*Provider)(nil)
var _ provider.QueryExplainer = (*Provider)(nil)

// Option configures the SQL Provider.
type Option func(*Provider)

// WithNumberedParams enables $1, $2 style placeholders (Postgres).
func WithNumberedParams() Option {
	return func(p *Provider) { p.compiler.Numbered = true }
}

// WithName overrides the provider name (default: "sql").
func WithName(n string) Option {
	return func(p *Provider) { p.name = n }
}

// WithRetry enables automatic retry with exponential backoff for
// transient database errors (e.g. "driver: bad connection").
func WithRetry(opts ...func(*resiliency.RetryOptions)) Option {
	return func(p *Provider) { p.retry = opts }
}

// WithQueryLogger attaches a logger invoked before every SQL execution.
// Parameters are redacted to "[REDACTED]" to prevent accidental leaks
// of sensitive data (passwords, tokens, PII) in log output.
func WithQueryLogger(fn QueryLogger) Option {
	return func(p *Provider) { p.logger = fn; p.rawLog = false }
}

// WithQueryLoggerUnsafe attaches a logger that receives raw parameter
// values. Use only in development — parameters may contain sensitive data.
func WithQueryLoggerUnsafe(fn QueryLogger) Option {
	return func(p *Provider) { p.logger = fn; p.rawLog = true }
}

// New creates a SQL provider. The db connection should already be open.
func New(db *sql.DB, opts ...Option) *Provider {
	p := &Provider{
		db:       db,
		compiler: &Compiler{},
		name:     "sql",
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func (p *Provider) logQuery(c Compiled) {
	if p.logger == nil {
		return
	}
	if p.rawLog {
		p.logger(c.SQL, c.Params)
		return
	}
	redacted := make([]any, len(c.Params))
	for i := range c.Params {
		redacted[i] = "[REDACTED]"
	}
	p.logger(c.SQL, redacted)
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Transactions:     true,
		Aggregations:     true,
		NestedFilters:    true,
		PartialUpdate:    true,
		Sorting:          true,
		OffsetPagination: true,
		SchemaMigrations: true,
	}
}

func (p *Provider) Open(ctx context.Context, dsn string) error {
	return p.db.PingContext(ctx)
}

func (p *Provider) Close() error {
	return p.db.Close()
}

// --- CRUD ---

func (p *Provider) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	values := structToMap(meta, entity)
	c := p.compiler.Insert(meta, values)

	if meta.PrimaryKey != nil && meta.PrimaryKey.AutoIncr {
		if p.compiler.Numbered {
			c.SQL += fmt.Sprintf(" RETURNING %s", quoteIdent(meta.PrimaryKey.Column))
			p.logQuery(c)
			var id any
			err := p.retryDo(ctx, func() error {
				return p.db.QueryRowContext(ctx, c.SQL, c.Params...).Scan(&id)
			})
			return id, err
		}
		var resID int64
		p.logQuery(c)
		err := p.retryDo(ctx, func() error {
			res, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
			if err != nil {
				return err
			}
			resID, err = res.LastInsertId()
			return err
		})
		return resID, err
	}

	p.logQuery(c)
	err := p.retryDo(ctx, func() error {
		_, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return pkFromStruct(meta, entity), nil
}

func (p *Provider) Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error {
	values := structToMap(meta, entity)
	pk := pkFromStruct(meta, entity)
	c := p.compiler.Update(meta, values, changed, pk)
	if c.SQL == "" {
		return nil
	}
	p.logQuery(c)
	return p.retryDo(ctx, func() error {
		_, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
		return err
	})
}

func (p *Provider) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	c := p.compiler.Delete(meta, pkValue)
	p.logQuery(c)
	return p.retryDo(ctx, func() error {
		_, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
		return err
	})
}

func (p *Provider) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	c := p.compiler.FindByPK(meta, pkValue)
	p.logQuery(c)
	return p.retryDo(ctx, func() error {
		row := p.db.QueryRowContext(ctx, c.SQL, c.Params...)
		return scanRow(meta, row, dest)
	})
}

// --- Query ---

func (p *Provider) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	if err := provider.ValidateQueryCapabilities(p.Capabilities(), q); err != nil {
		return err
	}
	c := p.compiler.Select(meta, q)
	p.logQuery(c)
	return p.retryDo(ctx, func() error {
		rows, err := p.db.QueryContext(ctx, c.SQL, c.Params...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return scanRows(meta, rows, dest)
	})
}

// --- QueryExplainer (dry-run) ---

func (p *Provider) ExplainSelect(meta *model.EntityMeta, q query.Query) provider.CompiledQuery {
	c := p.compiler.Select(meta, q)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}
}

func (p *Provider) ExplainFindByPK(meta *model.EntityMeta, pkValue any) provider.CompiledQuery {
	c := p.compiler.FindByPK(meta, pkValue)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}
}

func (p *Provider) ExplainInsert(meta *model.EntityMeta, entity any) provider.CompiledQuery {
	values := structToMap(meta, entity)
	c := p.compiler.Insert(meta, values)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}
}

func (p *Provider) ExplainUpdate(meta *model.EntityMeta, entity any, changed []string) provider.CompiledQuery {
	values := structToMap(meta, entity)
	pk := pkFromStruct(meta, entity)
	c := p.compiler.Update(meta, values, changed, pk)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}
}

func (p *Provider) ExplainDelete(meta *model.EntityMeta, pkValue any) provider.CompiledQuery {
	c := p.compiler.Delete(meta, pkValue)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}
}

// --- Transactions ---

func (p *Provider) Begin(ctx context.Context) (provider.Tx, error) {
	stx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: stx, compiler: p.compiler, logger: p.logger, rawLog: p.rawLog}, nil
}

type sqlTx struct {
	tx       *sql.Tx
	compiler *Compiler
	logger   QueryLogger
	rawLog   bool
}

func (t *sqlTx) logQuery(c Compiled) {
	if t.logger == nil {
		return
	}
	if t.rawLog {
		t.logger(c.SQL, c.Params)
		return
	}
	redacted := make([]any, len(c.Params))
	for i := range c.Params {
		redacted[i] = "[REDACTED]"
	}
	t.logger(c.SQL, redacted)
}

func (t *sqlTx) Commit() error   { return t.tx.Commit() }
func (t *sqlTx) Rollback() error { return t.tx.Rollback() }

func (t *sqlTx) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	values := structToMap(meta, entity)
	c := t.compiler.Insert(meta, values)

	if meta.PrimaryKey != nil && meta.PrimaryKey.AutoIncr {
		if t.compiler.Numbered {
			c.SQL += fmt.Sprintf(" RETURNING %s", quoteIdent(meta.PrimaryKey.Column))
			t.logQuery(c)
			var id any
			err := t.tx.QueryRowContext(ctx, c.SQL, c.Params...).Scan(&id)
			return id, err
		}
		t.logQuery(c)
		res, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
		if err != nil {
			return nil, err
		}
		id, err := res.LastInsertId()
		return id, err
	}

	t.logQuery(c)
	_, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	return pkFromStruct(meta, entity), err
}

func (t *sqlTx) Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error {
	values := structToMap(meta, entity)
	pk := pkFromStruct(meta, entity)
	c := t.compiler.Update(meta, values, changed, pk)
	if c.SQL == "" {
		return nil
	}
	t.logQuery(c)
	_, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	return err
}

func (t *sqlTx) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	c := t.compiler.Delete(meta, pkValue)
	t.logQuery(c)
	_, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	return err
}

func (t *sqlTx) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	c := t.compiler.FindByPK(meta, pkValue)
	t.logQuery(c)
	row := t.tx.QueryRowContext(ctx, c.SQL, c.Params...)
	return scanRow(meta, row, dest)
}

func (t *sqlTx) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	c := t.compiler.Select(meta, q)
	t.logQuery(c)
	rows, err := t.tx.QueryContext(ctx, c.SQL, c.Params...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanRows(meta, rows, dest)
}

// --- helpers ---

func structToMap(meta *model.EntityMeta, entity any) map[string]any {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	m := make(map[string]any, len(meta.Fields))
	for _, f := range meta.Fields {
		m[f.FieldName] = val.FieldByName(f.FieldName).Interface()
	}
	return m
}

func pkFromStruct(meta *model.EntityMeta, entity any) any {
	if meta.PrimaryKey == nil {
		return nil
	}
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return val.FieldByName(meta.PrimaryKey.FieldName).Interface()
}

func scanRow(meta *model.EntityMeta, row *sql.Row, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dest must be *struct")
	}
	val = val.Elem()

	ptrs := make([]any, len(meta.Fields))
	for i, f := range meta.Fields {
		ptrs[i] = val.FieldByName(f.FieldName).Addr().Interface()
	}
	return row.Scan(ptrs...)
}

func scanRows(meta *model.EntityMeta, rows *sql.Rows, dest any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be *[]T or *[]*T")
	}

	// Read actual column names returned by the query
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("columns: %w", err)
	}

	// Build column→field index for O(1) lookup
	colToField := make(map[string]*model.FieldMeta, len(meta.Fields))
	for i := range meta.Fields {
		colToField[meta.Fields[i].Column] = &meta.Fields[i]
	}

	sliceVal := dv.Elem()
	elemType := sliceVal.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr

	for rows.Next() {
		var elem reflect.Value
		if isPtr {
			elem = reflect.New(elemType.Elem())
		} else {
			elem = reflect.New(elemType)
		}

		target := elem.Elem()

		// Build scan destinations aligned to actual column order
		ptrs := make([]any, len(cols))
		for i, col := range cols {
			if fm, ok := colToField[col]; ok {
				ptrs[i] = target.FieldByName(fm.FieldName).Addr().Interface()
			} else {
				// Column not mapped (e.g. from a JOIN) — discard into a throwaway
				var discard any
				ptrs[i] = &discard
			}
		}

		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		if isPtr {
			sliceVal = reflect.Append(sliceVal, elem)
		} else {
			sliceVal = reflect.Append(sliceVal, target)
		}
	}

	dv.Elem().Set(sliceVal)
	return rows.Err()
}

// RegisterDefault registers the SQL provider as default.
func RegisterDefault(db *sql.DB, opts ...Option) *Provider {
	p := New(db, opts...)
	provider.Register(p.name, p)
	provider.SetDefault(p.name)
	return p
}

// retryDo wraps fn with the provider-level retry policy if configured.
func (p *Provider) retryDo(ctx context.Context, fn func() error) error {
	if len(p.retry) > 0 {
		return resiliency.Retry(ctx, fn, p.retry...)
	}
	return fn()
}
