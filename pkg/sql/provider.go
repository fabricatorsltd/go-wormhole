package sql

import (
	"context"
	"database/sql"
	"encoding/json"
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

// SQLDB exposes the underlying *sql.DB for advanced operations.
func (p *Provider) SQLDB() *sql.DB { return p.db }

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

// Upsert executes an INSERT ... ON CONFLICT for entity, implementing the
// provider.Upserter capability. The conflict target is a caller-known
// unique/PK column set, so there is no generated-PK writeback; it returns
// the entity's (client-set) primary key.
func (p *Provider) Upsert(ctx context.Context, meta *model.EntityMeta, entity any, conflict provider.ConflictClause) (any, error) {
	c := p.compiler.InsertOnConflict(meta, structToMap(meta, entity), conflict)
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

// DeleteWhere implements provider.BulkDeleter for the SQL provider.
// Emits a single DELETE … WHERE … against the entity table.
// Returns rowsAffected, or -1 if the driver cannot report it.
func (p *Provider) DeleteWhere(ctx context.Context, meta *model.EntityMeta, q query.Query) (int64, error) {
	c := p.compiler.DeleteWhere(meta, q)
	p.logQuery(c)
	var affected int64
	err := p.retryDo(ctx, func() error {
		res, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
		if err != nil {
			return err
		}
		n, errN := res.RowsAffected()
		if errN != nil {
			affected = -1
			return nil
		}
		affected = n
		return nil
	})
	return affected, err
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
	q, err := provider.ValidateQueryCapabilities(meta, p.Capabilities(), q)
	if err != nil {
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

func (p *Provider) ExplainSelect(meta *model.EntityMeta, q query.Query) (provider.CompiledQuery, error) {
	q, err := provider.ValidateQueryCapabilities(meta, p.Capabilities(), q)
	if err != nil {
		return provider.CompiledQuery{}, err
	}
	c := p.compiler.Select(meta, q)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}, nil
}

func (p *Provider) ExplainFindByPK(meta *model.EntityMeta, pkValue any) (provider.CompiledQuery, error) {
	c := p.compiler.FindByPK(meta, pkValue)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}, nil
}

func (p *Provider) ExplainInsert(meta *model.EntityMeta, entity any) (provider.CompiledQuery, error) {
	values := structToMap(meta, entity)
	c := p.compiler.Insert(meta, values)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}, nil
}

func (p *Provider) ExplainUpdate(meta *model.EntityMeta, entity any, changed []string) (provider.CompiledQuery, error) {
	values := structToMap(meta, entity)
	pk := pkFromStruct(meta, entity)
	c := p.compiler.Update(meta, values, changed, pk)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}, nil
}

func (p *Provider) ExplainDelete(meta *model.EntityMeta, pkValue any) (provider.CompiledQuery, error) {
	c := p.compiler.Delete(meta, pkValue)
	return provider.CompiledQuery{SQL: c.SQL, Params: c.Params}, nil
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

// Upsert executes an INSERT ... ON CONFLICT inside the transaction,
// implementing provider.Upserter on the SQL Tx.
func (t *sqlTx) Upsert(ctx context.Context, meta *model.EntityMeta, entity any, conflict provider.ConflictClause) (any, error) {
	c := t.compiler.InsertOnConflict(meta, structToMap(meta, entity), conflict)
	t.logQuery(c)
	_, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	return pkFromStruct(meta, entity), err
}

// TxRunner is the raw-SQL escape hatch exposed by a SQL transaction. A
// provider.Tx obtained from a SQL provider (e.g. inside
// DbContext.Transaction) satisfies it, letting callers run hand-written
// SQL — such as SELECT ... FOR UPDATE — atomically alongside the ORM's
// tracked operations.
type TxRunner interface {
	ExecContext(ctx context.Context, stmt string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, stmt string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, stmt string, args ...any) *sql.Row
}

var _ TxRunner = (*sqlTx)(nil)

func (t *sqlTx) ExecContext(ctx context.Context, stmt string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, stmt, args...)
}

func (t *sqlTx) QueryContext(ctx context.Context, stmt string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, stmt, args...)
}

func (t *sqlTx) QueryRowContext(ctx context.Context, stmt string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, stmt, args...)
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

// DeleteWhere mirrors Provider.DeleteWhere for in-transaction bulk deletes.
func (t *sqlTx) DeleteWhere(ctx context.Context, meta *model.EntityMeta, q query.Query) (int64, error) {
	c := t.compiler.DeleteWhere(meta, q)
	t.logQuery(c)
	res, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	if err != nil {
		return 0, err
	}
	n, errN := res.RowsAffected()
	if errN != nil {
		return -1, nil
	}
	return n, nil
}

func (t *sqlTx) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	c := t.compiler.FindByPK(meta, pkValue)
	t.logQuery(c)
	row := t.tx.QueryRowContext(ctx, c.SQL, c.Params...)
	return scanRow(meta, row, dest)
}

func (t *sqlTx) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	var err error
	q, err = provider.ValidateQueryCapabilities(meta, provider.Capabilities{
		Transactions:     true,
		Aggregations:     true,
		NestedFilters:    true,
		PartialUpdate:    true,
		Sorting:          true,
		OffsetPagination: true,
		SchemaMigrations: true,
	}, q)
	if err != nil {
		return err
	}
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
		v := val.FieldByName(f.FieldName).Interface()
		// `json`-tagged fields are serialized to a JSON text/blob column.
		if _, ok := f.Tags["json"]; ok {
			if b, err := json.Marshal(v); err == nil {
				v = string(b)
			}
		}
		m[f.FieldName] = v
	}
	return m
}

// jsonScanner unmarshals a JSON text/blob column into a Go field. Used for
// `json`-tagged fields so struct/slice/map columns round-trip.
type jsonScanner struct{ dst any }

func (s jsonScanner) Scan(src any) error {
	if src == nil {
		return nil
	}
	var b []byte
	switch v := src.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("json column: cannot scan %T", src)
	}
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, s.dst)
}

// scanTarget returns the rows.Scan destination for a field: a JSON
// unmarshaling wrapper for `json`-tagged fields, otherwise the field's
// address.
func scanTarget(field reflect.Value, fm *model.FieldMeta) any {
	addr := field.Addr().Interface()
	if _, ok := fm.Tags["json"]; ok {
		return jsonScanner{dst: addr}
	}
	return addr
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
	for i := range meta.Fields {
		fm := &meta.Fields[i]
		ptrs[i] = scanTarget(val.FieldByName(fm.FieldName), fm)
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
				ptrs[i] = scanTarget(target.FieldByName(fm.FieldName), fm)
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
