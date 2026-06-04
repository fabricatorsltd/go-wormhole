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

// WithMySQLDialect targets MySQL: backtick-quoted identifiers and the
// ON DUPLICATE KEY UPDATE upsert form. Placeholders stay "?".
func WithMySQLDialect() Option {
	return func(p *Provider) { p.compiler.Backtick = true }
}

// WithSQLServerDialect targets SQL Server: @p1 placeholders, [bracket]-quoted
// identifiers, SELECT TOP, and the MERGE upsert form.
func WithSQLServerDialect() Option {
	return func(p *Provider) {
		p.compiler.AtPrefixed = true
		p.compiler.BracketQuote = true
		p.compiler.UseTOP = true
	}
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

// CompositeKeysSupported reports that the SQL providers can key entities on a
// composite (multi-column) primary key. Implements provider.CompositeKeyer.
func (p *Provider) CompositeKeysSupported() bool { return true }

// SQLDB exposes the underlying *sql.DB for advanced operations.
func (p *Provider) SQLDB() *sql.DB { return p.db }

func (p *Provider) Capabilities() provider.Capabilities { return sqlCapabilities() }

// sqlCapabilities is the single source of truth for what the SQL backend can do
// natively, shared by the Provider, its Tx, and the bulk-statement guards.
func sqlCapabilities() provider.Capabilities {
	return provider.Capabilities{
		Transactions:     true,
		Aggregations:     true,
		NestedFilters:    true,
		PartialUpdate:    true,
		Sorting:          true,
		OffsetPagination: true,
		SchemaMigrations: true,
		Subqueries:       true,
		SetOperations:    true,
		CaseExpressions:  true,
		JSONQueries:      true,
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
	var res sql.Result
	err := p.retryDo(ctx, func() error {
		r, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
		if err != nil {
			return err
		}
		res = r
		return nil
	})
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if cerr := versionConflict(meta, rows); cerr != nil {
		return cerr
	}
	// Single statement: ExecContext autocommits, so the bump is durable.
	writeBackVersion(meta, entity)
	return nil
}

func (p *Provider) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	c := p.compiler.Delete(meta, pkValue)
	p.logQuery(c)
	return p.retryDo(ctx, func() error {
		_, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
		return err
	})
}

func (p *Provider) DeleteVersioned(ctx context.Context, meta *model.EntityMeta, pkValue, version any) (int64, error) {
	c := p.compiler.DeleteVersioned(meta, pkValue, version)
	p.logQuery(c)
	var rows int64
	err := p.retryDo(ctx, func() error {
		res, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
		if err != nil {
			return err
		}
		rows, err = res.RowsAffected()
		return err
	})
	return rows, err
}

// DeleteWhere implements provider.BulkDeleter for the SQL provider.
// Emits a single DELETE … WHERE … against the entity table.
// Returns rowsAffected, or -1 if the driver cannot report it.
func (p *Provider) DeleteWhere(ctx context.Context, meta *model.EntityMeta, q query.Query) (int64, error) {
	if _, err := provider.ValidateQueryCapabilities(meta, sqlCapabilities(), q); err != nil {
		return 0, err
	}
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

// InsertBatch persists several same-type entities using as few multi-row
// INSERT statements as possible, implementing provider.BatchInserter on the SQL
// Tx. Entities whose emitted column set differs (because `default:`-tagged
// fields drop out when zero) cannot share one statement, so the batch is split
// into maximal consecutive runs of identical column sets. Order is preserved,
// matching the per-row path's semantics. No generated keys are written back:
// the flush path only batches client-assigned primary keys.
func (t *sqlTx) InsertBatch(ctx context.Context, meta *model.EntityMeta, entities []any) error {
	rows := make([]map[string]any, len(entities))
	keys := make([]string, len(entities))
	for i, e := range entities {
		rows[i] = structToMap(meta, e)
		keys[i] = insertColumnKey(meta, rows[i])
	}
	for i := 0; i < len(entities); {
		j := i + 1
		for j < len(entities) && keys[j] == keys[i] {
			j++
		}
		// The sub-run [i,j) shares a column set. Split it further so a single
		// statement never exceeds the placeholder budget: a multi-row INSERT
		// carries cols*rows parameters, and drivers cap that (SQLite historically
		// 999, MSSQL 2100, Postgres 65535). EF Core bounds batch size for the same
		// reason. maxBatchParams is conservative so no backend rejects a batch the
		// per-row path would have accepted.
		cols := len(insertColumns(meta, rows[i]))
		if cols < 1 {
			cols = 1
		}
		maxRows := maxBatchParams / cols
		if maxRows < 1 {
			maxRows = 1
		}
		for s := i; s < j; {
			end := s + maxRows
			if end > j {
				end = j
			}
			c := t.compiler.InsertMany(meta, rows[s:end])
			t.logQuery(c)
			if _, err := t.tx.ExecContext(ctx, c.SQL, c.Params...); err != nil {
				return err
			}
			s = end
		}
		i = j
	}
	return nil
}

// maxBatchParams caps the placeholder count in a single multi-row INSERT. Set
// below SQLite's historical 999-variable limit so a batch never fails on a
// backend the per-row path would have served.
const maxBatchParams = 900

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
	res, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	// Detect the conflict now (to roll the transaction back), but defer the
	// in-memory version bump until the caller commits; see DbContext.flush.
	return versionConflict(meta, rows)
}

func (t *sqlTx) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	c := t.compiler.Delete(meta, pkValue)
	t.logQuery(c)
	_, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	return err
}

func (t *sqlTx) DeleteVersioned(ctx context.Context, meta *model.EntityMeta, pkValue, version any) (int64, error) {
	c := t.compiler.DeleteVersioned(meta, pkValue, version)
	t.logQuery(c)
	res, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DeleteWhere mirrors Provider.DeleteWhere for in-transaction bulk deletes.
func (t *sqlTx) DeleteWhere(ctx context.Context, meta *model.EntityMeta, q query.Query) (int64, error) {
	if _, err := provider.ValidateQueryCapabilities(meta, sqlCapabilities(), q); err != nil {
		return 0, err
	}
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

// marshalJSONAssignments returns sets with json-tagged values serialized to
// their stored string form, mirroring structToMap so the bulk-update path
// stores json columns the same way Save does.
func marshalJSONAssignments(meta *model.EntityMeta, sets []query.Assignment) []query.Assignment {
	out := make([]query.Assignment, len(sets))
	for i, s := range sets {
		out[i] = s
		if f := meta.FieldByColumn(s.Field); f != nil {
			if _, ok := f.Tags["json"]; ok {
				if b, err := json.Marshal(s.Value); err == nil {
					out[i].Value = string(b)
				}
			}
		}
	}
	return out
}

func (p *Provider) UpdateWhere(ctx context.Context, meta *model.EntityMeta, q query.Query, sets []query.Assignment) (int64, error) {
	if _, err := provider.ValidateQueryCapabilities(meta, sqlCapabilities(), q); err != nil {
		return 0, err
	}
	sets = marshalJSONAssignments(meta, sets)
	c := p.compiler.UpdateWhere(meta, q, sets)
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

func (t *sqlTx) UpdateWhere(ctx context.Context, meta *model.EntityMeta, q query.Query, sets []query.Assignment) (int64, error) {
	if _, err := provider.ValidateQueryCapabilities(meta, sqlCapabilities(), q); err != nil {
		return 0, err
	}
	sets = marshalJSONAssignments(meta, sets)
	c := t.compiler.UpdateWhere(meta, q, sets)
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
	q, err = provider.ValidateQueryCapabilities(meta, sqlCapabilities(), q)
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
	for i := range meta.Fields {
		f := &meta.Fields[i]
		v := f.ValueIn(val).Interface()
		// `json`-tagged fields are serialized to a JSON text/blob column.
		if _, ok := f.Tags["json"]; ok {
			if b, err := json.Marshal(v); err == nil {
				v = string(b)
			}
		}
		m[f.FieldName] = v
	}
	// In a single-table hierarchy, the discriminator is owned by the mapping,
	// not the caller: force this type's value so every write is correctly tagged
	// regardless of what the struct field held.
	if meta.Discriminator != nil {
		m[meta.Discriminator.FieldName] = meta.DiscriminatorValue
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

// pkFromStruct returns the entity's primary-key value: a scalar for a
// single-column key, or a []any tuple (in column order) for a composite key.
// The composite form is understood by the compiler's writeKeyWhere.
func pkFromStruct(meta *model.EntityMeta, entity any) any {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	keys := meta.PrimaryKeys
	if len(keys) == 0 {
		if meta.PrimaryKey == nil {
			return nil
		}
		keys = []*model.FieldMeta{meta.PrimaryKey}
	}
	if len(keys) == 1 {
		return val.FieldByName(keys[0].FieldName).Interface()
	}
	vals := make([]any, len(keys))
	for i, k := range keys {
		vals[i] = val.FieldByName(k.FieldName).Interface()
	}
	return vals
}

// versionConflict reports the optimistic-concurrency outcome of a versioned
// UPDATE: when the entity has a version column and the statement matched no
// rows, the row was changed or deleted by another transaction. It does not
// mutate the entity, so it is safe to call inside a not-yet-committed
// transaction (the caller decides when to write the new version back).
func versionConflict(meta *model.EntityMeta, rows int64) error {
	if meta.Version != nil && rows == 0 {
		return provider.ErrConcurrencyConflict
	}
	return nil
}

// writeBackVersion increments the in-memory version field by one to match the
// server-side bump. Call only after the UPDATE has been committed, otherwise a
// later rollback would leave the entity ahead of the database.
func writeBackVersion(meta *model.EntityMeta, entity any) {
	if meta.Version == nil {
		return
	}
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	f := val.FieldByName(meta.Version.FieldName)
	if !f.IsValid() || !f.CanSet() {
		return
	}
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f.SetInt(f.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		f.SetUint(f.Uint() + 1)
	}
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
		ptrs[i] = scanTarget(fm.ValueIn(val), fm)
	}
	return row.Scan(ptrs...)
}

func scanRows(meta *model.EntityMeta, rows *sql.Rows, dest any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be *[]T or *[]*T")
	}

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("columns: %w", err)
	}
	colToField := columnFieldIndex(meta)

	sliceVal := dv.Elem()
	elemType := sliceVal.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	structType := elemType
	if isPtr {
		structType = elemType.Elem()
	}

	for rows.Next() {
		elem, err := scanElem(rows, cols, colToField, structType) // *T
		if err != nil {
			return err
		}
		if isPtr {
			sliceVal = reflect.Append(sliceVal, elem)
		} else {
			sliceVal = reflect.Append(sliceVal, elem.Elem())
		}
	}

	dv.Elem().Set(sliceVal)
	return rows.Err()
}

// columnFieldIndex maps a storage column name to its field metadata for O(1)
// scan-target lookup.
func columnFieldIndex(meta *model.EntityMeta) map[string]*model.FieldMeta {
	idx := make(map[string]*model.FieldMeta, len(meta.Fields))
	for i := range meta.Fields {
		idx[meta.Fields[i].Column] = &meta.Fields[i]
	}
	return idx
}

// scanElem scans the current row into a freshly allocated *structType, aligning
// scan targets to the query's actual column order. Columns with no mapped field
// (e.g. from a JOIN) are discarded. Shared by the buffered (scanRows) and
// streaming (ExecuteStream) paths so their per-row scanning cannot drift.
func scanElem(rows *sql.Rows, cols []string, colToField map[string]*model.FieldMeta, structType reflect.Type) (reflect.Value, error) {
	elem := reflect.New(structType) // *T
	target := elem.Elem()
	ptrs := make([]any, len(cols))
	for i, col := range cols {
		if fm, ok := colToField[col]; ok {
			ptrs[i] = scanTarget(fm.ValueIn(target), fm)
		} else {
			var discard any
			ptrs[i] = &discard
		}
	}
	if err := rows.Scan(ptrs...); err != nil {
		return reflect.Value{}, err
	}
	return elem, nil
}

// ExecuteStream scans a query row by row, invoking yield with each row as a
// fresh *T, implementing provider.StreamExecutor. It does not retry (a
// partially consumed stream cannot be replayed) and closes the rows even when
// the consumer stops early or panics.
func (p *Provider) ExecuteStream(ctx context.Context, meta *model.EntityMeta, q query.Query, structType reflect.Type, yield func(any) bool) error {
	q, err := provider.ValidateQueryCapabilities(meta, p.Capabilities(), q)
	if err != nil {
		return err
	}
	c := p.compiler.Select(meta, q)
	p.logQuery(c)

	rows, err := p.db.QueryContext(ctx, c.SQL, c.Params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("columns: %w", err)
	}
	colToField := columnFieldIndex(meta)

	for rows.Next() {
		elem, err := scanElem(rows, cols, colToField, structType)
		if err != nil {
			return err
		}
		if !yield(elem.Interface()) {
			return nil
		}
	}
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
