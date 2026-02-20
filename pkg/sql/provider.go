package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/provider"
	"github.com/mirkobrombin/go-wormhole/pkg/query"
)

// Provider implements provider.Provider for any database/sql-compatible driver.
type Provider struct {
	db       *sql.DB
	compiler *Compiler
	name     string
}

var _ provider.Provider = (*Provider)(nil)

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

func (p *Provider) Name() string { return p.name }

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
			// Postgres: INSERT … RETURNING id
			c.SQL += fmt.Sprintf(" RETURNING %s", quoteIdent(meta.PrimaryKey.Column))
			var id any
			err := p.db.QueryRowContext(ctx, c.SQL, c.Params...).Scan(&id)
			return id, err
		}
		// SQLite / MySQL: use LastInsertId
		res, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
		if err != nil {
			return nil, err
		}
		id, err := res.LastInsertId()
		return id, err
	}

	_, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
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
	_, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
	return err
}

func (p *Provider) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	c := p.compiler.Delete(meta, pkValue)
	_, err := p.db.ExecContext(ctx, c.SQL, c.Params...)
	return err
}

func (p *Provider) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	c := p.compiler.FindByPK(meta, pkValue)
	row := p.db.QueryRowContext(ctx, c.SQL, c.Params...)
	return scanRow(meta, row, dest)
}

// --- Query ---

func (p *Provider) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	c := p.compiler.Select(meta, q)
	rows, err := p.db.QueryContext(ctx, c.SQL, c.Params...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanRows(meta, rows, dest)
}

// --- Transactions ---

func (p *Provider) Begin(ctx context.Context) (provider.Tx, error) {
	stx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: stx, compiler: p.compiler}, nil
}

type sqlTx struct {
	tx       *sql.Tx
	compiler *Compiler
}

func (t *sqlTx) Commit() error   { return t.tx.Commit() }
func (t *sqlTx) Rollback() error { return t.tx.Rollback() }

func (t *sqlTx) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	values := structToMap(meta, entity)
	c := t.compiler.Insert(meta, values)

	if meta.PrimaryKey != nil && meta.PrimaryKey.AutoIncr {
		if t.compiler.Numbered {
			c.SQL += fmt.Sprintf(" RETURNING %s", quoteIdent(meta.PrimaryKey.Column))
			var id any
			err := t.tx.QueryRowContext(ctx, c.SQL, c.Params...).Scan(&id)
			return id, err
		}
		res, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
		if err != nil {
			return nil, err
		}
		id, err := res.LastInsertId()
		return id, err
	}

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
	_, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	return err
}

func (t *sqlTx) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	c := t.compiler.Delete(meta, pkValue)
	_, err := t.tx.ExecContext(ctx, c.SQL, c.Params...)
	return err
}

func (t *sqlTx) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	c := t.compiler.FindByPK(meta, pkValue)
	row := t.tx.QueryRowContext(ctx, c.SQL, c.Params...)
	return scanRow(meta, row, dest)
}

func (t *sqlTx) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	c := t.compiler.Select(meta, q)
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
		return fmt.Errorf("dest must be *[]T")
	}

	sliceVal := dv.Elem()
	elemType := sliceVal.Type().Elem()

	for rows.Next() {
		elem := reflect.New(elemType).Elem()
		ptrs := make([]any, len(meta.Fields))
		for i, f := range meta.Fields {
			ptrs[i] = elem.FieldByName(f.FieldName).Addr().Interface()
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		sliceVal = reflect.Append(sliceVal, elem)
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
