package migrations

import (
	"fmt"
	"reflect"
	"strings"
)

// SchemaBuilder accumulates DDL operations and can render them as SQL.
// Providers can implement dialect-specific builders; this default
// produces standard SQL (compatible with SQLite/Postgres).
type SchemaBuilder struct {
	ops     []MigrationOp
	dialect Dialect
}

// Dialect customises SQL generation for a specific database.
type Dialect interface {
	QuoteIdent(s string) string
	AutoIncrementClause() string
	SupportsIfNotExists() bool
}

// DefaultDialect produces standard SQL with double-quote identifiers.
type DefaultDialect struct{}

func (DefaultDialect) QuoteIdent(s string) string      { return `"` + s + `"` }
func (DefaultDialect) AutoIncrementClause() string      { return "AUTOINCREMENT" }
func (DefaultDialect) SupportsIfNotExists() bool        { return true }

// NewBuilder creates a SchemaBuilder with the default dialect.
func NewBuilder() *SchemaBuilder {
	return &SchemaBuilder{dialect: DefaultDialect{}}
}

// NewBuilderWith creates a SchemaBuilder with a custom dialect.
func NewBuilderWith(d Dialect) *SchemaBuilder {
	return &SchemaBuilder{dialect: d}
}

// --- fluent DDL methods ---

func (b *SchemaBuilder) CreateTable(name string, columns ...ColumnDef) {
	b.ops = append(b.ops, CreateTableOp{Table: name, Columns: columns})
}

func (b *SchemaBuilder) DropTable(name string) {
	b.ops = append(b.ops, DropTableOp{Table: name})
}

func (b *SchemaBuilder) AddColumn(table string, col ColumnDef) {
	b.ops = append(b.ops, AddColumnOp{Table: table, Column: col})
}

func (b *SchemaBuilder) DropColumn(table, colName string) {
	b.ops = append(b.ops, DropColumnOp{Table: table, Column: colName})
}

func (b *SchemaBuilder) CreateIndex(name, table string, unique bool, columns ...string) {
	b.ops = append(b.ops, CreateIndexOp{Name: name, Table: table, Unique: unique, Columns: columns})
}

func (b *SchemaBuilder) DropIndex(name string) {
	b.ops = append(b.ops, DropIndexOp{Name: name})
}

func (b *SchemaBuilder) AlterColumn(table string, col ColumnDef) {
	b.ops = append(b.ops, AlterColumnOp{Table: table, Column: col})
}

// Ops returns the accumulated operations.
func (b *SchemaBuilder) Ops() []MigrationOp { return b.ops }

// SQL renders all accumulated operations as a semicolon-separated SQL string.
func (b *SchemaBuilder) SQL() string {
	var stmts []string
	for _, op := range b.ops {
		stmts = append(stmts, b.renderOp(op))
	}
	return strings.Join(stmts, ";\n") + ";"
}

// Statements returns each operation as an individual SQL string.
func (b *SchemaBuilder) Statements() []string {
	out := make([]string, len(b.ops))
	for i, op := range b.ops {
		out[i] = b.renderOp(op)
	}
	return out
}

func (b *SchemaBuilder) renderOp(op MigrationOp) string {
	q := b.dialect.QuoteIdent
	switch o := op.(type) {
	case CreateTableOp:
		return b.renderCreateTable(o, q)
	case DropTableOp:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", q(o.Table))
	case AddColumnOp:
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
			q(o.Table), b.renderColumnDef(o.Column, q))
	case DropColumnOp:
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
			q(o.Table), q(o.Column))
	case AlterColumnOp:
		return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
			q(o.Table), q(o.Column.Name), b.resolveType(o.Column))
	case CreateIndexOp:
		u := ""
		if o.Unique {
			u = "UNIQUE "
		}
		cols := make([]string, len(o.Columns))
		for i, c := range o.Columns {
			cols[i] = q(c)
		}
		return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
			u, q(o.Name), q(o.Table), strings.Join(cols, ", "))
	case DropIndexOp:
		return fmt.Sprintf("DROP INDEX IF EXISTS %s", q(o.Name))
	default:
		return fmt.Sprintf("-- unknown op: %T", op)
	}
}

func (b *SchemaBuilder) renderCreateTable(o CreateTableOp, q func(string) string) string {
	var colDefs []string
	for _, c := range o.Columns {
		colDefs = append(colDefs, b.renderColumnDef(c, q))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		q(o.Table), strings.Join(colDefs, ",\n  "))
}

func (b *SchemaBuilder) renderColumnDef(c ColumnDef, q func(string) string) string {
	var parts []string
	parts = append(parts, q(c.Name))
	parts = append(parts, b.resolveType(c))

	if c.PrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	}
	if c.AutoIncr {
		parts = append(parts, b.dialect.AutoIncrementClause())
	}
	if !c.Nullable && !c.PrimaryKey {
		parts = append(parts, "NOT NULL")
	}
	if c.Default != "" {
		parts = append(parts, "DEFAULT "+c.Default)
	}
	return strings.Join(parts, " ")
}

func (b *SchemaBuilder) resolveType(c ColumnDef) string {
	if c.SQLType != "" {
		return strings.ToUpper(c.SQLType)
	}
	return GoTypeToSQL(c.GoType)
}

// GoTypeToSQL maps a Go reflect.Type to a default SQL type.
func GoTypeToSQL(t reflect.Type) string {
	if t == nil {
		return "TEXT"
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		return "INTEGER"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Uint, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return "INTEGER"
	case reflect.Uint64:
		return "BIGINT"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "TEXT"
	default:
		return "TEXT"
	}
}
