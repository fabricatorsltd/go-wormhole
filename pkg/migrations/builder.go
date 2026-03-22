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
	// AutoIncrementType returns a replacement SQL type for auto-increment
	// columns (e.g. "SERIAL" for Postgres). Return "" to keep the original type.
	AutoIncrementType(baseType string) string
	SupportsIfNotExists() bool

	// Optional methods for dialect-specific DDL operations.
	// These can be checked via type assertion:
	// if d, ok := dialect.(interface{ DisableConstraints(table string) string }); ok { ... }

	// DisableConstraints returns a SQL statement to disable foreign key checks
	// or other constraints for a given table.
	// DisableConstraints(table string) string

	// EnableConstraints returns a SQL statement to enable foreign key checks
	// or other constraints for a given table.
	// EnableConstraints(table string) string

	// SetIdentityInsert returns a SQL statement to enable or disable IDENTITY_INSERT
	// for a given table, primarily for MSSQL.
	// SetIdentityInsert(table string, enable bool) string

	// ResetSequence returns a SQL statement to reset the sequence for an auto-increment
	// column to its current maximum value, primarily for PostgreSQL.
	// ResetSequence(table string, column string) string

	// ColumnName returns the database column name for a given Go field name.
	// This allows dialects to apply naming conventions (e.g., snake_case).
	// ColumnName(fieldName string) string
}

// DefaultDialect produces standard SQL with double-quote identifiers.
type DefaultDialect struct{}

func (DefaultDialect) QuoteIdent(s string) string         { return `"` + s + `"` }
func (DefaultDialect) AutoIncrementClause() string         { return "AUTOINCREMENT" }
func (DefaultDialect) AutoIncrementType(string) string     { return "" }
func (DefaultDialect) SupportsIfNotExists() bool           { return true }
func (DefaultDialect) ColumnName(fieldName string) string { return strings.ToLower(fieldName) } // Default to lowercase for simple cases

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
		addKw := "ADD COLUMN"
		if ac, ok := b.dialect.(interface{ AddColumnKeyword() string }); ok {
			addKw = ac.AddColumnKeyword()
		}
		return fmt.Sprintf("ALTER TABLE %s %s %s",
			q(o.Table), addKw, b.renderColumnDef(o.Column, q))
	case DropColumnOp:
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
			q(o.Table), q(o.Column))
	case AlterColumnOp:
		if as, ok := b.dialect.(interface{ AlterColumnSuffix() string }); ok {
			suffix := as.AlterColumnSuffix()
			if suffix == "" {
				// MSSQL style: ALTER COLUMN col newtype (no TYPE keyword)
				return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s",
					q(o.Table), q(o.Column.Name), b.resolveType(o.Column))
			}
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
			q(o.Table), q(o.Column.Name), b.resolveType(o.Column))
		if o.Column.Using != "" {
			stmt += " USING " + o.Column.Using
		}
		return stmt
	case CreateIndexOp:
		u := ""
		if o.Unique {
			u = "UNIQUE "
		}
		cols := make([]string, len(o.Columns))
		for i, c := range o.Columns {
			cols[i] = q(c)
		}
		ifne := ""
		if b.dialect.SupportsIfNotExists() {
			ifne = "IF NOT EXISTS "
		}
		return fmt.Sprintf("CREATE %sINDEX %s%s ON %s (%s)",
			u, ifne, q(o.Name), q(o.Table), strings.Join(cols, ", "))
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
	ifne := ""
	if b.dialect.SupportsIfNotExists() {
		ifne = "IF NOT EXISTS "
	}
	return fmt.Sprintf("CREATE TABLE %s%s (\n  %s\n)",
		ifne, q(o.Table), strings.Join(colDefs, ",\n  "))
}

func (b *SchemaBuilder) renderColumnDef(c ColumnDef, q func(string) string) string {
	var parts []string
	parts = append(parts, q(c.Name))

	resolvedType := b.resolveType(c)

	// Let the dialect replace the type for auto-increment columns
	// (e.g. Postgres: INTEGER → SERIAL)
	if c.AutoIncr {
		if replacement := b.dialect.AutoIncrementType(resolvedType); replacement != "" {
			resolvedType = replacement
		}
	}
	parts = append(parts, resolvedType)

	if c.PrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	}
	if c.AutoIncr {
		clause := b.dialect.AutoIncrementClause()
		if clause != "" {
			parts = append(parts, clause)
		}
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
	// time.Time → TIMESTAMPTZ
	if t == reflect.TypeOf((*interface{ UnixNano() int64 })(nil)).Elem() {
		return "TIMESTAMPTZ"
	}
	// Named type check: time.Time is a struct in the "time" package
	if t.Kind() == reflect.Struct && t.PkgPath() == "time" && t.Name() == "Time" {
		return "TIMESTAMPTZ"
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
