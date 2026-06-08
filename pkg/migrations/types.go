package migrations

import "reflect"

// ColumnRef is a column-level foreign-key reference. It renders as
// REFERENCES "table" ("column") [ON DELETE <action>] inline in the column
// definition, which every supported dialect accepts (including SQLite, where
// table-level ALTER ADD CONSTRAINT is unavailable).
type ColumnRef struct {
	Table    string `json:"table"`               // referenced table
	Column   string `json:"column"`              // referenced column
	OnDelete string `json:"on_delete,omitempty"` // optional action: CASCADE, SET NULL, RESTRICT, ...
}

// ColumnDef describes a single column in a CREATE TABLE or ADD COLUMN. The JSON
// tags let it be serialized into the model snapshot file; GoType is omitted
// because the snapshot resolves a concrete SQLType before writing.
type ColumnDef struct {
	Name       string       `json:"name"`
	SQLType    string       `json:"sql_type,omitempty"`
	PrimaryKey bool         `json:"primary_key,omitempty"`
	AutoIncr   bool         `json:"auto_incr,omitempty"`
	Nullable   bool         `json:"nullable,omitempty"`
	Default    string       `json:"default,omitempty"`     // literal default expression
	Index      string       `json:"index,omitempty"`       // explicit secondary index name
	IndexOrder int          `json:"index_order,omitempty"` // 1-based position in a composite index; 0 = unspecified
	Indexed    bool         `json:"indexed,omitempty"`     // a secondary index is requested
	Unique     bool         `json:"unique,omitempty"`      // the index is unique
	Ref        *ColumnRef   `json:"ref,omitempty"`         // foreign-key reference
	GoType     reflect.Type `json:"-"`
}

// --- Migration operations ---

// OpKind identifies the DDL operation type.
type OpKind int

const (
	OpCreateTable OpKind = iota
	OpDropTable
	OpAddColumn
	OpDropColumn
	OpAlterColumn
	OpCreateIndex
	OpDropIndex
	OpRawSQL
)

// MigrationOp is a single DDL operation emitted by the differ.
type MigrationOp interface {
	Kind() OpKind
}

// CreateTableOp creates a new table with the given columns.
type CreateTableOp struct {
	Table   string      `json:"table"`
	Columns []ColumnDef `json:"columns"`
}

func (o CreateTableOp) Kind() OpKind { return OpCreateTable }

// DropTableOp drops an existing table.
type DropTableOp struct {
	Table string `json:"table"`
}

func (o DropTableOp) Kind() OpKind { return OpDropTable }

// AddColumnOp adds a column to an existing table.
type AddColumnOp struct {
	Table  string    `json:"table"`
	Column ColumnDef `json:"column"`
}

func (o AddColumnOp) Kind() OpKind { return OpAddColumn }

// DropColumnOp removes a column from an existing table.
type DropColumnOp struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

func (o DropColumnOp) Kind() OpKind { return OpDropColumn }

// AlterColumnOp changes the type or constraints of an existing column.
type AlterColumnOp struct {
	Table  string    `json:"table"`
	Column ColumnDef `json:"column"` // new definition
}

func (o AlterColumnOp) Kind() OpKind { return OpAlterColumn }

// CreateIndexOp creates a secondary index.
type CreateIndexOp struct {
	Name    string   `json:"name"`
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
}

func (o CreateIndexOp) Kind() OpKind { return OpCreateIndex }

// DropIndexOp drops a secondary index.
type DropIndexOp struct {
	Name  string `json:"name"`
	Table string `json:"table,omitempty"` // required by MySQL/MSSQL DROP INDEX; optional for Postgres/SQLite
}

func (o DropIndexOp) Kind() OpKind { return OpDropIndex }

// RawSQLOp lets a migration emit a literal SQL statement that the
// differ-based DDL ops can't express — typically DML (seeding rows,
// backfilling values, swapping enum strings) or dialect-specific
// constructs like ON CONFLICT clauses, partial indexes, or stored
// procedures.
//
// The renderer passes SQL through unmodified, so it's on the migration
// author to make sure the statement is valid for every dialect they
// target. Wrap dialect-specific syntax with a runtime check on the
// Dialect type if a single migration has to support multiple
// databases.
type RawSQLOp struct {
	SQL string `json:"sql"`
}

func (o RawSQLOp) Kind() OpKind { return OpRawSQL }

// DatabaseSchema represents the current state of the database for diffing.
type DatabaseSchema struct {
	Tables map[string]*TableSchema `json:"tables"`
}

// TableSchema describes one table in the current database.
type TableSchema struct {
	Name    string                `json:"name"`
	Columns map[string]*ColumnDef `json:"columns"`
}
