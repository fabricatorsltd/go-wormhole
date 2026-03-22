package migrations

import "reflect"

// ColumnDef describes a single column in a CREATE TABLE or ADD COLUMN.
type ColumnDef struct {
	Name       string // storage column name (snake_case)
	SQLType    string // explicit SQL type, e.g. "varchar(255)"
	PrimaryKey bool
	AutoIncr   bool
	Nullable   bool
	Default    string // literal default expression, e.g. "'active'"
	Index      string // secondary index name (empty = none)
	GoType     reflect.Type
	// Using is an optional USING expression appended to ALTER COLUMN TYPE.
	// Required when Postgres cannot auto-cast the existing type (e.g. TEXT → TIMESTAMPTZ).
	// Example: `NULLIF("created_at", '')::timestamptz`
	Using string
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
)

// MigrationOp is a single DDL operation emitted by the differ.
type MigrationOp interface {
	Kind() OpKind
}

// CreateTableOp creates a new table with the given columns.
type CreateTableOp struct {
	Table   string
	Columns []ColumnDef
}

func (o CreateTableOp) Kind() OpKind { return OpCreateTable }

// DropTableOp drops an existing table.
type DropTableOp struct {
	Table string
}

func (o DropTableOp) Kind() OpKind { return OpDropTable }

// AddColumnOp adds a column to an existing table.
type AddColumnOp struct {
	Table  string
	Column ColumnDef
}

func (o AddColumnOp) Kind() OpKind { return OpAddColumn }

// DropColumnOp removes a column from an existing table.
type DropColumnOp struct {
	Table  string
	Column string
}

func (o DropColumnOp) Kind() OpKind { return OpDropColumn }

// AlterColumnOp changes the type or constraints of an existing column.
type AlterColumnOp struct {
	Table  string
	Column ColumnDef // new definition
}

func (o AlterColumnOp) Kind() OpKind { return OpAlterColumn }

// CreateIndexOp creates a secondary index.
type CreateIndexOp struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

func (o CreateIndexOp) Kind() OpKind { return OpCreateIndex }

// DropIndexOp drops a secondary index.
type DropIndexOp struct {
	Name string
}

func (o DropIndexOp) Kind() OpKind { return OpDropIndex }

// DatabaseSchema represents the current state of the database for diffing.
type DatabaseSchema struct {
	Tables map[string]*TableSchema
}

// TableSchema describes one table in the current database.
type TableSchema struct {
	Name    string
	Columns map[string]*ColumnDef
}
