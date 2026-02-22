package migrations

// MSSQLDialect generates DDL for Microsoft SQL Server (T-SQL).
//
// Key differences from standard SQL:
//   - Identifiers are quoted with [brackets]
//   - Auto-increment uses IDENTITY(1,1) as a column property
//   - ADD COLUMN syntax is just ADD (no COLUMN keyword)
//   - ALTER COLUMN syntax omits the TYPE keyword
//   - IF NOT EXISTS is supported for DROP but not for CREATE TABLE
type MSSQLDialect struct{}

func (MSSQLDialect) QuoteIdent(s string) string      { return "[" + s + "]" }
func (MSSQLDialect) AutoIncrementClause() string      { return "IDENTITY(1,1)" }
func (MSSQLDialect) AutoIncrementType(string) string   { return "" }
func (MSSQLDialect) SupportsIfNotExists() bool         { return false }

// AddColumnKeyword returns the T-SQL keyword for adding a column.
// MSSQL uses ALTER TABLE t ADD col ... (no COLUMN keyword).
func (MSSQLDialect) AddColumnKeyword() string { return "ADD" }

// AlterColumnSuffix returns the T-SQL syntax fragment for ALTER COLUMN.
// MSSQL uses ALTER TABLE t ALTER COLUMN col newtype (no TYPE keyword).
func (MSSQLDialect) AlterColumnSuffix() string { return "" }
