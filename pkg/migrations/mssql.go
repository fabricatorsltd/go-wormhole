package migrations

import "fmt"

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
func (MSSQLDialect) AutoIncrementClause() string     { return "IDENTITY(1,1)" }
func (MSSQLDialect) AutoIncrementType(string) string { return "" }
func (MSSQLDialect) SupportsIfNotExists() bool       { return false }

// AddColumnKeyword returns the T-SQL keyword for adding a column.
// MSSQL uses ALTER TABLE t ADD col ... (no COLUMN keyword).
func (MSSQLDialect) AddColumnKeyword() string { return "ADD" }

// AlterColumnSuffix returns the T-SQL syntax fragment for ALTER COLUMN.
// MSSQL uses ALTER TABLE t ALTER COLUMN col newtype (no TYPE keyword).
func (MSSQLDialect) AlterColumnSuffix() string { return "" }

// DisableConstraints returns a SQL statement to disable foreign key checks
// or other constraints for a given table in MSSQL.
func (d MSSQLDialect) DisableConstraints(table string) string {
	return fmt.Sprintf("ALTER TABLE %s NOCHECK CONSTRAINT ALL", d.QuoteIdent(table))
}

// EnableConstraints returns a SQL statement to enable foreign key checks
// or other constraints for a given table in MSSQL.
func (d MSSQLDialect) EnableConstraints(table string) string {
	return fmt.Sprintf("ALTER TABLE %s CHECK CONSTRAINT ALL", d.QuoteIdent(table))
}

// SetIdentityInsert returns a SQL statement to enable or disable IDENTITY_INSERT
// for a given table in MSSQL.
func (d MSSQLDialect) SetIdentityInsert(table string, enable bool) string {
	state := "OFF"
	if enable {
		state = "ON"
	}
	return fmt.Sprintf("SET IDENTITY_INSERT %s %s", d.QuoteIdent(table), state)
}

// ResetSequence returns an empty string as sequence resetting is not directly
// applicable in the same way for MSSQL identity columns.
func (MSSQLDialect) ResetSequence(table string, column string) string {
	return "" // Not directly applicable in the same way as other DBs
}

// ColumnName returns the database column name for a given Go field name,
// returning the original PascalCase name for MSSQL.
func (MSSQLDialect) ColumnName(fieldName string) string {
	return fieldName
}
