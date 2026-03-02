package migrations

import (
	"fmt"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations/util"
)

// PostgresDialect generates DDL for PostgreSQL.
type PostgresDialect struct{}

func (PostgresDialect) QuoteIdent(s string) string      { return `"` + s + `"` }
func (PostgresDialect) AutoIncrementClause() string      { return "" }
func (PostgresDialect) SupportsIfNotExists() bool        { return true }

// AutoIncrementType maps INTEGER → SERIAL, BIGINT → BIGSERIAL.
func (PostgresDialect) AutoIncrementType(baseType string) string {
	switch baseType {
	case "INTEGER", "INT":
		return "SERIAL"
	case "BIGINT":
		return "BIGSERIAL"
	case "SMALLINT":
		return "SMALLSERIAL"
	default:
		return "SERIAL"
	}
}

// DisableConstraints returns a SQL statement to disable foreign key checks
// or other constraints for a given table in PostgreSQL.
func (d PostgresDialect) DisableConstraints(table string) string {
	return fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER ALL;", d.QuoteIdent(table))
}

// EnableConstraints returns a SQL statement to enable foreign key checks
// or other constraints for a given table in PostgreSQL.
func (d PostgresDialect) EnableConstraints(table string) string {
	return fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER ALL;", d.QuoteIdent(table))
}

// SetIdentityInsert is not directly applicable to PostgreSQL.
func (PostgresDialect) SetIdentityInsert(table string, enable bool) string {
	return "" // Not applicable
}

// ResetSequence returns a SQL statement to reset the sequence for an auto-increment
// column in PostgreSQL.
func (d PostgresDialect) ResetSequence(table string, column string) string {
	return fmt.Sprintf("SELECT setval(pg_get_serial_sequence('%s', '%s'), (SELECT MAX(%s) FROM %s));",
		table, column, d.QuoteIdent(column), d.QuoteIdent(table))
}

// ColumnName returns the database column name for a given Go field name,
// converted to snake_case for PostgreSQL.
func (PostgresDialect) ColumnName(fieldName string) string {
	return util.ToSnake(fieldName)
}
