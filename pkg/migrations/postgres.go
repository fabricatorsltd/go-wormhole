package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/fabricatorsltd/go-wormhole/pkg/util"
)

// migrationLockKey is the fixed advisory-lock key used to serialize migration
// runs. It is an arbitrary constant shared by every wormhole process.
const migrationLockKey int64 = 0x776F726D68 // "wormh"

const migrationLockName = "wormhole_migrations"

// PostgresDialect generates DDL for PostgreSQL.
type PostgresDialect struct{}

func (PostgresDialect) QuoteIdent(s string) string  { return `"` + s + `"` }
func (PostgresDialect) AutoIncrementClause() string { return "" }
func (PostgresDialect) SupportsIfNotExists() bool   { return true }

// MapColumnType renders portable SQL types as their Postgres-native form. It is
// applied at DDL-render time only, so the stored ColumnDef keeps its portable
// type and schema diffing stays stable. An empty return keeps the type as-is.
func (PostgresDialect) MapColumnType(sqlType string) string {
	switch strings.ToUpper(sqlType) {
	case "TIMESTAMP", "DATETIME":
		return "TIMESTAMPTZ"
	case "BLOB":
		return "BYTEA"
	default:
		return ""
	}
}

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

func (PostgresDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

// AcquireLock takes a session-level advisory lock, blocking until it is granted.
func (PostgresDialect) AcquireLock(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey)
	return err
}

// ReleaseLock frees the advisory lock taken by AcquireLock.
func (PostgresDialect) ReleaseLock(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey)
	return err
}

// IdempotentHistoryDDL creates the migration history table if it is absent so an
// idempotent script is self-contained on a fresh database.
func (d PostgresDialect) IdempotentHistoryDDL(historyTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    %s TEXT PRIMARY KEY,
    %s TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`, d.QuoteIdent(historyTable), d.QuoteIdent("migration_id"), d.QuoteIdent("applied_at"))
}

// WrapIdempotent guards a migration's statements with a history-existence check
// so the script can be re-run safely. PostgreSQL has no top-level IF, so the
// block runs inside an anonymous DO body.
func (d PostgresDialect) WrapIdempotent(migrationID, historyTable string, statements []string) string {
	id := strings.ReplaceAll(migrationID, "'", "''")
	var b strings.Builder
	b.WriteString("DO $$\nBEGIN\n")
	fmt.Fprintf(&b, "IF NOT EXISTS (SELECT 1 FROM %s WHERE %s = '%s') THEN\n",
		d.QuoteIdent(historyTable), d.QuoteIdent("migration_id"), id)
	for _, s := range statements {
		b.WriteString("    ")
		b.WriteString(strings.TrimRight(strings.TrimSpace(s), ";"))
		b.WriteString(";\n")
	}
	fmt.Fprintf(&b, "    INSERT INTO %s (%s) VALUES ('%s');\n",
		d.QuoteIdent(historyTable), d.QuoteIdent("migration_id"), id)
	b.WriteString("END IF;\nEND $$;\n")
	return b.String()
}

// ColumnName returns the database column name for a given Go field name,
// converted to snake_case for PostgreSQL.
func (PostgresDialect) ColumnName(fieldName string) string {
	return util.ToSnake(fieldName)
}
