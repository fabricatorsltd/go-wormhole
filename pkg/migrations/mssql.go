package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

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

// AcquireLock takes a session-scoped application lock via sp_getapplock, waiting
// up to 30s. The procedure returns a status >= 0 on success and < 0 on failure
// (e.g. -1 timeout), so the return value is captured and checked.
func (MSSQLDialect) AcquireLock(ctx context.Context, conn *sql.Conn) error {
	const q = `DECLARE @r int;
EXEC @r = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 30000;
SELECT @r;`
	var r int
	if err := conn.QueryRowContext(ctx, q, sql.Named("p1", migrationLockName)).Scan(&r); err != nil {
		return err
	}
	if r < 0 {
		return fmt.Errorf("could not acquire migration lock %q (sp_getapplock returned %d)", migrationLockName, r)
	}
	return nil
}

// ReleaseLock frees the application lock taken by AcquireLock.
func (MSSQLDialect) ReleaseLock(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx,
		`EXEC sp_releaseapplock @Resource = @p1, @LockOwner = 'Session';`,
		sql.Named("p1", migrationLockName))
	return err
}

// IdempotentHistoryDDL creates the migration history table if it is absent so an
// idempotent script is self-contained on a fresh database. SQL Server has no
// CREATE TABLE IF NOT EXISTS, so an OBJECT_ID guard is used; TIMESTAMP is a
// rowversion type here, so the applied-at column is DATETIME2.
func (d MSSQLDialect) IdempotentHistoryDDL(historyTable string) string {
	return fmt.Sprintf(`IF OBJECT_ID(N'%s', N'U') IS NULL
BEGIN
    CREATE TABLE %s (
        %s NVARCHAR(255) NOT NULL PRIMARY KEY,
        %s DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
`, historyTable, d.QuoteIdent(historyTable), d.QuoteIdent("migration_id"), d.QuoteIdent("applied_at"))
}

// WrapIdempotent guards a migration's statements with a history-existence check
// using a top-level T-SQL IF block.
func (d MSSQLDialect) WrapIdempotent(migrationID, historyTable string, statements []string) string {
	id := strings.ReplaceAll(migrationID, "'", "''")
	var b strings.Builder
	fmt.Fprintf(&b, "IF NOT EXISTS (SELECT 1 FROM %s WHERE %s = '%s')\nBEGIN\n",
		d.QuoteIdent(historyTable), d.QuoteIdent("migration_id"), id)
	for _, s := range statements {
		b.WriteString("    ")
		b.WriteString(strings.TrimRight(strings.TrimSpace(s), ";"))
		b.WriteString(";\n")
	}
	fmt.Fprintf(&b, "    INSERT INTO %s (%s) VALUES ('%s');\n",
		d.QuoteIdent(historyTable), d.QuoteIdent("migration_id"), id)
	b.WriteString("END;\n")
	return b.String()
}

// DropIndexSQL renders T-SQL's table-scoped DROP INDEX (IF EXISTS, SQL Server 2016+).
func (d MSSQLDialect) DropIndexSQL(name, table string) string {
	return fmt.Sprintf("DROP INDEX IF EXISTS %s ON %s", d.QuoteIdent(name), d.QuoteIdent(table))
}

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
