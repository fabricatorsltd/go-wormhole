package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fabricatorsltd/go-wormhole/pkg/util"
)

// MySQLDialect generates DDL for MySQL / MariaDB.
//
// WARNING: MySQL performs an implicit COMMIT on every DDL statement.
// Transactional rollback of DDL is NOT possible. The Runner will still
// wrap operations in a transaction for the history-table insert, but
// partial DDL failures cannot be rolled back automatically.
type MySQLDialect struct{}

func (MySQLDialect) QuoteIdent(s string) string      { return "`" + s + "`" }
func (MySQLDialect) AutoIncrementClause() string     { return "AUTO_INCREMENT" }
func (MySQLDialect) AutoIncrementType(string) string { return "" }
func (MySQLDialect) SupportsIfNotExists() bool       { return true }

// AcquireLock takes a named lock via GET_LOCK, waiting up to 30s. GET_LOCK
// returns 1 on success, 0 on timeout, and NULL on error, so the result is
// checked rather than assumed.
func (MySQLDialect) AcquireLock(ctx context.Context, conn *sql.Conn) error {
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", migrationLockName).Scan(&got); err != nil {
		return err
	}
	if !got.Valid || got.Int64 != 1 {
		return fmt.Errorf("could not acquire migration lock %q within 30s", migrationLockName)
	}
	return nil
}

// ReleaseLock frees the named lock taken by AcquireLock.
func (MySQLDialect) ReleaseLock(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", migrationLockName)
	return err
}

// DropIndexSQL renders MySQL's table-scoped DROP INDEX (no IF EXISTS).
func (d MySQLDialect) DropIndexSQL(name, table string) string {
	return fmt.Sprintf("DROP INDEX %s ON %s", d.QuoteIdent(name), d.QuoteIdent(table))
}

// DisableConstraints returns a SQL statement to disable foreign key checks
// or other constraints globally in MySQL.
func (MySQLDialect) DisableConstraints(table string) string {
	return "SET FOREIGN_KEY_CHECKS = 0;"
}

// EnableConstraints returns a SQL statement to enable foreign key checks
// or other constraints globally in MySQL.
func (MySQLDialect) EnableConstraints(table string) string {
	return "SET FOREIGN_KEY_CHECKS = 1;"
}

// SetIdentityInsert is not applicable to MySQL.
func (MySQLDialect) SetIdentityInsert(table string, enable bool) string {
	return "" // Not applicable
}

// ResetSequence returns an empty string as sequence resetting is not directly
// applicable in the same way for MySQL auto-increment columns without a specific value.
func (MySQLDialect) ResetSequence(table string, column string) string {
	return ""
}

// ColumnName returns the database column name for a given Go field name,
// converted to snake_case for MySQL.
func (MySQLDialect) ColumnName(fieldName string) string {
	return util.ToSnake(fieldName)
}
