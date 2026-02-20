package migrations

// MySQLDialect generates DDL for MySQL / MariaDB.
//
// WARNING: MySQL performs an implicit COMMIT on every DDL statement.
// Transactional rollback of DDL is NOT possible. The Runner will still
// wrap operations in a transaction for the history-table insert, but
// partial DDL failures cannot be rolled back automatically.
type MySQLDialect struct{}

func (MySQLDialect) QuoteIdent(s string) string         { return "`" + s + "`" }
func (MySQLDialect) AutoIncrementClause() string         { return "AUTO_INCREMENT" }
func (MySQLDialect) AutoIncrementType(string) string     { return "" }
func (MySQLDialect) SupportsIfNotExists() bool           { return true }
