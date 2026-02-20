package migrations

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
