package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/tracker"
)

// DefaultBatchSize is the default number of records to process in a single batch.
const DefaultBatchSize = 1000

// DataMigrator is a placeholder implementation for the CLI "database sync" command.
//
// NOTE: The provider.Provider interface in this repository does not expose schema
// discovery or cursor-based streaming, so full data synchronization cannot be
// implemented generically without additional inputs (entity metas, types, etc.).
type DataMigrator struct {
	sourceProvider      provider.Provider
	destinationProvider provider.Provider
	changeTracker       *tracker.Tracker
	batchSize           int
}

type sqlDBProvider interface {
	SQLDB() *sql.DB
	Name() string
}

func NewDataMigrator(source, destination provider.Provider, changeTracker *tracker.Tracker) *DataMigrator {
	return &DataMigrator{
		sourceProvider:      source,
		destinationProvider: destination,
		changeTracker:       changeTracker,
		batchSize:           DefaultBatchSize,
	}
}

func (dm *DataMigrator) WithBatchSize(size int) *DataMigrator {
	if size > 0 {
		dm.batchSize = size
	}
	return dm
}

func (dm *DataMigrator) FullSync(ctx context.Context) error {
	src, ok := dm.sourceProvider.(sqlDBProvider)
	if !ok {
		return fmt.Errorf("migrations.DataMigrator.FullSync: source provider does not expose SQLDB")
	}
	dst, ok := dm.destinationProvider.(sqlDBProvider)
	if !ok {
		return fmt.Errorf("migrations.DataMigrator.FullSync: destination provider does not expose SQLDB")
	}

	srcDialect := normalizeDialect(src.Name())
	dstDialect := normalizeDialect(dst.Name())
	sourceTables, err := listTables(ctx, src.SQLDB(), srcDialect)
	if err != nil {
		return fmt.Errorf("list source tables: %w", err)
	}
	destinationTables, err := listTables(ctx, dst.SQLDB(), dstDialect)
	if err != nil {
		return fmt.Errorf("list destination tables: %w", err)
	}

	destinationSet := make(map[string]struct{}, len(destinationTables))
	for _, table := range destinationTables {
		destinationSet[table] = struct{}{}
	}
	sharedTables := make([]string, 0, len(sourceTables))
	for _, table := range sourceTables {
		if _, ok := destinationSet[table]; ok {
			sharedTables = append(sharedTables, table)
		}
	}
	sort.Strings(sharedTables)
	if len(sharedTables) == 0 {
		return fmt.Errorf("no shared tables found between source and destination")
	}

	for _, table := range sharedTables {
		sourceColumns, err := tableColumns(ctx, src.SQLDB(), srcDialect, table)
		if err != nil {
			return fmt.Errorf("read source columns for %q: %w", table, err)
		}
		destinationColumns, err := tableColumns(ctx, dst.SQLDB(), dstDialect, table)
		if err != nil {
			return fmt.Errorf("read destination columns for %q: %w", table, err)
		}
		columns := intersectColumns(sourceColumns, destinationColumns)
		if len(columns) == 0 {
			continue
		}

		primaryKeys, err := primaryKeyColumns(ctx, dst.SQLDB(), dstDialect, table)
		if err != nil {
			return fmt.Errorf("read destination primary key for %q: %w", table, err)
		}
		primaryKeys = intersectColumns(primaryKeys, columns)
		if len(primaryKeys) == 0 {
			return fmt.Errorf("table %q has no destination primary key in shared columns", table)
		}

		if err := dm.syncTable(ctx, src.SQLDB(), dst.SQLDB(), srcDialect, dstDialect, table, columns, primaryKeys); err != nil {
			return fmt.Errorf("sync table %q: %w", table, err)
		}
	}

	return nil
}

func (dm *DataMigrator) syncTable(
	ctx context.Context,
	srcDB, dstDB *sql.DB,
	srcDialect, dstDialect, table string,
	columns, primaryKeys []string,
) error {
	selectSQL := fmt.Sprintf("SELECT %s FROM %s", joinQuoted(columns, srcDialect), quoteIdent(srcDialect, table))
	rows, err := srcDB.QueryContext(ctx, selectSQL)
	if err != nil {
		return fmt.Errorf("query source rows: %w", err)
	}
	defer rows.Close()

	columnTypes, _ := rows.ColumnTypes()
	tx, err := dstDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin destination transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for rows.Next() {
		values := make([]any, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("scan source row: %w", err)
		}
		for i := range values {
			dbType := ""
			if i < len(columnTypes) && columnTypes[i] != nil {
				dbType = columnTypes[i].DatabaseTypeName()
			}
			values[i] = normalizeScannedValue(values[i], dbType)
		}

		affected, err := updateRow(ctx, tx, dstDialect, table, columns, primaryKeys, values)
		if err != nil {
			return err
		}
		if affected == 0 {
			if err := insertRow(ctx, tx, dstDialect, table, columns, values); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate source rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit destination transaction: %w", err)
	}
	committed = true
	return nil
}

func updateRow(ctx context.Context, tx *sql.Tx, dialect, table string, columns, primaryKeys []string, values []any) (int64, error) {
	columnIndex := make(map[string]int, len(columns))
	for i, c := range columns {
		columnIndex[c] = i
	}

	pkSet := make(map[string]struct{}, len(primaryKeys))
	for _, c := range primaryKeys {
		pkSet[c] = struct{}{}
	}

	setCols := make([]string, 0, len(columns))
	for _, c := range columns {
		if _, ok := pkSet[c]; !ok {
			setCols = append(setCols, c)
		}
	}

	if len(setCols) == 0 {
		exists, err := rowExists(ctx, tx, dialect, table, primaryKeys, columnIndex, values)
		if err != nil {
			return 0, err
		}
		if exists {
			return 1, nil
		}
		return 0, nil
	}

	args := make([]any, 0, len(setCols)+len(primaryKeys))
	placeholderIndex := 1
	setParts := make([]string, 0, len(setCols))
	for _, c := range setCols {
		setParts = append(setParts, fmt.Sprintf("%s = %s", quoteIdent(dialect, c), bindVar(dialect, placeholderIndex)))
		args = append(args, values[columnIndex[c]])
		placeholderIndex++
	}
	whereParts := make([]string, 0, len(primaryKeys))
	for _, c := range primaryKeys {
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", quoteIdent(dialect, c), bindVar(dialect, placeholderIndex)))
		args = append(args, values[columnIndex[c]])
		placeholderIndex++
	}

	stmt := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		quoteIdent(dialect, table),
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " AND "),
	)
	res, err := tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		return 0, fmt.Errorf("update %q failed: %w", table, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("update %q rows affected: %w", table, err)
	}
	return affected, nil
}

func insertRow(ctx context.Context, tx *sql.Tx, dialect, table string, columns []string, values []any) error {
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = bindVar(dialect, i+1)
	}
	stmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(dialect, table),
		joinQuoted(columns, dialect),
		strings.Join(placeholders, ", "),
	)
	if _, err := tx.ExecContext(ctx, stmt, values...); err != nil {
		return fmt.Errorf("insert into %q failed: %w", table, err)
	}
	return nil
}

func rowExists(ctx context.Context, tx *sql.Tx, dialect, table string, primaryKeys []string, columnIndex map[string]int, values []any) (bool, error) {
	whereParts := make([]string, 0, len(primaryKeys))
	args := make([]any, 0, len(primaryKeys))
	for i, c := range primaryKeys {
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", quoteIdent(dialect, c), bindVar(dialect, i+1)))
		args = append(args, values[columnIndex[c]])
	}
	stmt := fmt.Sprintf(
		"SELECT 1 FROM %s WHERE %s",
		quoteIdent(dialect, table),
		strings.Join(whereParts, " AND "),
	)
	var one int
	err := tx.QueryRowContext(ctx, stmt, args...).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check existing row in %q: %w", table, err)
	}
	return true, nil
}

func listTables(ctx context.Context, db *sql.DB, dialect string) ([]string, error) {
	var q string
	switch dialect {
	case "sqlite":
		q = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	case "postgres":
		q = "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type='BASE TABLE' ORDER BY table_name"
	case "mysql":
		q = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type='BASE TABLE' ORDER BY table_name"
	case "mssql":
		q = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME"
	default:
		q = "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' ORDER BY table_name"
	}

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, rows.Err()
}

func tableColumns(ctx context.Context, db *sql.DB, dialect, table string) ([]string, error) {
	if dialect == "sqlite" {
		rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(dialect, table)))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var cols []string
		for rows.Next() {
			var cid int
			var name, colType string
			var notNull, pk int
			var def sql.NullString
			if err := rows.Scan(&cid, &name, &colType, &notNull, &def, &pk); err != nil {
				return nil, err
			}
			cols = append(cols, name)
		}
		return cols, rows.Err()
	}

	tableLiteral := quoteLiteral(table)
	var q string
	switch dialect {
	case "postgres":
		q = fmt.Sprintf(
			"SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '%s' ORDER BY ordinal_position",
			tableLiteral,
		)
	case "mysql":
		q = fmt.Sprintf(
			"SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '%s' ORDER BY ordinal_position",
			tableLiteral,
		)
	case "mssql":
		q = fmt.Sprintf(
			"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION",
			tableLiteral,
		)
	default:
		q = fmt.Sprintf(
			"SELECT column_name FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position",
			tableLiteral,
		)
	}

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func primaryKeyColumns(ctx context.Context, db *sql.DB, dialect, table string) ([]string, error) {
	if dialect == "sqlite" {
		rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(dialect, table)))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		type pkEntry struct {
			order int
			name  string
		}
		var entries []pkEntry
		for rows.Next() {
			var cid int
			var name, colType string
			var notNull, pk int
			var def sql.NullString
			if err := rows.Scan(&cid, &name, &colType, &notNull, &def, &pk); err != nil {
				return nil, err
			}
			if pk > 0 {
				entries = append(entries, pkEntry{order: pk, name: name})
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].order < entries[j].order })
		out := make([]string, len(entries))
		for i := range entries {
			out[i] = entries[i].name
		}
		return out, nil
	}

	tableLiteral := quoteLiteral(table)
	var q string
	switch dialect {
	case "postgres":
		q = fmt.Sprintf(
			`SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = current_schema()
  AND tc.table_name = '%s'
ORDER BY kcu.ordinal_position`, tableLiteral)
	case "mysql":
		q = fmt.Sprintf(
			`SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = DATABASE()
  AND tc.table_name = '%s'
ORDER BY kcu.ordinal_position`, tableLiteral)
	case "mssql":
		q = fmt.Sprintf(
			`SELECT kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND tc.TABLE_NAME = '%s'
ORDER BY kcu.ORDINAL_POSITION`, tableLiteral)
	default:
		return nil, nil
	}

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func intersectColumns(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, c := range right {
		rightSet[c] = struct{}{}
	}
	out := make([]string, 0, len(left))
	for _, c := range left {
		if _, ok := rightSet[c]; ok {
			out = append(out, c)
		}
	}
	return out
}

func normalizeDialect(name string) string {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "postgres"), strings.Contains(n, "pgx"):
		return "postgres"
	case strings.Contains(n, "mysql"), strings.Contains(n, "maria"):
		return "mysql"
	case strings.Contains(n, "sqlite"):
		return "sqlite"
	case strings.Contains(n, "sqlserver"), strings.Contains(n, "mssql"):
		return "mssql"
	default:
		return n
	}
}

func bindVar(dialect string, idx int) string {
	switch dialect {
	case "postgres":
		return fmt.Sprintf("$%d", idx)
	case "mssql":
		return fmt.Sprintf("@p%d", idx)
	default:
		return "?"
	}
}

func quoteIdent(dialect, name string) string {
	switch dialect {
	case "mysql":
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	default:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

func quoteLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func joinQuoted(columns []string, dialect string) string {
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = quoteIdent(dialect, c)
	}
	return strings.Join(quoted, ", ")
}

func normalizeScannedValue(v any, dbType string) any {
	bytes, ok := v.([]byte)
	if !ok {
		return v
	}
	upperType := strings.ToUpper(dbType)
	if strings.Contains(upperType, "BLOB") ||
		strings.Contains(upperType, "BINARY") ||
		strings.Contains(upperType, "BYTEA") {
		return bytes
	}
	return string(bytes)
}
