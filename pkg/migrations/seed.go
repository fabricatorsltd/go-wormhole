package migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// seedRow is one column-keyed seed record, as authored in a seed file.
type seedRow = map[string]any

// seedSnapshotFile records the primary keys last applied by seeding.
const seedSnapshotFile = ".seed_snapshot.json"

// SeedSnapshot stores, per table, the primary keys the seeder last applied. It
// lets a later run delete rows that were removed from the seed files without
// touching rows the seeder never created (manually inserted data is left alone).
type SeedSnapshot struct {
	Tables map[string][]seedRow `json:"tables"`
}

// compilerForDialect returns a DML compiler whose placeholder and quoting style
// match the migration dialect.
func compilerForDialect(d Dialect) *wsql.Compiler {
	switch d.(type) {
	case PostgresDialect:
		return &wsql.Compiler{Numbered: true}
	case MySQLDialect:
		return &wsql.Compiler{Backtick: true}
	case MSSQLDialect:
		return &wsql.Compiler{AtPrefixed: true, BracketQuote: true, UseTOP: true}
	default:
		return &wsql.Compiler{}
	}
}

// ReconcileSeeds makes the database match the seed files in seedsDir: every row
// is upserted by primary key, and rows that an earlier seed run created but that
// are no longer present are deleted. It runs in one transaction under the
// migration advisory lock and then records the applied primary keys.
//
// This diverges from EF Core, which bakes HasData reconciliation into a
// migration at add time; here it is applied at seed time, which always converges
// the database to the current seed set. Rows are owned by the seed set: a manual
// edit to a seeded row is reverted on the next seed. Seeding is SQL only.
func ReconcileSeeds(ctx context.Context, db *sql.DB, dialect Dialect, models []*model.EntityMeta, seedsDir string) error {
	if dialect == nil {
		dialect = DefaultDialect{}
	}
	byName := make(map[string]*model.EntityMeta, len(models))
	for _, m := range models {
		byName[m.Name] = m
	}

	files, err := loadSeedFiles(seedsDir)
	if err != nil {
		return err
	}
	old, err := loadSeedSnapshot(seedsDir)
	if err != nil {
		return err
	}

	compiler := compilerForDialect(dialect)
	next := make(map[string][]seedRow)

	err = withMigrationLock(ctx, db, dialect, func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for table, rows := range files {
			meta := byName[table]
			if meta == nil {
				_ = tx.Rollback()
				return fmt.Errorf("seed file %q.json has no matching model", table)
			}
			pks, err := reconcileTable(ctx, tx, compiler, meta, rows, old.Tables[table])
			if err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("seed %q: %w", table, err)
			}
			next[table] = pks
		}
		// A table seeded before but with no file now: delete every row the seeder
		// created, then drop it from the snapshot.
		for table, oldPKs := range old.Tables {
			if _, ok := files[table]; ok {
				continue
			}
			meta := byName[table]
			if meta == nil {
				continue
			}
			for _, pk := range oldPKs {
				if err := deletePK(ctx, tx, compiler, meta, pk); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("seed cleanup %q: %w", table, err)
				}
			}
		}
		return tx.Commit()
	})
	if err != nil {
		return err
	}
	return saveSeedSnapshot(seedsDir, SeedSnapshot{Tables: next})
}

// reconcileTable upserts every row and deletes previously-seeded primary keys
// that are absent now. It returns the primary keys present after this run.
func reconcileTable(ctx context.Context, tx *sql.Tx, c *wsql.Compiler, meta *model.EntityMeta, rows, oldPKs []seedRow) ([]seedRow, error) {
	pkFields := primaryKeyFields(meta)
	if len(pkFields) == 0 {
		return nil, fmt.Errorf("model has no primary key; seeding reconciles by primary key")
	}

	newPKs := make([]seedRow, 0, len(rows))
	present := make(map[string]bool, len(rows))
	for _, row := range rows {
		vals, err := fieldValues(meta, row)
		if err != nil {
			return nil, err
		}
		comp := c.InsertOnConflictSeed(meta, vals, conflictFor(meta, row, pkFields))
		if _, err := tx.ExecContext(ctx, comp.SQL, comp.Params...); err != nil {
			return nil, err
		}
		pk := pkOnly(row, pkFields)
		newPKs = append(newPKs, pk)
		present[pkKey(pkFields, pk)] = true
	}

	for _, opk := range oldPKs {
		if present[pkKey(pkFields, opk)] {
			continue
		}
		if err := deletePK(ctx, tx, c, meta, opk); err != nil {
			return nil, err
		}
	}
	return newPKs, nil
}

// fieldValues translates a column-keyed seed row into the FieldName-keyed map the
// compiler expects, coercing JSON values to each column's Go type. (The compiler
// looks values up by FieldName, so a column-keyed map would silently insert
// NULLs.)
func fieldValues(meta *model.EntityMeta, row seedRow) (map[string]any, error) {
	out := make(map[string]any, len(row))
	for col, v := range row {
		f := meta.FieldByColumn(col)
		if f == nil {
			return nil, fmt.Errorf("column %q is not mapped on %q", col, meta.Name)
		}
		out[f.FieldName] = coerceSeedValue(v, f.GoType)
	}
	return out, nil
}

// conflictFor targets the primary-key columns and updates only the non-key,
// non-computed columns the row actually provides.
func conflictFor(meta *model.EntityMeta, row seedRow, pkFields []*model.FieldMeta) provider.ConflictClause {
	isPK := make(map[string]bool, len(pkFields))
	cols := make([]string, 0, len(pkFields))
	for _, f := range pkFields {
		isPK[f.Column] = true
		cols = append(cols, f.Column)
	}
	var update []string
	for col := range row {
		f := meta.FieldByColumn(col)
		if f == nil || isPK[f.Column] || f.Computed {
			continue
		}
		update = append(update, f.Column)
	}
	sort.Strings(update)
	return provider.ConflictClause{Columns: cols, Update: update}
}

// deletePK removes one row by primary key.
func deletePK(ctx context.Context, tx *sql.Tx, c *wsql.Compiler, meta *model.EntityMeta, pk seedRow) error {
	comp := c.Delete(meta, pkValueOf(pk, primaryKeyFields(meta)))
	_, err := tx.ExecContext(ctx, comp.SQL, comp.Params...)
	return err
}

// primaryKeyFields returns the key fields, preferring the composite list.
func primaryKeyFields(meta *model.EntityMeta) []*model.FieldMeta {
	if len(meta.PrimaryKeys) > 0 {
		return meta.PrimaryKeys
	}
	if meta.PrimaryKey != nil {
		return []*model.FieldMeta{meta.PrimaryKey}
	}
	return nil
}

// pkOnly extracts just the primary-key columns from a row.
func pkOnly(row seedRow, pkFields []*model.FieldMeta) seedRow {
	out := make(seedRow, len(pkFields))
	for _, f := range pkFields {
		out[f.Column] = row[f.Column]
	}
	return out
}

// pkValueOf returns the compiler's primary-key value: a scalar for a single key,
// or a column-ordered tuple for a composite key, coerced to the key Go types.
func pkValueOf(pk seedRow, pkFields []*model.FieldMeta) any {
	if len(pkFields) == 1 {
		return coerceSeedValue(pk[pkFields[0].Column], pkFields[0].GoType)
	}
	vals := make([]any, len(pkFields))
	for i, f := range pkFields {
		vals[i] = coerceSeedValue(pk[f.Column], f.GoType)
	}
	return vals
}

// pkKey is a stable identity for a primary key, used to diff old against new.
func pkKey(pkFields []*model.FieldMeta, pk seedRow) string {
	parts := make([]string, len(pkFields))
	for i, f := range pkFields {
		parts[i] = fmt.Sprintf("%v", coerceSeedValue(pk[f.Column], f.GoType))
	}
	return strings.Join(parts, "\x00")
}

// coerceSeedValue narrows a JSON-decoded value to a column's Go type. JSON
// numbers decode to float64, so an integer column or key would otherwise carry a
// float (storing 1.0 or failing the ON CONFLICT match against an integer row).
func coerceSeedValue(v any, t reflect.Type) any {
	f, ok := v.(float64)
	if !ok || t == nil {
		return v
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int64(f)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint64(f)
	default:
		return v
	}
}

func loadSeedFiles(dir string) (map[string][]seedRow, error) {
	out := make(map[string][]seedRow)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") || name == seedSnapshotFile {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		var rows []seedRow
		if err := json.Unmarshal(b, &rows); err != nil {
			return nil, fmt.Errorf("seed file %s: %w", name, err)
		}
		out[strings.TrimSuffix(name, ".json")] = rows
	}
	return out, nil
}

func loadSeedSnapshot(dir string) (SeedSnapshot, error) {
	b, err := os.ReadFile(filepath.Join(dir, seedSnapshotFile))
	if os.IsNotExist(err) {
		return SeedSnapshot{Tables: map[string][]seedRow{}}, nil
	}
	if err != nil {
		return SeedSnapshot{}, err
	}
	var s SeedSnapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return SeedSnapshot{}, err
	}
	if s.Tables == nil {
		s.Tables = map[string][]seedRow{}
	}
	return s, nil
}

func saveSeedSnapshot(dir string, s SeedSnapshot) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	// Write then rename so a crash mid-write cannot leave a truncated snapshot
	// that fails to parse on the next run.
	final := filepath.Join(dir, seedSnapshotFile)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}
