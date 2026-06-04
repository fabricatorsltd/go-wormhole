package migrations_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	_ "github.com/glebarez/sqlite"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

// Every op kind must survive a JSON round-trip. If a new op type is added but
// not registered for (un)marshaling, this fails rather than silently dropping it.
func TestMigration_AllOpKindsRoundTrip(t *testing.T) {
	ops := []migrations.MigrationOp{
		migrations.CreateTableOp{Table: "t", Columns: []migrations.ColumnDef{
			{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
			{Name: "user_id", SQLType: "INTEGER", Ref: &migrations.ColumnRef{Table: "u", Column: "id"}},
			{Name: "email", SQLType: "TEXT", Indexed: true, Unique: true},
		}},
		migrations.DropTableOp{Table: "t"},
		migrations.AddColumnOp{Table: "t", Column: migrations.ColumnDef{Name: "age", SQLType: "INTEGER", Nullable: true}},
		migrations.DropColumnOp{Table: "t", Column: "age"},
		migrations.AlterColumnOp{Table: "t", Column: migrations.ColumnDef{Name: "age", SQLType: "BIGINT"}},
		migrations.CreateIndexOp{Name: "idx_t_email", Table: "t", Columns: []string{"email"}, Unique: true},
		migrations.DropIndexOp{Name: "idx_t_email", Table: "t"},
		migrations.RawSQLOp{SQL: "UPDATE t SET age = 0 WHERE age IS NULL"},
	}

	b, err := migrations.MarshalMigration("001_all", ops, nil)
	if err != nil {
		t.Fatal(err)
	}
	id, up, _, err := migrations.UnmarshalMigration(b)
	if err != nil {
		t.Fatal(err)
	}
	if id != "001_all" {
		t.Errorf("id: got %q", id)
	}
	if !reflect.DeepEqual(up, ops) {
		t.Errorf("ops did not round-trip:\n got %#v\nwant %#v", up, ops)
	}
	if len(up) != 8 {
		t.Errorf("expected all 8 op kinds, got %d", len(up))
	}
}

// Blocking case: a column whose type is only known via GoType (untagged/reflect
// path) must serialize with a concrete SQLType, or apply would emit broken DDL.
func TestMigration_NormalizesGoTypeColumn(t *testing.T) {
	ops := []migrations.MigrationOp{
		migrations.AddColumnOp{Table: "t", Column: migrations.ColumnDef{Name: "n", GoType: reflect.TypeOf(int64(0))}},
	}
	b, err := migrations.MarshalMigration("001", ops, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, up, _, err := migrations.UnmarshalMigration(b)
	if err != nil {
		t.Fatal(err)
	}
	col := up[0].(migrations.AddColumnOp).Column
	if col.SQLType != "BIGINT" {
		t.Errorf("GoType not resolved on serialize: SQLType=%q, want BIGINT", col.SQLType)
	}
}

func tbl(name string, fields ...model.FieldMeta) *model.EntityMeta {
	return &model.EntityMeta{Name: name, Fields: fields}
}

// The key invariant: the schema rebuilt from the serialized up-ops of a
// migration sequence must agree with the final model (ComputeDiff is empty).
// This guards both serialization fidelity and .json-vs-snapshot consistency.
func TestMigration_RebuiltSchemaMatchesModel(t *testing.T) {
	v1 := tbl("acct",
		model.FieldMeta{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
		model.FieldMeta{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
	)
	v2 := tbl("acct",
		model.FieldMeta{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
		model.FieldMeta{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
		model.FieldMeta{FieldName: "Email", Column: "email", GoType: reflect.TypeOf(""), Indexed: true, Unique: true},
		model.FieldMeta{FieldName: "Age", Column: "age", GoType: reflect.TypeOf(0)},
	)

	// Migration 1: empty -> v1. Migration 2: v1 -> v2. Serialize and decode each.
	ops1 := migrations.ComputeDiff([]*model.EntityMeta{v1}, migrations.DatabaseSchema{})
	ops2 := migrations.ComputeDiff([]*model.EntityMeta{v2}, migrations.MetaToSnapshot([]*model.EntityMeta{v1}))

	decode := func(id string, ops []migrations.MigrationOp) []migrations.MigrationOp {
		b, err := migrations.MarshalMigration(id, ops, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, up, _, err := migrations.UnmarshalMigration(b)
		if err != nil {
			t.Fatal(err)
		}
		return up
	}

	rebuilt := migrations.RebuildSnapshot([][]migrations.MigrationOp{
		decode("001", ops1),
		decode("002", ops2),
	})

	// The model diffs to zero against the schema replayed from the .json ops.
	if drift := migrations.ComputeDiff([]*model.EntityMeta{v2}, rebuilt); len(drift) != 0 {
		t.Fatalf("rebuilt schema drifted from model: %#v", drift)
	}
}

func TestReverseOps(t *testing.T) {
	up := []migrations.MigrationOp{
		migrations.CreateTableOp{Table: "t", Columns: []migrations.ColumnDef{{Name: "id", SQLType: "INTEGER"}}},
		migrations.AddColumnOp{Table: "t", Column: migrations.ColumnDef{Name: "age", SQLType: "INTEGER"}},
		migrations.CreateIndexOp{Name: "idx_t_age", Table: "t", Columns: []string{"age"}},
	}
	down := migrations.ReverseOps(up)
	// Reverse order: drop index, drop column, drop table.
	if len(down) != 3 {
		t.Fatalf("got %d down ops, want 3", len(down))
	}
	if _, ok := down[0].(migrations.DropIndexOp); !ok {
		t.Errorf("down[0]: got %T, want DropIndexOp", down[0])
	}
	if _, ok := down[2].(migrations.DropTableOp); !ok {
		t.Errorf("down[2]: got %T, want DropTableOp", down[2])
	}
}

// End-to-end: file-based migrations apply to a real database with no project
// compilation, are idempotent, and the snapshot file in the directory is skipped.
func TestApplyPendingFiles_E2E(t *testing.T) {
	dir := t.TempDir()

	m1, _ := migrations.MarshalMigration("001_users", []migrations.MigrationOp{
		migrations.CreateTableOp{Table: "users", Columns: []migrations.ColumnDef{
			{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
			{Name: "name", SQLType: "TEXT"},
		}},
	}, nil)
	m2, _ := migrations.MarshalMigration("002_add_age", []migrations.MigrationOp{
		migrations.AddColumnOp{Table: "users", Column: migrations.ColumnDef{Name: "age", SQLType: "INTEGER", Nullable: true}},
	}, nil)
	writeFile(t, filepath.Join(dir, "001_users.json"), m1)
	writeFile(t, filepath.Join(dir, "002_add_age.json"), m2)
	// A snapshot file in the same dir must be ignored by the runner.
	if err := migrations.WriteSnapshot(filepath.Join(dir, "schema_snapshot.json"), migrations.DatabaseSchema{Tables: map[string]*migrations.TableSchema{}}); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	applied, err := migrations.ApplyPendingFiles(ctx, db, migrations.DefaultDialect{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(applied, []string{"001_users", "002_add_age"}) {
		t.Fatalf("applied: got %v", applied)
	}

	// The schema is real: age column exists.
	if _, err := db.Exec(`INSERT INTO "users" ("name","age") VALUES ('a', 1)`); err != nil {
		t.Fatalf("schema not applied: %v", err)
	}

	// Idempotent: a second run applies nothing.
	again, err := migrations.ApplyPendingFiles(ctx, db, migrations.DefaultDialect{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Errorf("second apply: got %v, want none", again)
	}
}

func writeFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
