package migrations_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/glebarez/sqlite"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
)

func driftSchema(tables ...*migrations.TableSchema) migrations.DatabaseSchema {
	s := migrations.DatabaseSchema{Tables: map[string]*migrations.TableSchema{}}
	for _, t := range tables {
		s.Tables[t.Name] = t
	}
	return s
}

func driftTable(name string, cols ...*migrations.ColumnDef) *migrations.TableSchema {
	t := &migrations.TableSchema{Name: name, Columns: map[string]*migrations.ColumnDef{}}
	for _, c := range cols {
		t.Columns[c.Name] = c
	}
	return t
}

func driftCol(name, sqlType string) *migrations.ColumnDef {
	return &migrations.ColumnDef{Name: name, SQLType: sqlType}
}

func TestDetectDrift_NoDrift(t *testing.T) {
	snap := driftSchema(driftTable("users", driftCol("id", "INTEGER"), driftCol("email", "TEXT")))
	live := driftSchema(driftTable("users", driftCol("id", "INTEGER"), driftCol("email", "TEXT")))
	if d := migrations.DetectDrift(snap, live); len(d) != 0 {
		t.Fatalf("expected no drift, got %v", d)
	}
}

func TestDetectDrift_StructuralCases(t *testing.T) {
	snap := driftSchema(
		driftTable("users", driftCol("id", "INTEGER"), driftCol("email", "TEXT")),
		driftTable("orders", driftCol("id", "INTEGER")),
	)
	live := driftSchema(
		driftTable("users", driftCol("id", "INTEGER")), // email missing
		driftTable("audit", driftCol("id", "INTEGER")), // extra table; orders missing
	)

	drifts := migrations.DetectDrift(snap, live)
	kinds := map[migrations.DriftKind]string{}
	for _, d := range drifts {
		kinds[d.Kind] = d.Table + "." + d.Column
	}
	if kinds[migrations.DriftMissingColumn] != "users.email" {
		t.Errorf("missing column: got %q", kinds[migrations.DriftMissingColumn])
	}
	if kinds[migrations.DriftMissingTable] != "orders." {
		t.Errorf("missing table: got %q", kinds[migrations.DriftMissingTable])
	}
	if kinds[migrations.DriftExtraTable] != "audit." {
		t.Errorf("extra table: got %q", kinds[migrations.DriftExtraTable])
	}
}

// A real type change (different Go-type bucket) is drift...
func TestDetectDrift_RealTypeChange(t *testing.T) {
	snap := driftSchema(driftTable("t", driftCol("n", "INTEGER")))
	live := driftSchema(driftTable("t", driftCol("n", "TEXT")))
	drifts := migrations.DetectDrift(snap, live)
	if len(drifts) != 1 || drifts[0].Kind != migrations.DriftColumnType {
		t.Fatalf("expected one column_type drift, got %v", drifts)
	}
}

// ...but dialect spelling differences for the same logical type are NOT drift.
func TestDetectDrift_NoFalsePositiveOnDialectTypes(t *testing.T) {
	snap := driftSchema(driftTable("t",
		driftCol("created_at", "TIMESTAMP"),
		driftCol("name", "VARCHAR(255)"),
		driftCol("count", "INTEGER"),
	))
	// What Postgres introspection would report for the same columns.
	live := driftSchema(driftTable("t",
		driftCol("created_at", "TIMESTAMP WITH TIME ZONE"),
		driftCol("name", "TEXT"),
		driftCol("count", "INT4"),
	))
	if d := migrations.DetectDrift(snap, live); len(d) != 0 {
		t.Fatalf("dialect type spellings should not be drift, got %v", d)
	}
}

// End to end: introspect a real SQLite database and detect a column dropped
// out of band against the model snapshot.
func TestIntrospectAndDrift_E2E(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	for _, s := range []string{
		`CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "email" TEXT, "created_at" TIMESTAMP)`,
		`CREATE TABLE "_wormhole_migrations_history" ("migration_id" TEXT PRIMARY KEY, "applied_at" TIMESTAMP)`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	live, err := migrations.IntrospectSchema(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	// History table is excluded; user table is captured.
	if _, ok := live.Tables["_wormhole_migrations_history"]; ok {
		t.Error("history table should be excluded from introspection")
	}
	if live.Tables["users"] == nil || len(live.Tables["users"].Columns) != 3 {
		t.Fatalf("users not introspected correctly: %+v", live.Tables["users"])
	}

	// Snapshot says users should also have a "name" column that the DB lacks.
	snap := driftSchema(driftTable("users",
		driftCol("id", "INTEGER"), driftCol("email", "TEXT"), driftCol("created_at", "TIMESTAMP"), driftCol("name", "TEXT"),
	))
	drifts := migrations.DetectDrift(snap, live)
	if len(drifts) != 1 || drifts[0].Kind != migrations.DriftMissingColumn || drifts[0].Column != "name" {
		t.Fatalf("expected one missing-column drift for users.name, got %v", drifts)
	}
}

func TestIntrospectSchema_Empty(t *testing.T) {
	db, _ := sql.Open("sqlite", ":memory:")
	defer db.Close()
	db.SetMaxOpenConns(1)
	live, err := migrations.IntrospectSchema(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(live, migrations.DatabaseSchema{Tables: map[string]*migrations.TableSchema{}}) {
		t.Errorf("empty db should yield empty schema, got %+v", live)
	}
}
