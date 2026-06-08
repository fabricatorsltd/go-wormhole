package migrations_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/glebarez/sqlite"

	"github.com/fabricatorsltd/go-wormhole/pkg/discovery"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// reading has two fields sharing one explicit index name: they combine into a
// single composite index, columns in declaration order.
type reading struct {
	ID        int `db:"column:id;primary_key"`
	Station   int `db:"column:station;index:idx_reading_station_message_id"`
	MessageID int `db:"column:message_id;index:idx_reading_station_message_id"`
}

func createIndexOp(ops []migrations.MigrationOp, name string) *migrations.CreateIndexOp {
	for _, op := range ops {
		if ci, ok := op.(migrations.CreateIndexOp); ok && ci.Name == name {
			c := ci
			return &c
		}
	}
	return nil
}

// Two fields sharing an explicit index name generate one multi-column index.
func TestComputeDiff_CompositeIndexFromTags(t *testing.T) {
	meta := schema.Parse(&reading{})
	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, migrations.DatabaseSchema{})

	ci := createIndexOp(ops, "idx_reading_station_message_id")
	if ci == nil {
		t.Fatalf("composite index not generated; ops=%+v", ops)
	}
	if len(ci.Columns) != 2 || ci.Columns[0] != "station" || ci.Columns[1] != "message_id" {
		t.Errorf("columns: got %v, want [station message_id] in declaration order", ci.Columns)
	}
	if ci.Unique {
		t.Error("index should not be unique")
	}
}

// A model diffed against its own snapshot produces no index ops: the composite
// round-trips even though the snapshot does not preserve column order.
func TestComputeDiff_CompositeIndex_NoSpuriousDiff(t *testing.T) {
	meta := schema.Parse(&reading{})
	snap := migrations.MetaToSnapshot([]*model.EntityMeta{meta})

	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, snap)
	for _, op := range ops {
		switch op.(type) {
		case migrations.CreateIndexOp, migrations.DropIndexOp:
			t.Errorf("unchanged composite index produced a diff op: %T %+v", op, op)
		}
	}
}

type uniqReading struct {
	ID      int `db:"column:id;primary_key"`
	Station int `db:"column:station;unique_index:uq_reading_station_seq"`
	Seq     int `db:"column:seq;unique_index:uq_reading_station_seq"`
}

// A composite built from unique_index fields is a UNIQUE index.
func TestComputeDiff_UniqueCompositeIndex(t *testing.T) {
	meta := schema.Parse(&uniqReading{})
	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, migrations.DatabaseSchema{})

	ci := createIndexOp(ops, "uq_reading_station_seq")
	if ci == nil {
		t.Fatalf("unique composite index not generated; ops=%+v", ops)
	}
	if len(ci.Columns) != 2 || !ci.Unique {
		t.Errorf("want 2-column unique index, got columns=%v unique=%v", ci.Columns, ci.Unique)
	}
}

// The AST (compile-free) generator also builds composites from shared index
// names: this is the path the project CLI uses.
func TestDiscovery_CompositeIndexFromTags(t *testing.T) {
	dir := t.TempDir()
	src := "package main\n\n" +
		"type Reading struct {\n" +
		"\tID        int `db:\"column:id;primary_key\"`\n" +
		"\tStation   int `db:\"column:station;index:idx_reading_station_message_id\"`\n" +
		"\tMessageID int `db:\"column:message_id;index:idx_reading_station_message_id\"`\n" +
		"}\n"
	if err := os.WriteFile(filepath.Join(dir, "reading.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	models, err := discovery.DiscoverModels(dir)
	if err != nil {
		t.Fatal(err)
	}
	ops := migrations.ComputeDiff(models, migrations.DatabaseSchema{})
	ci := createIndexOp(ops, "idx_reading_station_message_id")
	if ci == nil || len(ci.Columns) != 2 || ci.Columns[0] != "station" || ci.Columns[1] != "message_id" {
		t.Fatalf("AST path did not build the composite index in declaration order: %+v", ci)
	}
}

// Applying the generated composite index creates a real 2-column index.
func TestApplyCompositeIndex_Creates2ColumnIndex(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE "reading" ("id" INTEGER PRIMARY KEY, "station" INTEGER, "message_id" INTEGER)`); err != nil {
		t.Fatal(err)
	}

	b := migrations.NewBuilderWith(migrations.DefaultDialect{})
	b.AddOp(migrations.CreateIndexOp{
		Name: "idx_reading_station_message_id", Table: "reading",
		Columns: []string{"station", "message_id"},
	})
	for _, stmt := range b.Statements() {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply %q: %v", stmt, err)
		}
	}

	rows, err := db.Query(`PRAGMA index_info('idx_reading_station_message_id')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols := 0
	for rows.Next() {
		cols++
	}
	if cols != 2 {
		t.Errorf("composite index should cover 2 columns, got %d", cols)
	}
}
