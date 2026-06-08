package migrations

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

func metaWithPK(name string) *model.EntityMeta {
	m := &model.EntityMeta{
		Name: name,
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
		},
	}
	m.PrimaryKeys = []*model.FieldMeta{&m.Fields[0]}
	m.PrimaryKey = &m.Fields[0]
	return m
}

// Removing a model from the code produces a DROP TABLE on the next diff: the
// snapshot (built from the previous models) still has the table, the current
// models do not. This is EF's "remove a DbSet" behavior on the up migration.
// Uses MetaToSnapshot as the snapshot source, matching what `migrations add`
// persists, rather than a hand-built schema.
func TestComputeDiff_DropsRemovedModel(t *testing.T) {
	keep, gone := metaWithPK("keep_tbl"), metaWithPK("gone_tbl")
	snap := MetaToSnapshot([]*model.EntityMeta{keep, gone})

	ops := ComputeDiff([]*model.EntityMeta{keep}, snap)

	var drops []string
	for _, op := range ops {
		if d, ok := op.(DropTableOp); ok {
			drops = append(drops, d.Table)
		}
	}
	if len(drops) != 1 || drops[0] != "gone_tbl" {
		t.Fatalf("removing one model should DROP only its table; got drops %v (ops %+v)", drops, ops)
	}
	out := GenerateSQLScript(ops, DefaultDialect{})
	if !strings.Contains(out, "DROP TABLE") || !strings.Contains(out, "gone_tbl") {
		t.Errorf("rendered DDL missing DROP TABLE gone_tbl: %s", out)
	}
}

// The full snapshot-file round-trip that `migrations add` performs: persist the
// snapshot from the prior models, remove one model, reload the snapshot from
// disk, and diff. The removed table drops; the kept table is untouched. This
// guards the persistence path (MetaToSnapshot -> WriteSnapshot -> LoadSnapshot),
// not just the in-memory differ.
func TestSnapshotFile_RemovedModelDropped(t *testing.T) {
	dir := t.TempDir()
	snapPath := filepath.Join(dir, "snapshot.json")

	keep, gone := metaWithPK("keep_tbl"), metaWithPK("gone_tbl")
	if err := WriteSnapshot(snapPath, MetaToSnapshot([]*model.EntityMeta{keep, gone})); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadSnapshot(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	ops := ComputeDiff([]*model.EntityMeta{keep}, loaded)

	var dropped, touchedKeep bool
	for _, op := range ops {
		switch o := op.(type) {
		case DropTableOp:
			if o.Table == "gone_tbl" {
				dropped = true
			}
			if o.Table == "keep_tbl" {
				touchedKeep = true
			}
		case CreateTableOp:
			if o.Table == "keep_tbl" {
				touchedKeep = true
			}
		}
	}
	if !dropped {
		t.Error("removed model not dropped after snapshot round-trip")
	}
	if touchedKeep {
		t.Error("kept model should be untouched")
	}
}

// Applying the generated drop actually removes the table from the database.
func TestApplyDropTable_RemovesTable(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE "gone_tbl" ("id" INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	b := NewBuilderWith(DefaultDialect{})
	b.AddOp(DropTableOp{Table: "gone_tbl"})
	for _, stmt := range b.Statements() {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply %q: %v", stmt, err)
		}
	}

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='gone_tbl'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("table still present after applying the drop")
	}
}

// Known limitation (documented, not a feature): the down migration of a table
// drop is empty. ReverseOps reverses CreateTable/AddColumn/CreateIndex but not
// DropTableOp, which carries no column info, so a dropped table is not recreated
// on rollback. EF makes drops reversible; this test pins the current contract so
// a future reversibility fix flips it deliberately.
func TestReverseOps_DropTableNotReversed(t *testing.T) {
	down := ReverseOps([]MigrationOp{DropTableOp{Table: "gone_tbl"}})
	if len(down) != 0 {
		t.Fatalf("expected no reverse op for a drop today (documented gap), got %+v", down)
	}
}
