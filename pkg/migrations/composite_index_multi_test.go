package migrations_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/discovery"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

// message_id belongs to BOTH its own single-column index and the composite (at
// position 2). station is the composite's leading column.
type multiIdx struct {
	ID        int `db:"column:id;primary_key"`
	Station   int `db:"column:station;index:idx_rs:1"`
	MessageID int `db:"column:message_id;index:idx_msg,idx_rs:2"`
}

func TestComputeDiff_FieldInMultipleIndexes(t *testing.T) {
	meta := schema.Parse(&multiIdx{})
	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, migrations.DatabaseSchema{})

	msg := createIndexOp(ops, "idx_msg")
	if msg == nil || len(msg.Columns) != 1 || msg.Columns[0] != "message_id" {
		t.Fatalf("standalone idx_msg should cover [message_id], got %+v", msg)
	}
	rs := createIndexOp(ops, "idx_rs")
	if rs == nil || len(rs.Columns) != 2 || rs.Columns[0] != "station" || rs.Columns[1] != "message_id" {
		t.Fatalf("composite idx_rs should cover [station, message_id], got %+v", rs)
	}
}

// The AST/CLI path also lets a field join several indexes.
func TestDiscovery_FieldInMultipleIndexes(t *testing.T) {
	dir := t.TempDir()
	src := "package main\n\n" +
		"type Reading struct {\n" +
		"\tID        int `db:\"column:id;primary_key\"`\n" +
		"\tStation   int `db:\"column:station;index:idx_rs:1\"`\n" +
		"\tMessageID int `db:\"column:message_id;index:idx_msg,idx_rs:2\"`\n" +
		"}\n"
	if err := os.WriteFile(filepath.Join(dir, "reading.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	models, err := discovery.DiscoverModels(dir)
	if err != nil {
		t.Fatal(err)
	}
	ops := migrations.ComputeDiff(models, migrations.DatabaseSchema{})
	if createIndexOp(ops, "idx_msg") == nil || createIndexOp(ops, "idx_rs") == nil {
		t.Fatalf("AST path must build both indexes; ops=%+v", ops)
	}
	rs := createIndexOp(ops, "idx_rs")
	if len(rs.Columns) != 2 || rs.Columns[0] != "station" || rs.Columns[1] != "message_id" {
		t.Errorf("composite order wrong via AST: %v", rs.Columns)
	}
}

type dupColIdx struct {
	ID int `db:"column:id;primary_key;table:dc"`
	A  int `db:"column:a;index:dup,dup"`
}

// Listing the same index twice on one field would build a malformed (a, a)
// index; it is rejected at generation time rather than failing at apply.
func TestValidateModels_DuplicateColumnInIndex(t *testing.T) {
	err := migrations.ValidateModels([]*model.EntityMeta{schema.Parse(&dupColIdx{})})
	if err == nil {
		t.Fatal("want rejection of a column listed twice in one index, got nil")
	}
}

// One field can be in a unique index and a non-unique index at the same time.
type multiUniq struct {
	ID    int `db:"column:id;primary_key"`
	Email int `db:"column:email;unique_index:uq_email;index:idx_email_lookup"`
}

func TestComputeDiff_FieldInUniqueAndNonUniqueIndex(t *testing.T) {
	meta := schema.Parse(&multiUniq{})
	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, migrations.DatabaseSchema{})

	uq := createIndexOp(ops, "uq_email")
	if uq == nil || !uq.Unique {
		t.Errorf("uq_email should be unique, got %+v", uq)
	}
	lookup := createIndexOp(ops, "idx_email_lookup")
	if lookup == nil || lookup.Unique {
		t.Errorf("idx_email_lookup should be non-unique, got %+v", lookup)
	}
}

// A multi-index model diffed against its own snapshot produces no index ops.
func TestComputeDiff_MultiIndex_NoSpuriousDiff(t *testing.T) {
	meta := schema.Parse(&multiIdx{})
	snap := migrations.MetaToSnapshot([]*model.EntityMeta{meta})
	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, snap)
	for _, op := range ops {
		switch op.(type) {
		case migrations.CreateIndexOp, migrations.DropIndexOp:
			t.Errorf("unchanged multi-index model produced a diff op: %T %+v", op, op)
		}
	}
}

// A snapshot written before the Indexes list existed (only the legacy single
// fields populated) still diffs correctly: effectiveRefs reconstructs membership
// from the single fields, so a composite model is a no-op against it.
func TestComputeDiff_MultiIndex_OldSnapshotBackCompat(t *testing.T) {
	meta := schema.Parse(&reading{}) // composite via a shared index name
	snap := migrations.MetaToSnapshot([]*model.EntityMeta{meta})
	for _, tbl := range snap.Tables {
		for _, c := range tbl.Columns {
			c.Indexes = nil // simulate a pre-1.12.1 snapshot
		}
	}
	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, snap)
	for _, op := range ops {
		switch op.(type) {
		case migrations.CreateIndexOp, migrations.DropIndexOp:
			t.Errorf("old snapshot (no Indexes list) must still diff to zero index ops: %T %+v", op, op)
		}
	}
}
