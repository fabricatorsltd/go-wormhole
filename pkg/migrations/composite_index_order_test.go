package migrations_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/discovery"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

func dropIndexOp(ops []migrations.MigrationOp, name string) *migrations.DropIndexOp {
	for _, op := range ops {
		if di, ok := op.(migrations.DropIndexOp); ok && di.Name == name {
			d := di
			return &d
		}
	}
	return nil
}

// MessageID is declared first but positioned 2; Station is declared second but
// positioned 1. The index column order follows the positions, not declaration.
type posReversed struct {
	ID        int `db:"column:id;primary_key"`
	MessageID int `db:"column:message_id;index:ix:2"`
	Station   int `db:"column:station;index:ix:1"`
}

func TestComputeDiff_ExplicitOrder_FollowsPositionNotDeclaration(t *testing.T) {
	meta := schema.Parse(&posReversed{})
	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, migrations.DatabaseSchema{})
	ci := createIndexOp(ops, "ix")
	if ci == nil || len(ci.Columns) != 2 || ci.Columns[0] != "station" || ci.Columns[1] != "message_id" {
		t.Fatalf("columns must follow :N order [station, message_id], got %+v", ci)
	}
}

type posAB struct {
	ID        int `db:"column:id;primary_key;table:reading"`
	Station   int `db:"column:station;index:ix:1"`
	MessageID int `db:"column:message_id;index:ix:2"`
}
type posBA struct {
	ID        int `db:"column:id;primary_key;table:reading"`
	Station   int `db:"column:station;index:ix:2"`
	MessageID int `db:"column:message_id;index:ix:1"`
}
type posNone struct {
	ID        int `db:"column:id;primary_key;table:reading"`
	Station   int `db:"column:station;index:ix"`
	MessageID int `db:"column:message_id;index:ix"`
}

// Changing the explicit order (1<->2) against a snapshot that recorded positions
// regenerates the index: one drop, one create.
func TestComputeDiff_ExplicitOrder_FlipRegenerates(t *testing.T) {
	snap := migrations.MetaToSnapshot([]*model.EntityMeta{schema.Parse(&posAB{})})
	ops := migrations.ComputeDiff([]*model.EntityMeta{schema.Parse(&posBA{})}, snap)

	if createIndexOp(ops, "ix") == nil || dropIndexOp(ops, "ix") == nil {
		t.Fatalf("flipping the order must drop+create the index; ops=%+v", ops)
	}
	ci := createIndexOp(ops, "ix")
	if ci.Columns[0] != "message_id" || ci.Columns[1] != "station" {
		t.Errorf("recreated index should be in the new order [message_id, station], got %v", ci.Columns)
	}
}

// Upgrading a project (snapshot predates positions) and adding positions to an
// existing index is a no-op: the snapshot side is unordered, so the diff falls
// back to a set comparison and does not rebuild.
func TestComputeDiff_ExplicitOrder_UpgradeNoOp(t *testing.T) {
	snap := migrations.MetaToSnapshot([]*model.EntityMeta{schema.Parse(&posNone{})}) // no positions recorded
	ops := migrations.ComputeDiff([]*model.EntityMeta{schema.Parse(&posAB{})}, snap)

	for _, op := range ops {
		switch op.(type) {
		case migrations.CreateIndexOp, migrations.DropIndexOp:
			t.Errorf("retrofitting positions on an unordered snapshot must not rebuild: %T %+v", op, op)
		}
	}
}

type mixedIdx struct {
	ID int `db:"column:id;primary_key;table:m"`
	A  int `db:"column:a;index:ix:1"`
	B  int `db:"column:b;index:ix"`
}
type dupIdx struct {
	ID int `db:"column:id;primary_key;table:d"`
	A  int `db:"column:a;index:ix:1"`
	B  int `db:"column:b;index:ix:1"`
}

// A composite index that mixes explicit and implicit positions is rejected.
func TestValidateModels_MixedIndexPositions(t *testing.T) {
	err := migrations.ValidateModels([]*model.EntityMeta{schema.Parse(&mixedIdx{})})
	if err == nil || !strings.Contains(err.Error(), "mixes") {
		t.Fatalf("want mixed-position rejection, got %v", err)
	}
}

// Two fields claiming the same position in one index is rejected.
func TestValidateModels_DuplicateIndexPositions(t *testing.T) {
	err := migrations.ValidateModels([]*model.EntityMeta{schema.Parse(&dupIdx{})})
	if err == nil || !strings.Contains(err.Error(), "repeats") {
		t.Fatalf("want duplicate-position rejection, got %v", err)
	}
}

type negIdx struct {
	ID int `db:"column:id;primary_key;table:n"`
	A  int `db:"column:a;index:ix:-1"`
	B  int `db:"column:b;index:ix:2"`
}

// A negative (non-1-based) position is rejected rather than silently reordering.
func TestValidateModels_NegativeIndexPosition(t *testing.T) {
	err := migrations.ValidateModels([]*model.EntityMeta{schema.Parse(&negIdx{})})
	if err == nil || !strings.Contains(err.Error(), "invalid column position") {
		t.Fatalf("want invalid-position rejection, got %v", err)
	}
}

// The AST/CLI path also honours explicit positions, in position order.
func TestDiscovery_ExplicitOrder(t *testing.T) {
	dir := t.TempDir()
	src := "package main\n\n" +
		"type Reading struct {\n" +
		"\tID        int `db:\"column:id;primary_key\"`\n" +
		"\tMessageID int `db:\"column:message_id;index:ix:2\"`\n" +
		"\tStation   int `db:\"column:station;index:ix:1\"`\n" +
		"}\n"
	if err := os.WriteFile(filepath.Join(dir, "reading.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	models, err := discovery.DiscoverModels(dir)
	if err != nil {
		t.Fatal(err)
	}
	ops := migrations.ComputeDiff(models, migrations.DatabaseSchema{})
	ci := createIndexOp(ops, "ix")
	if ci == nil || len(ci.Columns) != 2 || ci.Columns[0] != "station" || ci.Columns[1] != "message_id" {
		t.Fatalf("AST path must honour :N order [station, message_id], got %+v", ci)
	}
}
