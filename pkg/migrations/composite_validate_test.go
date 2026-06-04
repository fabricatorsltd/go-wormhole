package migrations_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

// ValidateModels rejects a composite-PK entity (the DDL generator would emit one
// PRIMARY KEY clause per column, which is invalid SQL).
func TestValidateModels_RejectsComposite(t *testing.T) {
	meta := &model.EntityMeta{
		Name: "order_line",
		Fields: []model.FieldMeta{
			{FieldName: "OrderID", Column: "order_id", PrimaryKey: true},
			{FieldName: "LineNo", Column: "line_no", PrimaryKey: true},
		},
	}
	meta.PrimaryKeys = []*model.FieldMeta{&meta.Fields[0], &meta.Fields[1]}
	meta.PrimaryKey = &meta.Fields[0]

	err := migrations.ValidateModels([]*model.EntityMeta{meta})
	if err == nil || !strings.Contains(err.Error(), "composite primary key") {
		t.Fatalf("want composite rejection, got %v", err)
	}
}

// ValidateModels rejects a computed column (generated-column DDL is not yet
// emitted; the column must be defined by hand).
func TestValidateModels_RejectsComputed(t *testing.T) {
	meta := &model.EntityMeta{
		Name: "c_item",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true},
			{FieldName: "Total", Column: "total", Computed: true},
		},
	}
	meta.PrimaryKeys = []*model.FieldMeta{&meta.Fields[0]}
	meta.PrimaryKey = &meta.Fields[0]

	err := migrations.ValidateModels([]*model.EntityMeta{meta})
	if err == nil || !strings.Contains(err.Error(), "computed") {
		t.Fatalf("want computed rejection, got %v", err)
	}
}

// A single-PK model passes validation.
func TestValidateModels_AllowsSingle(t *testing.T) {
	meta := &model.EntityMeta{
		Name:   "users",
		Fields: []model.FieldMeta{{FieldName: "ID", Column: "id", PrimaryKey: true}},
	}
	meta.PrimaryKeys = []*model.FieldMeta{&meta.Fields[0]}
	meta.PrimaryKey = &meta.Fields[0]

	if err := migrations.ValidateModels([]*model.EntityMeta{meta}); err != nil {
		t.Fatalf("single PK should pass, got %v", err)
	}
}
