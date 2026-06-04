package migrations

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

func ownedMetaFixture() *model.EntityMeta {
	meta := &model.EntityMeta{
		Name: "owned_customer",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0), Path: []int{0}},
			{FieldName: "Address.Street", Column: "address_street", GoType: reflect.TypeOf(""), Path: []int{1, 0}},
			{FieldName: "Address.City", Column: "address_city", GoType: reflect.TypeOf(""), Path: []int{1, 1}},
		},
	}
	meta.PrimaryKeys = []*model.FieldMeta{&meta.Fields[0]}
	meta.PrimaryKey = &meta.Fields[0]
	return meta
}

// Flattened owned columns are ordinary columns to the differ: they appear in the
// generated CREATE TABLE just like any declared field.
func TestCreateTableFromMeta_EmitsOwnedColumns(t *testing.T) {
	op := createTableFromMeta(ownedMetaFixture(), nil)
	got := make(map[string]bool, len(op.Columns))
	for _, c := range op.Columns {
		got[c.Name] = true
	}
	for _, want := range []string{"id", "address_street", "address_city"} {
		if !got[want] {
			t.Errorf("CREATE TABLE missing column %q (have %v)", want, got)
		}
	}
}

// Owned types use only ordinary scalar columns, so model validation accepts them.
func TestValidateModels_AcceptsOwned(t *testing.T) {
	if err := ValidateModels([]*model.EntityMeta{ownedMetaFixture()}); err != nil {
		t.Fatalf("owned-type model should validate, got %v", err)
	}
}
