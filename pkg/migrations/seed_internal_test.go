package migrations

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

// A JSON number (float64) is narrowed to an integer column's type so the value
// stored and the ON CONFLICT match are integers, not 1.0.
func TestCoerceSeedValue_IntFromFloat(t *testing.T) {
	got := coerceSeedValue(float64(1), reflect.TypeOf(int(0)))
	if v, ok := got.(int64); !ok || v != 1 {
		t.Fatalf("int column: got %#v, want int64(1)", got)
	}
	// A string passes through unchanged.
	if got := coerceSeedValue("alice", reflect.TypeOf("")); got != "alice" {
		t.Fatalf("string passthrough: got %#v", got)
	}
	// A float column keeps its float.
	if got := coerceSeedValue(float64(1.5), reflect.TypeOf(float64(0))); got != 1.5 {
		t.Fatalf("float column: got %#v", got)
	}
}

// fieldValues keys the result by FieldName (what the compiler looks up), not by
// column, so values actually land instead of inserting NULLs.
func TestFieldValues_KeyedByFieldName(t *testing.T) {
	meta := &model.EntityMeta{
		Name: "seed_user",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
		},
	}
	meta.BuildIndex()

	vals, err := fieldValues(meta, seedRow{"id": float64(7), "name": "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := vals["ID"].(int64); !ok || v != 7 {
		t.Errorf("ID should be int64(7) keyed by FieldName, got %#v", vals["ID"])
	}
	if vals["Name"] != "alice" {
		t.Errorf("Name should be alice keyed by FieldName, got %#v", vals["Name"])
	}
	if _, err := fieldValues(meta, seedRow{"nope": 1}); err == nil {
		t.Error("unknown column should error")
	}
}
