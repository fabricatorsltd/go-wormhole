package schema

import "testing"

type untaggedEntity struct {
	ID       int `db:"primary_key; auto_increment"`
	FullName string
	age      int
}

func TestParseIncludesExportedUntaggedFields(t *testing.T) {
	entity := &untaggedEntity{age: 7}
	meta := Parse(entity)

	if field := meta.Field("FullName"); field == nil || field.Column != "full_name" {
		t.Fatalf("untagged exported field = %+v, want full_name mapping", field)
	}
	if field := meta.Field("age"); field != nil {
		t.Fatalf("unexported field leaked into mapping: %+v", field)
	}
	if entity.age != 7 {
		t.Fatalf("private test field changed to %d", entity.age)
	}
}

type untaggedOwnedValue struct {
	Public  string
	private string
}

type untaggedOwnedEntity struct {
	ID    int                `db:"primary_key"`
	Value untaggedOwnedValue `db:"owned"`
}

func TestParseOwnedIncludesOnlyExportedUntaggedFields(t *testing.T) {
	entity := &untaggedOwnedEntity{Value: untaggedOwnedValue{private: "not persisted"}}
	meta := Parse(entity)

	if field := meta.FieldByColumn("value_public"); field == nil {
		t.Fatal("exported untagged owned field is missing")
	}
	if field := meta.FieldByColumn("value_private"); field != nil {
		t.Fatalf("unexported owned field leaked into mapping: %+v", field)
	}
	if entity.Value.private != "not persisted" {
		t.Fatalf("private owned test field changed to %q", entity.Value.private)
	}
}
