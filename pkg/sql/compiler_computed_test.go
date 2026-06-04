package sql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func computedMeta() *model.EntityMeta {
	meta := &model.EntityMeta{
		Name: "c_item",
		GoType: reflect.TypeOf(struct {
			ID    int
			Qty   int
			Total int
		}{}),
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, AutoIncr: true},
			{FieldName: "Qty", Column: "qty"},
			{FieldName: "Total", Column: "total", Computed: true},
		},
	}
	meta.PrimaryKey = &meta.Fields[0]
	meta.BuildIndex()
	return meta
}

// INSERT omits a computed column (the database generates it).
func TestCompiler_Insert_OmitsComputed(t *testing.T) {
	c := &wsql.Compiler{}
	meta := computedMeta()
	out := c.Insert(meta, map[string]any{"ID": 0, "Qty": 3, "Total": 99})

	want := `INSERT INTO "c_item" ("qty") VALUES (?)`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	if len(out.Params) != 1 || out.Params[0] != 3 {
		t.Errorf("params: got %v, want [3]", out.Params)
	}
}

// UPDATE never writes a computed column, even if it appears in the changed set.
func TestCompiler_Update_OmitsComputed(t *testing.T) {
	c := &wsql.Compiler{}
	meta := computedMeta()
	out := c.Update(meta, map[string]any{"ID": 1, "Qty": 5, "Total": 99}, []string{"Qty", "Total"}, 1)

	want := `UPDATE "c_item" SET "qty" = ? WHERE "id" = ?`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
}

// SELECT reads a computed column back.
func TestCompiler_Select_IncludesComputed(t *testing.T) {
	c := &wsql.Compiler{}
	meta := computedMeta()
	out := c.FindByPK(meta, 1)
	if !strings.Contains(out.SQL, `"total"`) {
		t.Fatalf("FindByPK should read the computed column:\n%s", out.SQL)
	}
}
