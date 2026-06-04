package sql_test

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func compositeMeta() *model.EntityMeta {
	meta := &model.EntityMeta{
		Name: "order_line",
		GoType: reflect.TypeOf(struct {
			OrderID int
			LineNo  int
			Qty     int
		}{}),
		Fields: []model.FieldMeta{
			{FieldName: "OrderID", Column: "order_id", PrimaryKey: true},
			{FieldName: "LineNo", Column: "line_no", PrimaryKey: true},
			{FieldName: "Qty", Column: "qty"},
		},
	}
	meta.PrimaryKeys = []*model.FieldMeta{&meta.Fields[0], &meta.Fields[1]}
	meta.PrimaryKey = &meta.Fields[0]
	meta.BuildIndex()
	return meta
}

// Delete on a composite key ANDs every key column.
func TestCompiler_Delete_Composite(t *testing.T) {
	c := &wsql.Compiler{}
	meta := compositeMeta()

	out := c.Delete(meta, []any{7, 3})

	want := `DELETE FROM "order_line" WHERE "order_id" = ? AND "line_no" = ?`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	if len(out.Params) != 2 || out.Params[0] != 7 || out.Params[1] != 3 {
		t.Errorf("params: got %v, want [7 3]", out.Params)
	}
}

// FindByPK on a composite key ANDs every key column, in order.
func TestCompiler_FindByPK_Composite(t *testing.T) {
	c := &wsql.Compiler{}
	meta := compositeMeta()

	out := c.FindByPK(meta, []any{7, 3})

	want := `SELECT "order_id", "line_no", "qty" FROM "order_line" WHERE "order_id" = ? AND "line_no" = ? LIMIT 1`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	if len(out.Params) != 2 {
		t.Errorf("params: want 2, got %d", len(out.Params))
	}
}

// Update on a composite key guards the WHERE on every key column, with the SET
// params first and the key params last in placeholder order.
func TestCompiler_Update_Composite(t *testing.T) {
	c := &wsql.Compiler{}
	meta := compositeMeta()
	values := map[string]any{"OrderID": 7, "LineNo": 3, "Qty": 99}

	out := c.Update(meta, values, []string{"Qty"}, []any{7, 3})

	want := `UPDATE "order_line" SET "qty" = ? WHERE "order_id" = ? AND "line_no" = ?`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	// SET value first, then the two key values.
	if len(out.Params) != 3 || out.Params[0] != 99 || out.Params[1] != 7 || out.Params[2] != 3 {
		t.Errorf("params: got %v, want [99 7 3]", out.Params)
	}
}

// Composite key + version column under numbered ($N) placeholders: SET params
// first, then each key column, then the version guard, all contiguous.
func TestCompiler_Update_CompositeVersionedNumbered(t *testing.T) {
	c := &wsql.Compiler{Numbered: true}
	meta := compositeMeta()
	meta.Fields = append(meta.Fields, model.FieldMeta{FieldName: "Ver", Column: "ver"})
	meta.Version = &meta.Fields[len(meta.Fields)-1]
	meta.BuildIndex()

	values := map[string]any{"OrderID": 7, "LineNo": 3, "Qty": 99, "Ver": 5}
	out := c.Update(meta, values, []string{"Qty"}, []any{7, 3})

	want := `UPDATE "order_line" SET "qty" = $1, "ver" = "ver" + 1 WHERE "order_id" = $2 AND "line_no" = $3 AND "ver" = $4`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
	// qty, key1, key2, version-guard
	if len(out.Params) != 4 || out.Params[0] != 99 || out.Params[1] != 7 || out.Params[2] != 3 || out.Params[3] != 5 {
		t.Errorf("params: got %v, want [99 7 3 5]", out.Params)
	}
}

// A single-column key still compiles exactly the old single-clause WHERE.
func TestCompiler_Delete_SingleKeyUnchanged(t *testing.T) {
	c := &wsql.Compiler{}
	meta := upsertMeta() // single PK "id"

	out := c.Delete(meta, "m1")

	want := `DELETE FROM "messages" WHERE "id" = ?`
	if out.SQL != want {
		t.Fatalf("SQL:\n got %q\nwant %q", out.SQL, want)
	}
}
