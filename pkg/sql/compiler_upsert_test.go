package sql_test

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func upsertMeta() *model.EntityMeta {
	meta := &model.EntityMeta{
		Name: "messages",
		GoType: reflect.TypeOf(struct {
			ID     string
			Hash   string
			Status string
		}{}),
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true},
			{FieldName: "Hash", Column: "hash"},
			{FieldName: "Status", Column: "status"},
		},
	}
	meta.PrimaryKey = &meta.Fields[0]
	meta.BuildIndex()
	return meta
}

func TestInsertOnConflict_DoNothing(t *testing.T) {
	c := &wsql.Compiler{}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "pending"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{Columns: []string{"id"}})

	want := `INSERT INTO "messages" ("id", "hash", "status") VALUES (?, ?, ?) ON CONFLICT ("id") DO NOTHING`
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
	if len(out.Params) != 3 {
		t.Fatalf("params: want 3, got %d", len(out.Params))
	}
}

func TestInsertOnConflict_DoUpdate(t *testing.T) {
	c := &wsql.Compiler{}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "confirmed"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{
		Columns: []string{"id"},
		Update:  []string{"hash", "status"},
	})

	want := `INSERT INTO "messages" ("id", "hash", "status") VALUES (?, ?, ?) ON CONFLICT ("id") DO UPDATE SET "hash" = EXCLUDED."hash", "status" = EXCLUDED."status"`
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
	if len(out.Params) != 3 {
		t.Fatalf("params: want 3, got %d", len(out.Params))
	}
}
