package sql_test

import (
	"database/sql"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// jsonDoc stores a nested map in a json-tagged text column.
type jsonDoc struct {
	ID   int            `db:"column:id;primary_key"`
	Data map[string]any `db:"column:data;json"`
}

func init() { dsl.Register(jsonDoc{}) }

// A query filters on a nested JSON path inside a json column and returns only
// the matching rows.
func TestE2E_JSONQuery_NestedPath(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "json_doc" ("id" INTEGER PRIMARY KEY, "data" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()
	ctx.Add(
		&jsonDoc{ID: 1, Data: map[string]any{"address": map[string]any{"city": "Berlin"}}},
		&jsonDoc{ID: 2, Data: map[string]any{"address": map[string]any{"city": "Paris"}}},
	)
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	r := &jsonDoc{}
	var got []jsonDoc
	if err := ctx.Set(&got).Where(dsl.JSONEq(r, &r.Data, "address.city", "Berlin")).All(); err != nil {
		t.Fatalf("json query: %v", err)
	}
	if len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("expected only the Berlin row, got %+v", got)
	}
}
