package migrations

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

type fkUser struct {
	ID     int        `db:"column:id;primary_key;auto_increment"`
	Name   string     `db:"column:name"`
	Orders []*fkOrder `db:"fk:user_id"` // 1:N -> FK column on fk_order
}

type fkOrder struct {
	ID     int     `db:"column:id;primary_key;auto_increment"`
	UserID int     `db:"column:user_id"`
	Total  float64 `db:"column:total"`
}

func TestComputeDiff_GeneratesForeignKey(t *testing.T) {
	targets := []*model.EntityMeta{schema.Parse(&fkUser{}), schema.Parse(&fkOrder{})}

	ops := ComputeDiff(targets, DatabaseSchema{})

	var orderCreate *CreateTableOp
	for i := range ops {
		if ct, ok := ops[i].(CreateTableOp); ok && ct.Table == "fk_order" {
			c := ct
			orderCreate = &c
		}
	}
	if orderCreate == nil {
		t.Fatal("no CreateTable op for fk_order")
	}

	var fkCol *ColumnDef
	for i := range orderCreate.Columns {
		if orderCreate.Columns[i].Name == "user_id" {
			fkCol = &orderCreate.Columns[i]
		}
	}
	if fkCol == nil || fkCol.Ref == nil {
		t.Fatalf("user_id column missing FK ref: %+v", fkCol)
	}
	if fkCol.Ref.Table != "fk_user" || fkCol.Ref.Column != "id" {
		t.Errorf("FK ref: got %s(%s), want fk_user(id)", fkCol.Ref.Table, fkCol.Ref.Column)
	}

	// Rendered DDL carries the REFERENCES clause.
	b := NewBuilderWith(DefaultDialect{})
	for _, op := range ops {
		if ct, ok := op.(CreateTableOp); ok && ct.Table == "fk_order" {
			b.CreateTable(ct.Table, ct.Columns...)
		}
	}
	sql := b.SQL()
	if !strings.Contains(sql, `REFERENCES "fk_user" ("id")`) {
		t.Errorf("rendered DDL missing FK reference:\n%s", sql)
	}
}
