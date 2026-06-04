package migrations_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

// Residual 1: the Postgres dialect renders portable types as native ones, so a
// time.Time column becomes timestamptz and []byte becomes bytea.
func TestPostgresDialect_MapColumnType(t *testing.T) {
	b := migrations.NewBuilderWith(migrations.PostgresDialect{})
	b.CreateTable("events",
		migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
		migrations.ColumnDef{Name: "created_at", SQLType: "TIMESTAMP"},
		migrations.ColumnDef{Name: "payload", SQLType: "BLOB", Nullable: true},
	)
	sql := b.SQL()

	if !strings.Contains(sql, `"created_at" TIMESTAMPTZ`) {
		t.Errorf("timestamp not mapped to timestamptz:\n%s", sql)
	}
	if !strings.Contains(sql, `"payload" BYTEA`) {
		t.Errorf("blob not mapped to bytea:\n%s", sql)
	}
	// The auto-increment PK mapping must still win for integers.
	if !strings.Contains(sql, `"id" SERIAL`) {
		t.Errorf("serial mapping regressed:\n%s", sql)
	}
}

// The render-time mapping must not introduce schema-diff churn: the stored
// column type stays portable, so diffing a model against a snapshot built from
// the same model produces no AlterColumnOp.
func TestPostgresDialect_MapColumnType_NoChurn(t *testing.T) {
	meta := &model.EntityMeta{
		Name: "events",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
			{FieldName: "CreatedAt", Column: "created_at", GoType: reflect.TypeOf(time.Time{}), Tags: map[string]string{"type": "TIMESTAMP"}},
		},
	}
	snapshot := migrations.MetaToSnapshot([]*model.EntityMeta{meta})
	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, snapshot)
	for _, op := range ops {
		if _, ok := op.(migrations.AlterColumnOp); ok {
			t.Fatalf("unexpected AlterColumnOp (churn): %+v", op)
		}
	}
}

// Residual 2: diffing against the schema rebuilt from existing migrations emits
// an incremental AddColumn, not a full CreateTable.
func TestRebuildFromMigrations_IncrementalDiff(t *testing.T) {
	first := migrations.Migration{
		ID: "001_create_users",
		Up: func(b *migrations.SchemaBuilder) {
			b.CreateTable("users",
				migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "name", SQLType: "TEXT"},
			)
		},
	}

	current := migrations.RebuildFromMigrations([]migrations.Migration{first})

	// The model now has an extra column.
	meta := &model.EntityMeta{
		Name: "users",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Name", Column: "name", GoType: reflect.TypeOf("")},
			{FieldName: "Age", Column: "age", GoType: reflect.TypeOf(0)},
		},
	}

	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, current)

	var addedAge, createdTable bool
	for _, op := range ops {
		switch o := op.(type) {
		case migrations.AddColumnOp:
			if o.Column.Name == "age" {
				addedAge = true
			}
		case migrations.CreateTableOp:
			if o.Table == "users" {
				createdTable = true
			}
		}
	}
	if !addedAge {
		t.Error("expected incremental AddColumnOp for age")
	}
	if createdTable {
		t.Error("must not recreate an existing table when diffing against the rebuilt snapshot")
	}
}
