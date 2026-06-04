package migrations_test

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

type snapUser struct {
	ID        int       `db:"column:id;primary_key;auto_increment"`
	Email     string    `db:"column:email;unique"`            // unique index
	TenantID  string    `db:"column:tenant_id;index"`         // plain index
	Name      string    `db:"column:name;nullable"`           // nullable
	Status    string    `db:"column:status;default:'active'"` // default
	Age       int       `db:"column:age"`                     // untagged type -> GoType-resolved
	CreatedAt time.Time `db:"column:created_at"`              // time.Time -> GoType-resolved
}

// The critical guarantee: a model written to a snapshot and re-diffed produces
// ZERO ops total, including for indexed and unique columns and GoType-resolved
// types. If indexes re-emitted or a type churned, `add` would never report
// "no changes" and would write a spurious migration every time.
func TestSnapshot_ZeroDiffTotal(t *testing.T) {
	meta := schema.Parse(&snapUser{})
	path := filepath.Join(t.TempDir(), "schema_snapshot.json")

	if err := migrations.WriteSnapshot(path, migrations.MetaToSnapshot([]*model.EntityMeta{meta})); err != nil {
		t.Fatal(err)
	}
	loaded, err := migrations.LoadSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}

	ops := migrations.ComputeDiff([]*model.EntityMeta{meta}, loaded)
	if len(ops) != 0 {
		t.Fatalf("expected zero ops on re-diff, got %d:", len(ops))
	}
	for _, op := range ops {
		t.Errorf("  unexpected op: %T %+v", op, op)
	}
}

func acctMeta(indexed, unique bool) *model.EntityMeta {
	return &model.EntityMeta{
		Name: "acct",
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, GoType: reflect.TypeOf(0)},
			{FieldName: "Email", Column: "email", GoType: reflect.TypeOf(""), Indexed: indexed, Unique: unique},
		},
	}
}

func TestSnapshot_IndexAddedAndDropped(t *testing.T) {
	noIdx := migrations.MetaToSnapshot([]*model.EntityMeta{acctMeta(false, false)})
	withIdx := migrations.MetaToSnapshot([]*model.EntityMeta{acctMeta(true, true)})

	// Adding the unique index emits exactly one CreateIndexOp, nothing else.
	add := migrations.ComputeDiff([]*model.EntityMeta{acctMeta(true, true)}, noIdx)
	if len(add) != 1 {
		t.Fatalf("index add: got %d ops, want 1: %+v", len(add), add)
	}
	ci, ok := add[0].(migrations.CreateIndexOp)
	if !ok || ci.Name != "uniq_acct_email" || !ci.Unique {
		t.Fatalf("index add: got %+v, want CreateIndex uniq_acct_email unique", add[0])
	}

	// Removing it emits exactly one DropIndexOp.
	drop := migrations.ComputeDiff([]*model.EntityMeta{acctMeta(false, false)}, withIdx)
	if len(drop) != 1 {
		t.Fatalf("index drop: got %d ops, want 1: %+v", len(drop), drop)
	}
	if di, ok := drop[0].(migrations.DropIndexOp); !ok || di.Name != "uniq_acct_email" {
		t.Fatalf("index drop: got %+v, want DropIndex uniq_acct_email", drop[0])
	}
}

func TestSnapshot_FileRoundTripDeterministic(t *testing.T) {
	meta := schema.Parse(&snapUser{})
	snap := migrations.MetaToSnapshot([]*model.EntityMeta{meta})
	p1 := filepath.Join(t.TempDir(), "a.json")
	p2 := filepath.Join(t.TempDir(), "b.json")
	if err := migrations.WriteSnapshot(p1, snap); err != nil {
		t.Fatal(err)
	}
	if err := migrations.WriteSnapshot(p2, snap); err != nil {
		t.Fatal(err)
	}
	// Deterministic output (JSON map keys sort), so it is clean in version control.
	b1, _ := migrations.LoadSnapshot(p1)
	b2, _ := migrations.LoadSnapshot(p2)
	if !reflect.DeepEqual(b1, b2) {
		t.Error("snapshot round-trip not deterministic")
	}
	if b1.Tables["snap_user"] == nil || b1.Tables["snap_user"].Columns["created_at"].SQLType != "TIMESTAMP" {
		t.Errorf("loaded snapshot missing resolved type: %+v", b1.Tables["snap_user"])
	}
}

// A dropped index must render valid DROP INDEX for each dialect: MySQL/MSSQL
// require the table, Postgres/SQLite drop by name.
func TestDropIndex_RespectsDialect(t *testing.T) {
	ops := []migrations.MigrationOp{migrations.DropIndexOp{Name: "idx_acct_email", Table: "acct"}}

	cases := []struct {
		dialect migrations.Dialect
		want    string
	}{
		{migrations.DefaultDialect{}, `DROP INDEX IF EXISTS "idx_acct_email"`},
		{migrations.PostgresDialect{}, `DROP INDEX IF EXISTS "idx_acct_email"`},
		{migrations.MySQLDialect{}, "DROP INDEX `idx_acct_email` ON `acct`"},
		{migrations.MSSQLDialect{}, "DROP INDEX IF EXISTS [idx_acct_email] ON [acct]"},
	}
	for _, c := range cases {
		got := migrations.GenerateSQLScript(ops, c.dialect)
		if !strings.Contains(got, c.want) {
			t.Errorf("%T: got %q, want it to contain %q", c.dialect, got, c.want)
		}
	}
}

func TestScriptMigrations_RendersWithDialect(t *testing.T) {
	migs := []migrations.Migration{
		{ID: "002_add_orders", Up: func(b *migrations.SchemaBuilder) {
			b.CreateTable("orders", migrations.ColumnDef{Name: "id", SQLType: "INTEGER", PrimaryKey: true, AutoIncr: true})
		}},
		{ID: "001_create_users", Up: func(b *migrations.SchemaBuilder) {
			b.CreateTable("users", migrations.ColumnDef{Name: "created_at", SQLType: "TIMESTAMP"})
		}},
	}
	out := migrations.ScriptMigrations(migs, migrations.PostgresDialect{})

	// ID order, dialect applied.
	if strings.Index(out, "001_create_users") > strings.Index(out, "002_add_orders") {
		t.Error("migrations not scripted in ID order")
	}
	if !strings.Contains(out, "TIMESTAMPTZ") || !strings.Contains(out, "SERIAL") {
		t.Errorf("dialect not applied in script:\n%s", out)
	}
}
