package discovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

// reproSource mirrors the model in the bug report: pointer and time.Time fields
// plus unique/index tags.
const reproSource = `package main

import "time"

type User struct {
	ID          string     ` + "`db:\"primary_key\"`" + `
	Email       string     ` + "`db:\"column:email;unique\"`" + `
	TenantID    string     ` + "`db:\"column:tenant_id;index\"`" + `
	LastLoginAt *time.Time ` + "`db:\"column:last_login_at\"`" + `
	CreatedAt   time.Time  ` + "`db:\"column:created_at\"`" + `
}
`

func discoverRepro(t *testing.T) *model.EntityMeta {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "models.go"), []byte(reproSource), 0o644); err != nil {
		t.Fatal(err)
	}
	models, err := DiscoverModels(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range models {
		if m.Name == "user" {
			return m
		}
	}
	t.Fatal("user model not discovered")
	return nil
}

func TestDiscovery_FieldTypesAndNullability(t *testing.T) {
	u := discoverRepro(t)

	cases := []struct {
		field    string
		sqlType  string
		nullable bool
	}{
		{"CreatedAt", "TIMESTAMP", false}, // time.Time, not TEXT
		{"LastLoginAt", "TIMESTAMP", true}, // *time.Time: timestamp AND nullable
		{"Email", "TEXT", false},
		{"ID", "TEXT", false},
	}
	for _, c := range cases {
		f := u.Field(c.field)
		if f == nil {
			t.Errorf("%s: not discovered", c.field)
			continue
		}
		if got := f.Tags["type"]; got != c.sqlType {
			t.Errorf("%s type: got %q, want %q", c.field, got, c.sqlType)
		}
		if f.Nullable != c.nullable {
			t.Errorf("%s nullable: got %v, want %v", c.field, f.Nullable, c.nullable)
		}
	}
}

func TestDiscovery_UniqueAndIndexTags(t *testing.T) {
	u := discoverRepro(t)

	email := u.Field("Email")
	if email == nil || !email.Indexed || !email.Unique {
		t.Fatalf("Email: want indexed+unique, got %+v", email)
	}
	tenant := u.Field("TenantID")
	if tenant == nil || !tenant.Indexed || tenant.Unique {
		t.Fatalf("TenantID: want indexed, not unique, got %+v", tenant)
	}
}

func TestDiscovery_NamedUniqueIndex(t *testing.T) {
	dir := t.TempDir()
	src := `package main

type Acct struct {
	ID   string ` + "`db:\"primary_key\"`" + `
	Slug string ` + "`db:\"column:slug;unique_index:uq_acct_slug\"`" + `
}
`
	if err := os.WriteFile(filepath.Join(dir, "m.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	models, err := DiscoverModels(dir)
	if err != nil {
		t.Fatal(err)
	}
	var slug *model.FieldMeta
	for _, m := range models {
		if m.Name == "acct" {
			slug = m.Field("Slug")
		}
	}
	if slug == nil {
		t.Fatal("acct.Slug not discovered")
	}
	if slug.Index != "uq_acct_slug" || !slug.Unique || !slug.Indexed {
		t.Fatalf("named unique index dropped: %+v", slug)
	}
}

// The differ turns the unique/index tags into CreateIndexOps with table-qualified
// names and the right Unique flag.
func TestDiscovery_IndexOpsGenerated(t *testing.T) {
	u := discoverRepro(t)
	ops := migrations.ComputeDiff([]*model.EntityMeta{u}, migrations.DatabaseSchema{})

	var uniqEmail, idxTenant bool
	for _, op := range ops {
		ci, ok := op.(migrations.CreateIndexOp)
		if !ok {
			continue
		}
		if ci.Name == "uniq_user_email" && ci.Unique && ci.Columns[0] == "email" {
			uniqEmail = true
		}
		if ci.Name == "idx_user_tenant_id" && !ci.Unique && ci.Columns[0] == "tenant_id" {
			idxTenant = true
		}
	}
	if !uniqEmail {
		t.Error("missing unique index uniq_user_email on email")
	}
	if !idxTenant {
		t.Error("missing index idx_user_tenant_id on tenant_id")
	}
}

// Diffing the discovered model against the schema the runtime ORM actually
// expects (timestamps for time columns, nullable for the pointer column) must
// produce no column-level ops. This fails on the original bug, where the
// generator emitted TEXT/NOT NULL and the differ would then want to alter every
// time column back. The target schema is specified independently of discovery,
// so the check is not a self-comparison.
func TestDiscovery_MatchesExpectedSchema(t *testing.T) {
	u := discoverRepro(t)

	current := migrations.DatabaseSchema{Tables: map[string]*migrations.TableSchema{
		"user": {
			Name: "user",
			Columns: map[string]*migrations.ColumnDef{
				"id":            {Name: "id", SQLType: "TEXT", PrimaryKey: true},
				"email":         {Name: "email", SQLType: "TEXT"},
				"tenant_id":     {Name: "tenant_id", SQLType: "TEXT"},
				"last_login_at": {Name: "last_login_at", SQLType: "TIMESTAMP", Nullable: true},
				"created_at":    {Name: "created_at", SQLType: "TIMESTAMP"},
			},
		},
	}}

	ops := migrations.ComputeDiff([]*model.EntityMeta{u}, current)
	for _, op := range ops {
		switch op.(type) {
		case migrations.CreateTableOp, migrations.AddColumnOp,
			migrations.AlterColumnOp, migrations.DropColumnOp:
			t.Errorf("generator disagrees with expected schema: %T %+v", op, op)
		}
	}
}
