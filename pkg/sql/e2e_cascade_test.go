package sql_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

type cascUser struct {
	ID     int          `db:"column:id;primary_key;auto_increment"`
	Orders []*cascOrder `db:"fk:user_id;on_delete:cascade"`
}

type cascOrder struct {
	ID     int     `db:"column:id;primary_key;auto_increment"`
	UserID int     `db:"column:user_id"`
	Total  float64 `db:"column:total"`
}

func TestCascade_ParsedAndGenerated(t *testing.T) {
	u := schema.Parse(&cascUser{})
	rel := u.Relation("Orders")
	if rel == nil || rel.OnDelete != "CASCADE" {
		t.Fatalf("Orders relation OnDelete: got %+v, want CASCADE", rel)
	}

	ops := migrations.ComputeDiff(
		[]*model.EntityMeta{u, schema.Parse(&cascOrder{})},
		migrations.DatabaseSchema{},
	)
	script := migrations.GenerateSQLScript(ops, migrations.DefaultDialect{})
	if !strings.Contains(script, `REFERENCES "casc_user" ("id") ON DELETE CASCADE`) {
		t.Errorf("generated DDL missing cascade FK:\n%s", script)
	}
}

type dmParent struct {
	ID       int        `db:"column:id;primary_key;auto_increment"`
	Children []*dmChild `db:"fk:parent_id;on_delete:cascade"` // owner declares cascade
}

type dmChild struct {
	ID       int       `db:"column:id;primary_key;auto_increment"`
	ParentID int       `db:"column:parent_id"`
	Parent   *dmParent `db:"ref"` // child also models the relation, without on_delete
}

// When both sides model the relationship, the referential action must survive
// regardless of which entity the differ processes first.
func TestCascade_BothSidesModeled_OrderIndependent(t *testing.T) {
	p := schema.Parse(&dmParent{})
	c := schema.Parse(&dmChild{})

	for _, order := range [][]*model.EntityMeta{{p, c}, {c, p}} {
		ops := migrations.ComputeDiff(order, migrations.DatabaseSchema{})
		script := migrations.GenerateSQLScript(ops, migrations.DefaultDialect{})
		if !strings.Contains(script, `REFERENCES "dm_parent" ("id") ON DELETE CASCADE`) {
			t.Errorf("cascade lost for entity order %v:\n%s", []string{order[0].Name, order[1].Name}, script)
		}
	}
}

// The generated ON DELETE CASCADE is enforced: deleting a parent removes its
// children at the database level (SQLite requires foreign_keys = ON).
func TestE2E_Cascade_DeletesChildren(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatal(err)
	}

	ops := migrations.ComputeDiff(
		[]*model.EntityMeta{schema.Parse(&cascUser{}), schema.Parse(&cascOrder{})},
		migrations.DatabaseSchema{},
	)
	script := migrations.GenerateSQLScript(ops, migrations.DefaultDialect{})
	var clean []string
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		clean = append(clean, line)
	}
	for _, stmt := range strings.Split(strings.Join(clean, "\n"), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("exec %q: %v", s, err)
			}
		}
	}

	if _, err := db.Exec(`INSERT INTO "casc_user" ("id") VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "casc_order" ("id","user_id","total") VALUES (1,1,10),(2,1,20)`); err != nil {
		t.Fatal(err)
	}

	// Deleting the parent cascades to its orders.
	if _, err := db.Exec(`DELETE FROM "casc_user" WHERE "id" = 1`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "casc_order"`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("orders after parent delete: got %d, want 0 (cascade)", n)
	}
}
