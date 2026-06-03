package sql_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type incUser struct {
	ID      int          `db:"column:id;primary_key;auto_increment"`
	Name    string       `db:"column:name"`
	Orders  []*incOrder  `db:"fk:user_id"`  // 1:N
	Profile *incProfile  `db:"fk:user_id"`  // 1:1 (FK on profile)
}

type incOrder struct {
	ID     int     `db:"column:id;primary_key;auto_increment"`
	UserID int     `db:"column:user_id"`
	Total  float64 `db:"column:total"`
	User   *incUser `db:"ref"` // belongs-to (FK user_id on this table)
}

type incProfile struct {
	ID     int    `db:"column:id;primary_key;auto_increment"`
	UserID int    `db:"column:user_id"`
	Bio    string `db:"column:bio"`
}

func init() {
	dsl.Register(incUser{})
	dsl.Register(incOrder{})
	dsl.Register(incProfile{})
}

func openIncludeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	stmts := []string{
		`CREATE TABLE "inc_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL)`,
		`CREATE TABLE "inc_order" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "user_id" INTEGER NOT NULL, "total" REAL NOT NULL DEFAULT 0)`,
		`CREATE TABLE "inc_profile" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "user_id" INTEGER NOT NULL, "bio" TEXT NOT NULL)`,
		`INSERT INTO "inc_user" ("id","name") VALUES (1,'Alice'),(2,'Bob')`,
		`INSERT INTO "inc_order" ("id","user_id","total") VALUES (1,1,10),(2,1,20),(3,2,30)`,
		`INSERT INTO "inc_profile" ("id","user_id","bio") VALUES (1,1,'hi from alice')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}
	return db
}

func TestE2E_Include_OneToManyAndOneToOne(t *testing.T) {
	db := openIncludeDB(t)
	defer db.Close()

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var users []*incUser
	if err := ctx.Set(&users).
		Include("Orders").
		Include("Profile").
		OrderBy("id", 0).
		All(); err != nil {
		t.Fatal(err)
	}

	if len(users) != 2 {
		t.Fatalf("users: got %d, want 2", len(users))
	}

	alice := users[0]
	if alice.Name != "Alice" {
		t.Fatalf("first user: got %q, want Alice", alice.Name)
	}
	if len(alice.Orders) != 2 {
		t.Errorf("Alice.Orders: got %d, want 2", len(alice.Orders))
	}
	if alice.Profile == nil || alice.Profile.Bio != "hi from alice" {
		t.Errorf("Alice.Profile: got %+v, want bio 'hi from alice'", alice.Profile)
	}

	bob := users[1]
	if len(bob.Orders) != 1 {
		t.Errorf("Bob.Orders: got %d, want 1", len(bob.Orders))
	}
	if bob.Profile != nil {
		t.Errorf("Bob.Profile: got %+v, want nil", bob.Profile)
	}
}

func TestE2E_Include_BelongsTo(t *testing.T) {
	db := openIncludeDB(t)
	defer db.Close()

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var orders []*incOrder
	if err := ctx.Set(&orders).Include("User").OrderBy("id", 0).All(); err != nil {
		t.Fatal(err)
	}
	if len(orders) != 3 {
		t.Fatalf("orders: got %d, want 3", len(orders))
	}
	for _, o := range orders {
		if o.User == nil {
			t.Fatalf("order %d: User not loaded", o.ID)
		}
	}
	if orders[0].User.Name != "Alice" || orders[2].User.Name != "Bob" {
		t.Errorf("belongs-to names: got %q / %q, want Alice / Bob",
			orders[0].User.Name, orders[2].User.Name)
	}
}

func TestE2E_Include_UnknownRelation(t *testing.T) {
	db := openIncludeDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var users []*incUser
	err := ctx.Set(&users).Include("Nope").All()
	if err == nil || !strings.Contains(err.Error(), "no relation") {
		t.Fatalf("want unknown-relation error, got %v", err)
	}
}

type mStudent struct {
	ID      int        `db:"column:id;primary_key;auto_increment"`
	Courses []*mCourse `db:"join:enrollments;ref:student_id;fk:course_id"`
}

type mCourse struct {
	ID    int    `db:"column:id;primary_key;auto_increment"`
	Title string `db:"column:title"`
}

func TestE2E_Include_ManyToManyUnsupported(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "m_student" ("id" INTEGER PRIMARY KEY AUTOINCREMENT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "m_student" ("id") VALUES (1)`); err != nil {
		t.Fatal(err)
	}

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var students []*mStudent
	err = ctx.Set(&students).Include("Courses").All()
	if err == nil || !strings.Contains(err.Error(), "many-to-many") {
		t.Fatalf("want many-to-many unsupported error, got %v", err)
	}
}

// Regression: a 64-bit parent primary key must still match a 32-bit child
// foreign key during eager loading (key normalization).
type widthUser struct {
	ID     int64         `db:"column:id;primary_key;auto_increment"`
	Name   string        `db:"column:name"`
	Orders []*widthOrder `db:"fk:user_id"`
}

type widthOrder struct {
	ID     int     `db:"column:id;primary_key;auto_increment"`
	UserID int     `db:"column:user_id"`
	Total  float64 `db:"column:total"`
}

func TestE2E_Include_IntegerWidthMismatch(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, s := range []string{
		`CREATE TABLE "width_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL)`,
		`CREATE TABLE "width_order" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "user_id" INTEGER NOT NULL, "total" REAL NOT NULL DEFAULT 0)`,
		`INSERT INTO "width_user" ("id","name") VALUES (1,'Alice')`,
		`INSERT INTO "width_order" ("id","user_id","total") VALUES (1,1,10),(2,1,20)`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var users []*widthUser
	if err := ctx.Set(&users).Include("Orders").All(); err != nil {
		t.Fatal(err)
	}
	if len(users) != 1 || len(users[0].Orders) != 2 {
		t.Fatalf("int64 PK / int FK: got %d orders, want 2", len(users[0].Orders))
	}
}
