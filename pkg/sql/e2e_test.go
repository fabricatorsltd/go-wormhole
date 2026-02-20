package sql_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	wctx "github.com/mirkobrombin/go-wormhole/pkg/context"
	"github.com/mirkobrombin/go-wormhole/pkg/dsl"
	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/query"
	"github.com/mirkobrombin/go-wormhole/pkg/schema"
	wsql "github.com/mirkobrombin/go-wormhole/pkg/sql"

	"github.com/mirkobrombin/go-foundation/pkg/resiliency"
)

type User struct {
	ID   int    `db:"column:id; primary_key; auto_increment"`
	Name string `db:"column:name; type:text"`
	Age  int    `db:"column:age"`
}

func init() {
	dsl.Register(User{})
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// SQLite :memory: is per-connection; force single conn to
	// avoid different connections seeing different databases.
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE "user" (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT,
		"name" TEXT NOT NULL,
		"age" INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// --- End-to-end CRUD tests ---

func TestE2E_InsertAndFind(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	alice := &User{Name: "Alice", Age: 30}
	id, err := p.Insert(ctx, meta, alice)
	if err != nil {
		t.Fatal(err)
	}

	var found User
	if err := p.Find(ctx, meta, id, &found); err != nil {
		t.Fatal(err)
	}
	if found.Name != "Alice" || found.Age != 30 {
		t.Fatalf("unexpected: %+v", found)
	}
}

func TestE2E_PartialUpdate(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	u := &User{Name: "Bob", Age: 20}
	id, _ := p.Insert(ctx, meta, u)

	u.ID = int(id.(int64))
	u.Age = 25
	// Only update Age
	if err := p.Update(ctx, meta, u, []string{"Age"}); err != nil {
		t.Fatal(err)
	}

	var found User
	p.Find(ctx, meta, id, &found)
	if found.Age != 25 {
		t.Fatalf("expected age=25, got %d", found.Age)
	}
	if found.Name != "Bob" {
		t.Fatalf("name should be unchanged, got %s", found.Name)
	}
}

func TestE2E_Delete(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	u := &User{Name: "Charlie", Age: 40}
	id, _ := p.Insert(ctx, meta, u)

	if err := p.Delete(ctx, meta, id); err != nil {
		t.Fatal(err)
	}

	var found User
	err := p.Find(ctx, meta, id, &found)
	if err == nil {
		t.Fatal("expected not found after delete")
	}
}

func TestE2E_QueryWithAST(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	for i := 0; i < 20; i++ {
		u := &User{Name: fmt.Sprintf("user%02d", i), Age: 10 + i}
		p.Insert(ctx, meta, u)
	}

	// Query: age > 20, ordered DESC, limit 5
	q := query.From("user").
		Filter(query.Predicate{Field: "age", Op: query.OpGt, Value: 20}).
		OrderBy("age", query.Desc).
		Limit(5).
		Build()

	var results []User
	if err := p.Execute(ctx, meta, q, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5, got %d", len(results))
	}
	if results[0].Age != 29 {
		t.Fatalf("expected first age=29, got %d", results[0].Age)
	}
}

func TestE2E_QueryPointerSlice(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	for i := 0; i < 5; i++ {
		p.Insert(ctx, meta, &User{Name: fmt.Sprintf("p%d", i), Age: i})
	}

	q := query.From("user").Build()
	var results []*User
	if err := p.Execute(ctx, meta, q, &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5, got %d", len(results))
	}
	for _, r := range results {
		if r == nil {
			t.Fatal("nil pointer in results")
		}
	}
}

func TestE2E_DSLPointerTracking(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	for i := 0; i < 10; i++ {
		p.Insert(ctx, meta, &User{Name: fmt.Sprintf("dsl%d", i), Age: 15 + i})
	}

	u := &User{}
	cond := dsl.Gt(u, &u.Age, 20)

	q := query.From("user").
		Filter(cond).
		Build()

	var results []User
	p.Execute(ctx, meta, q, &results)
	for _, r := range results {
		if r.Age <= 20 {
			t.Fatalf("DSL filter failed: age %d <= 20", r.Age)
		}
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results (21-24), got %d", len(results))
	}
}

func TestE2E_Transaction(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	tx.Insert(ctx, meta, &User{Name: "tx1", Age: 1})
	tx.Insert(ctx, meta, &User{Name: "tx2", Age: 2})
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	q := query.From("user").Build()
	var results []User
	p.Execute(ctx, meta, q, &results)
	if len(results) != 2 {
		t.Fatalf("expected 2 after commit, got %d", len(results))
	}
}

func TestE2E_TransactionRollback(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	tx, _ := p.Begin(ctx)
	tx.Insert(ctx, meta, &User{Name: "rollback", Age: 99})
	tx.Rollback()

	q := query.From("user").Build()
	var results []User
	p.Execute(ctx, meta, q, &results)
	if len(results) != 0 {
		t.Fatalf("expected 0 after rollback, got %d", len(results))
	}
}

// --- DbContext integration tests ---

func TestE2E_DbContextSaveChanges(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	dbCtx := wctx.New(p)
	ctx := context.Background()

	alice := &User{Name: "Alice", Age: 30}
	dbCtx.Add(alice)

	if err := dbCtx.SaveChanges(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify it was inserted
	meta := schema.Parse(&User{})
	q := query.From("user").Build()
	var results []User
	p.Execute(ctx, meta, q, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 inserted, got %d", len(results))
	}
	if results[0].Name != "Alice" {
		t.Fatalf("expected Alice, got %s", results[0].Name)
	}
}

func TestE2E_DbContextModifyAndSave(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	dbCtx := wctx.New(p)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	// Insert directly
	id, _ := p.Insert(ctx, meta, &User{Name: "Bob", Age: 20})

	// Fetch via Set().Find() — auto-tracks
	var u User
	if err := dbCtx.Set(&u).Find(id); err != nil {
		t.Fatal(err)
	}

	// Modify in memory
	u.Age = 35

	// Save — should produce partial UPDATE (only age)
	if err := dbCtx.Save(); err != nil {
		t.Fatal(err)
	}

	// Verify
	var check User
	p.Find(ctx, meta, id, &check)
	if check.Age != 35 {
		t.Fatalf("expected age=35, got %d", check.Age)
	}
	if check.Name != "Bob" {
		t.Fatalf("name should be unchanged: %s", check.Name)
	}
}

func TestE2E_GenericFind(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	dbCtx := wctx.New(p)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	p.Insert(ctx, meta, &User{Name: "Generic", Age: 42})

	u, err := wctx.Find[User](ctx, dbCtx, int64(1))
	if err != nil {
		t.Fatal(err)
	}
	if u.Name != "Generic" {
		t.Fatalf("expected Generic, got %s", u.Name)
	}

	// Must be tracked
	entry, ok := dbCtx.Entry(u)
	if !ok {
		t.Fatal("not tracked")
	}
	if entry.State != model.Unchanged {
		t.Fatalf("expected Unchanged, got %v", entry.State)
	}
}

func TestE2E_GenericQuery(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	dbCtx := wctx.New(p)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	for i := 0; i < 10; i++ {
		p.Insert(ctx, meta, &User{Name: fmt.Sprintf("q%d", i), Age: 10 + i})
	}

	u := &User{}
	results, err := wctx.Query[User](dbCtx).
		Where(dsl.Gt(u, &u.Age, 15)).
		OrderBy("age", query.Asc).
		Limit(3).
		Exec(ctx)

	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3, got %d", len(results))
	}
	if results[0].Age != 16 {
		t.Fatalf("expected first age=16, got %d", results[0].Age)
	}
}

// --- Stress / batch test ---

func TestE2E_StressBatchInsert(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	dbCtx := wctx.New(p)
	ctx := context.Background()

	const n = 500
	for i := 0; i < n; i++ {
		dbCtx.Add(&User{Name: fmt.Sprintf("stress%d", i), Age: i})
	}

	if err := dbCtx.SaveChanges(ctx); err != nil {
		t.Fatal(err)
	}

	meta := schema.Parse(&User{})
	q := query.From("user").Build()
	var results []User
	p.Execute(ctx, meta, q, &results)
	if len(results) != n {
		t.Fatalf("expected %d, got %d", n, len(results))
	}
}

func TestE2E_ProviderRetry(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db, wsql.WithRetry(resiliency.WithAttempts(2)))
	ctx := context.Background()
	meta := schema.Parse(&User{})

	// Insert should work normally with retry enabled
	id, err := p.Insert(ctx, meta, &User{Name: "retry", Age: 99})
	if err != nil {
		t.Fatal(err)
	}

	var found User
	if err := p.Find(ctx, meta, id, &found); err != nil {
		t.Fatal(err)
	}
	if found.Name != "retry" {
		t.Fatalf("expected retry, got %s", found.Name)
	}
}

func TestE2E_EntitySetWhereAll(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	dbCtx := wctx.New(p)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	for i := 0; i < 20; i++ {
		p.Insert(ctx, meta, &User{Name: fmt.Sprintf("set%d", i), Age: 10 + i})
	}

	var users []User
	err := dbCtx.Set(&users).
		Where(query.Predicate{Field: "age", Op: query.OpGte, Value: 20}).
		OrderBy("age", query.Asc).
		Limit(5).
		All()

	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 5 {
		t.Fatalf("expected 5, got %d", len(users))
	}
	if users[0].Age != 20 {
		t.Fatalf("expected first age=20, got %d", users[0].Age)
	}
}

func TestE2E_DeleteAndVerify(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&User{})

	id, _ := p.Insert(ctx, meta, &User{Name: "Del", Age: 50})

	// Use a fresh DbContext
	dbCtx := wctx.New(p)
	var u User
	dbCtx.Set(&u).Find(id)
	dbCtx.Remove(&u)

	if err := dbCtx.Save(); err != nil {
		t.Fatal(err)
	}

	// Verify removed (use a direct provider call, no DbContext)
	var check User
	err := p.Find(ctx, meta, id, &check)
	if err == nil && check.Name == "Del" {
		t.Fatal("expected record to be deleted")
	}
}

// Suppress unused import warning for reflect
var _ = reflect.TypeOf
