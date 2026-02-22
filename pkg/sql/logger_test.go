package sql_test

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/query"
	wsql "github.com/mirkobrombin/go-wormhole/pkg/sql"
)

func setupLoggerTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func loggerTestMeta() *model.EntityMeta {
	meta := testMeta()
	return meta
}

type logEntry struct {
	SQL    string
	Params []any
}

func TestWithQueryLogger_Redacted(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	var mu sync.Mutex
	var logs []logEntry

	p := wsql.New(db, wsql.WithQueryLogger(func(s string, params []any) {
		mu.Lock()
		defer mu.Unlock()
		logs = append(logs, logEntry{SQL: s, Params: append([]any{}, params...)})
	}))

	ctx := context.Background()
	meta := loggerTestMeta()

	// Insert triggers the logger
	_, err := p.Insert(ctx, meta, &struct {
		ID   int
		Name string
		Age  int
	}{Name: "secret_password_123", Age: 30})
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(logs) == 0 {
		t.Fatal("expected at least one log entry")
	}

	entry := logs[0]
	if !strings.Contains(entry.SQL, "INSERT") {
		t.Fatalf("expected INSERT in SQL: %s", entry.SQL)
	}

	// Params must be redacted
	for i, p := range entry.Params {
		s, ok := p.(string)
		if !ok || s != "[REDACTED]" {
			t.Fatalf("param[%d] should be [REDACTED], got %v", i, p)
		}
	}
}

func TestWithQueryLoggerUnsafe_RawParams(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	var mu sync.Mutex
	var logs []logEntry

	p := wsql.New(db, wsql.WithQueryLoggerUnsafe(func(s string, params []any) {
		mu.Lock()
		defer mu.Unlock()
		logs = append(logs, logEntry{SQL: s, Params: append([]any{}, params...)})
	}))

	ctx := context.Background()
	meta := loggerTestMeta()

	_, err := p.Insert(ctx, meta, &struct {
		ID   int
		Name string
		Age  int
	}{Name: "alice", Age: 25})
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(logs) == 0 {
		t.Fatal("expected at least one log entry")
	}

	entry := logs[0]

	// Raw params must contain actual values
	found := false
	for _, p := range entry.Params {
		if s, ok := p.(string); ok && s == "alice" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected raw param 'alice' in %v", entry.Params)
	}
}

func TestNoLogger_NoPanic(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	ctx := context.Background()
	meta := loggerTestMeta()

	// Must not panic when no logger is set
	_, err := p.Insert(ctx, meta, &struct {
		ID   int
		Name string
		Age  int
	}{Name: "bob", Age: 40})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueryLogger_Select(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	var lastSQL string

	p := wsql.New(db, wsql.WithQueryLogger(func(s string, params []any) {
		lastSQL = s
	}))

	ctx := context.Background()
	meta := loggerTestMeta()

	type User struct {
		ID   int
		Name string
		Age  int
	}

	var users []User
	q := query.From("users").Build()
	_ = p.Execute(ctx, meta, q, &users)

	if !strings.Contains(lastSQL, "SELECT") {
		t.Fatalf("expected SELECT in logged SQL: %s", lastSQL)
	}
}

func TestQueryLogger_Delete(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	var logged []logEntry

	p := wsql.New(db, wsql.WithQueryLogger(func(s string, params []any) {
		logged = append(logged, logEntry{SQL: s, Params: append([]any{}, params...)})
	}))

	ctx := context.Background()
	meta := loggerTestMeta()

	// Insert then delete
	_, _ = p.Insert(ctx, meta, &struct {
		ID   int
		Name string
		Age  int
	}{Name: "to_delete", Age: 99})

	_ = p.Delete(ctx, meta, 1)

	// Should have 2 log entries (insert + delete)
	if len(logged) < 2 {
		t.Fatalf("expected 2+ log entries, got %d", len(logged))
	}

	delEntry := logged[len(logged)-1]
	if !strings.Contains(delEntry.SQL, "DELETE") {
		t.Fatalf("expected DELETE: %s", delEntry.SQL)
	}

	// Params redacted
	for i, p := range delEntry.Params {
		s, ok := p.(string)
		if !ok || s != "[REDACTED]" {
			t.Fatalf("delete param[%d] should be [REDACTED], got %v", i, p)
		}
	}
}

// --- Explain (dry-run) tests ---

func TestExplainSelect(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	meta := loggerTestMeta()

	q := query.From("users").
		Filter(query.Predicate{Field: "age", Op: query.OpGt, Value: 18}).
		Limit(5).
		Build()

	c := p.ExplainSelect(meta, q)

	if !strings.Contains(c.SQL, "SELECT") {
		t.Fatalf("expected SELECT: %s", c.SQL)
	}
	if !strings.Contains(c.SQL, "age") {
		t.Fatalf("expected age filter: %s", c.SQL)
	}
	if len(c.Params) != 1 || c.Params[0] != 18 {
		t.Fatalf("expected raw param 18, got %v", c.Params)
	}
}

func TestExplainInsert(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	meta := loggerTestMeta()

	entity := &struct {
		ID   int
		Name string
		Age  int
	}{Name: "test", Age: 42}

	c := p.ExplainInsert(meta, entity)

	if !strings.Contains(c.SQL, "INSERT") {
		t.Fatalf("expected INSERT: %s", c.SQL)
	}
	if len(c.Params) == 0 {
		t.Fatal("expected params")
	}
}

func TestExplainFindByPK(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	meta := loggerTestMeta()

	c := p.ExplainFindByPK(meta, 7)

	if !strings.Contains(c.SQL, "SELECT") {
		t.Fatalf("expected SELECT: %s", c.SQL)
	}
	if len(c.Params) != 1 || c.Params[0] != 7 {
		t.Fatalf("expected param 7, got %v", c.Params)
	}
}

func TestExplainDelete(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	meta := loggerTestMeta()

	c := p.ExplainDelete(meta, 99)

	if !strings.Contains(c.SQL, "DELETE") {
		t.Fatalf("expected DELETE: %s", c.SQL)
	}
	if len(c.Params) != 1 || c.Params[0] != 99 {
		t.Fatalf("expected param 99, got %v", c.Params)
	}
}

func TestExplainUpdate(t *testing.T) {
	db := setupLoggerTestDB(t)
	defer db.Close()

	p := wsql.New(db)
	meta := loggerTestMeta()

	entity := &struct {
		ID   int
		Name string
		Age  int
	}{ID: 1, Name: "updated", Age: 50}

	c := p.ExplainUpdate(meta, entity, []string{"Name"})

	if !strings.Contains(c.SQL, "UPDATE") {
		t.Fatalf("expected UPDATE: %s", c.SQL)
	}
	if len(c.Params) == 0 {
		t.Fatal("expected params")
	}
}
