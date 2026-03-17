package context_test

import (
	stdctx "context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"

	"github.com/mirkobrombin/go-foundation/pkg/resiliency"
)

// --- mock provider ---

type mockProvider struct {
	findCalls   int32
	execCalls   int32
	findErr     error
	execErr     error
	insertedPK  any
	beginCalled bool
	committed   bool
}

func (m *mockProvider) Name() string                                              { return "mock" }
func (m *mockProvider) Open(_ stdctx.Context, _ string) error                     { return nil }
func (m *mockProvider) Close() error                                              { return nil }
func (m *mockProvider) Delete(_ stdctx.Context, _ *model.EntityMeta, _ any) error { return nil }

func (m *mockProvider) Insert(_ stdctx.Context, _ *model.EntityMeta, _ any) (any, error) {
	m.insertedPK = 1
	return 1, nil
}

func (m *mockProvider) Update(_ stdctx.Context, _ *model.EntityMeta, _ any, _ []string) error {
	return nil
}

func (m *mockProvider) Find(_ stdctx.Context, meta *model.EntityMeta, pk any, dest any) error {
	atomic.AddInt32(&m.findCalls, 1)
	if m.findErr != nil {
		return m.findErr
	}
	// Populate dest with fake data via reflection
	v := reflect.ValueOf(dest)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if meta.PrimaryKey != nil {
		f := v.FieldByName(meta.PrimaryKey.FieldName)
		if f.CanSet() {
			f.Set(reflect.ValueOf(pk))
		}
	}
	return nil
}

func (m *mockProvider) Execute(_ stdctx.Context, _ *model.EntityMeta, _ query.Query, _ any) error {
	atomic.AddInt32(&m.execCalls, 1)
	return m.execErr
}

func (m *mockProvider) Begin(_ stdctx.Context) (provider.Tx, error) {
	m.beginCalled = true
	return mockTx{m: m}, nil
}

type mockTx struct{ m *mockProvider }

func (t mockTx) Commit() error   { t.m.committed = true; return nil }
func (t mockTx) Rollback() error { return nil }
func (t mockTx) Insert(ctx stdctx.Context, meta *model.EntityMeta, entity any) (any, error) {
	return t.m.Insert(ctx, meta, entity)
}
func (t mockTx) Update(ctx stdctx.Context, meta *model.EntityMeta, entity any, changed []string) error {
	return t.m.Update(ctx, meta, entity, changed)
}
func (t mockTx) Delete(ctx stdctx.Context, meta *model.EntityMeta, pk any) error {
	return t.m.Delete(ctx, meta, pk)
}
func (t mockTx) Find(ctx stdctx.Context, meta *model.EntityMeta, pk any, dest any) error {
	return t.m.Find(ctx, meta, pk, dest)
}
func (t mockTx) Execute(ctx stdctx.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	return t.m.Execute(ctx, meta, q, dest)
}

// --- QueryExplainer on mock ---

func (m *mockProvider) ExplainSelect(meta *model.EntityMeta, q query.Query) provider.CompiledQuery {
	return provider.CompiledQuery{SQL: "SELECT FROM " + meta.Name, Params: nil}
}

func (m *mockProvider) ExplainFindByPK(meta *model.EntityMeta, pkValue any) provider.CompiledQuery {
	return provider.CompiledQuery{SQL: "SELECT FROM " + meta.Name + " WHERE pk=?", Params: []any{pkValue}}
}

func (m *mockProvider) ExplainInsert(meta *model.EntityMeta, entity any) provider.CompiledQuery {
	return provider.CompiledQuery{SQL: "INSERT INTO " + meta.Name, Params: []any{"mock"}}
}

func (m *mockProvider) ExplainUpdate(meta *model.EntityMeta, entity any, changed []string) provider.CompiledQuery {
	return provider.CompiledQuery{SQL: "UPDATE " + meta.Name, Params: []any{"mock"}}
}

func (m *mockProvider) ExplainDelete(meta *model.EntityMeta, pkValue any) provider.CompiledQuery {
	return provider.CompiledQuery{SQL: "DELETE FROM " + meta.Name + " WHERE pk=?", Params: []any{pkValue}}
}

// --- test entities ---

type TestUser struct {
	ID   int    `db:"column:id; primary_key; auto_increment"`
	Name string `db:"column:name"`
	Age  int    `db:"column:age"`
}

// --- tests ---

func TestSetFind(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	var u TestUser
	if err := ctx.Set(&u).Find(42); err != nil {
		t.Fatalf("Find: %v", err)
	}

	if u.ID != 42 {
		t.Fatalf("expected ID=42, got %d", u.ID)
	}

	// Must be tracked as Unchanged
	entry, ok := ctx.Entry(&u)
	if !ok {
		t.Fatal("entity not tracked after Find")
	}
	if entry.State != model.Unchanged {
		t.Fatalf("expected Unchanged, got %v", entry.State)
	}
}

func TestSetFindWithReadRetry(t *testing.T) {
	var calls int32
	mp := &mockProvider{
		findErr: errors.New("connection reset"),
	}
	ctx := wctx.New(mp,
		wctx.WithReadRetry(resiliency.WithAttempts(3)),
	)

	var u TestUser
	err := ctx.Set(&u).Find(1)
	if err == nil {
		t.Fatal("expected error after retries")
	}

	calls = atomic.LoadInt32(&mp.findCalls)
	if calls != 3 {
		t.Fatalf("expected 3 retry attempts, got %d", calls)
	}
}

func TestSetFindWithCircuitBreaker(t *testing.T) {
	mp := &mockProvider{
		findErr: errors.New("db down"),
	}
	ctx := wctx.New(mp,
		wctx.WithCircuitBreaker(2, 100*time.Millisecond),
	)

	var u TestUser

	// First two calls fail → breaker opens
	_ = ctx.Set(&u).Find(1)
	_ = ctx.Set(&u).Find(1)

	// Third call should fail with circuit open (not hitting provider)
	callsBefore := atomic.LoadInt32(&mp.findCalls)
	err := ctx.Set(&u).Find(1)
	callsAfter := atomic.LoadInt32(&mp.findCalls)

	if err == nil {
		t.Fatal("expected circuit breaker error")
	}
	if !errors.Is(err, resiliency.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got: %v", err)
	}
	if callsAfter != callsBefore {
		t.Fatal("provider should not be called when circuit is open")
	}
}

func TestSaveConvenience(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	// No pending changes → Save returns nil
	if err := ctx.Save(); err != nil {
		t.Fatalf("Save with no changes: %v", err)
	}
}

func TestSetWhereAll(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	var users []TestUser
	err := ctx.Set(&users).
		Where(query.Predicate{Field: "age", Op: query.OpGt, Value: 18}).
		Limit(10).
		All()

	if err != nil {
		t.Fatalf("All: %v", err)
	}

	calls := atomic.LoadInt32(&mp.execCalls)
	if calls != 1 {
		t.Fatalf("expected 1 Execute call, got %d", calls)
	}
}

func TestSetAddAndTrack(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	u := &TestUser{Name: "alice", Age: 30}
	ctx.Set(u).Add(u)

	entry, ok := ctx.Entry(u)
	if !ok {
		t.Fatal("entity not tracked after Add")
	}
	if entry.State != model.Added {
		t.Fatalf("expected Added, got %v", entry.State)
	}
}

// --- PendingSQL tests ---

func TestPendingSQL_Insert(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	u := &TestUser{Name: "alice", Age: 30}
	ctx.Add(u)

	changes, err := ctx.PendingSQL()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Operation != "INSERT" {
		t.Fatalf("expected INSERT, got %s", changes[0].Operation)
	}
	if changes[0].SQL == "" {
		t.Fatal("SQL should not be empty")
	}
}

func TestPendingSQL_Update(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	u := &TestUser{ID: 1, Name: "alice", Age: 30}
	ctx.Attach(u)

	u.Name = "bob"

	changes, err := ctx.PendingSQL()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Operation != "UPDATE" {
		t.Fatalf("expected UPDATE, got %s", changes[0].Operation)
	}
}

func TestPendingSQL_Delete(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	u := &TestUser{ID: 5, Name: "charlie", Age: 40}
	ctx.Attach(u)
	ctx.Remove(u)

	changes, err := ctx.PendingSQL()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	if changes[0].Operation != "DELETE" {
		t.Fatalf("expected DELETE, got %s", changes[0].Operation)
	}
}

func TestPendingSQL_Empty(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	changes, err := ctx.PendingSQL()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Fatalf("expected 0 changes, got %d", len(changes))
	}
}

func TestPendingSQL_Mixed(t *testing.T) {
	mp := &mockProvider{}
	ctx := wctx.New(mp)

	u1 := &TestUser{Name: "new_user", Age: 20}
	ctx.Add(u1)

	u2 := &TestUser{ID: 2, Name: "existing", Age: 50}
	ctx.Attach(u2)
	u2.Age = 51

	u3 := &TestUser{ID: 3, Name: "to_delete", Age: 60}
	ctx.Attach(u3)
	ctx.Remove(u3)

	changes, err := ctx.PendingSQL()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(changes))
	}

	ops := map[string]bool{}
	for _, c := range changes {
		ops[c.Operation] = true
	}
	if !ops["INSERT"] || !ops["UPDATE"] || !ops["DELETE"] {
		t.Fatalf("expected INSERT+UPDATE+DELETE, got %v", ops)
	}
}
