package context_test

import (
	stdctx "context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	wctx "github.com/mirkobrombin/go-wormhole/pkg/context"
	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/provider"
	"github.com/mirkobrombin/go-wormhole/pkg/query"

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

func (m *mockProvider) Name() string                                       { return "mock" }
func (m *mockProvider) Open(_ stdctx.Context, _ string) error              { return nil }
func (m *mockProvider) Close() error                                       { return nil }
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
