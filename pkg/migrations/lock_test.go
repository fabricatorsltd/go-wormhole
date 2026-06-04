package migrations

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/glebarez/sqlite"
)

// spyLock records the order of acquire/run/release so the wiring can be checked
// without a real database lock.
type spyLock struct {
	calls      *[]string
	acquireErr error
	DefaultDialect
}

func (s spyLock) AcquireLock(ctx context.Context, conn *sql.Conn) error {
	*s.calls = append(*s.calls, "acquire")
	return s.acquireErr
}

func (s spyLock) ReleaseLock(ctx context.Context, conn *sql.Conn) error {
	*s.calls = append(*s.calls, "release")
	return nil
}

func openMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// withMigrationLock acquires the lock, runs the body, then releases, in order.
func TestWithMigrationLock_Order(t *testing.T) {
	db := openMem(t)
	var calls []string
	err := withMigrationLock(context.Background(), db, spyLock{calls: &calls}, func() error {
		calls = append(calls, "run")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := calls; len(got) != 3 || got[0] != "acquire" || got[1] != "run" || got[2] != "release" {
		t.Fatalf("call order: got %v, want [acquire run release]", got)
	}
}

// A failed acquire skips the body and surfaces the error.
func TestWithMigrationLock_AcquireFails(t *testing.T) {
	db := openMem(t)
	var calls []string
	sentinel := errors.New("locked by another process")
	err := withMigrationLock(context.Background(), db, spyLock{calls: &calls, acquireErr: sentinel}, func() error {
		calls = append(calls, "run")
		return nil
	})
	if err == nil {
		t.Fatal("expected error when acquire fails")
	}
	for _, c := range calls {
		if c == "run" {
			t.Fatal("body must not run when the lock cannot be acquired")
		}
	}
}

// A dialect that does not implement MigrationLock (e.g. SQLite) runs the body
// directly with no lock round-trip.
func TestWithMigrationLock_NoLocker(t *testing.T) {
	db := openMem(t)
	ran := false
	err := withMigrationLock(context.Background(), db, DefaultDialect{}, func() error {
		ran = true
		return nil
	})
	if err != nil || !ran {
		t.Fatalf("no-locker path: ran=%v err=%v", ran, err)
	}
}
