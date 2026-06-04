package migrations

import (
	"context"
	"database/sql"
	"fmt"
)

// MigrationLock is an optional dialect capability: a session-scoped advisory
// lock that serializes concurrent migration runs so two processes cannot apply
// the same pending migrations at once. A dialect that does not need one (SQLite
// is single-writer) simply does not implement it, and migrations run unguarded.
//
// Both methods receive the same dedicated *sql.Conn so the lock, which most
// engines scope to a session, is acquired and released on one connection.
type MigrationLock interface {
	AcquireLock(ctx context.Context, conn *sql.Conn) error
	ReleaseLock(ctx context.Context, conn *sql.Conn) error
}

// withMigrationLock runs fn while holding the dialect's advisory lock on a
// dedicated connection. Dialects that do not implement MigrationLock (e.g.
// SQLite) run fn directly. The lock is released even if fn fails or ctx is
// cancelled, on the same connection it was taken.
//
// With a locking dialect the pool must allow at least two connections: the lock
// holds one for the whole critical section while fn opens another to apply the
// migrations. SQLite, the only backend commonly capped at one connection, takes
// the no-lock fast path, so this is not a concern there.
func withMigrationLock(ctx context.Context, db *sql.DB, dialect Dialect, fn func() error) error {
	locker, ok := dialect.(MigrationLock)
	if !ok {
		return fn()
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open lock connection: %w", err)
	}
	defer conn.Close()

	if err := locker.AcquireLock(ctx, conn); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// Release on a non-cancelled context so a cancelled run still frees the
		// lock for the next process.
		_ = locker.ReleaseLock(context.WithoutCancel(ctx), conn)
	}()

	return fn()
}
