package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

// Migration represents a single migration with Up and Down methods.
type Migration struct {
	ID   string
	Up   func(b *SchemaBuilder)
	Down func(b *SchemaBuilder)
}

// Runner executes pending migrations against a database.
type Runner struct {
	db         *sql.DB
	migrations []Migration
	dialect    Dialect
}

// NewRunner creates a migration runner.
func NewRunner(db *sql.DB, dialect ...Dialect) *Runner {
	var d Dialect = DefaultDialect{}
	if len(dialect) > 0 {
		d = dialect[0]
	}
	return &Runner{db: db, dialect: d}
}

// Add registers a migration. Migrations are sorted by ID before execution.
func (r *Runner) Add(m Migration) {
	r.migrations = append(r.migrations, m)
}

// Pending returns the list of migration IDs that haven't been applied yet.
func (r *Runner) Pending(ctx context.Context) ([]string, error) {
	if err := EnsureHistoryTable(ctx, r.db); err != nil {
		return nil, err
	}

	applied, err := AppliedMigrations(ctx, r.db)
	if err != nil {
		return nil, err
	}

	sort.Slice(r.migrations, func(i, j int) bool {
		return r.migrations[i].ID < r.migrations[j].ID
	})

	var pending []string
	for _, m := range r.migrations {
		if !applied[m.ID] {
			pending = append(pending, m.ID)
		}
	}
	return pending, nil
}

// Up applies all pending migrations in order.
// Each migration runs in its own transaction (DDL + history record).
func (r *Runner) Up(ctx context.Context) error {
	if err := EnsureHistoryTable(ctx, r.db); err != nil {
		return err
	}

	applied, err := AppliedMigrations(ctx, r.db)
	if err != nil {
		return err
	}

	sort.Slice(r.migrations, func(i, j int) bool {
		return r.migrations[i].ID < r.migrations[j].ID
	})

	for _, m := range r.migrations {
		if applied[m.ID] {
			continue
		}
		if err := r.applyUp(ctx, m); err != nil {
			return fmt.Errorf("migration %s: %w", m.ID, err)
		}
	}
	return nil
}

// Down rolls back the last applied migration.
func (r *Runner) Down(ctx context.Context) error {
	if err := EnsureHistoryTable(ctx, r.db); err != nil {
		return err
	}

	applied, err := AppliedMigrations(ctx, r.db)
	if err != nil {
		return err
	}

	// Find last applied
	sort.Slice(r.migrations, func(i, j int) bool {
		return r.migrations[i].ID < r.migrations[j].ID
	})

	var last *Migration
	for i := len(r.migrations) - 1; i >= 0; i-- {
		if applied[r.migrations[i].ID] {
			last = &r.migrations[i]
			break
		}
	}
	if last == nil {
		return nil // nothing to roll back
	}

	return r.applyDown(ctx, *last)
}

func (r *Runner) applyUp(ctx context.Context, m Migration) error {
	b := NewBuilderWith(r.dialect)
	m.Up(b)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	for _, stmt := range b.Statements() {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}

	if err := RecordMigration(ctx, tx, m.ID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("record: %w", err)
	}

	return tx.Commit()
}

func (r *Runner) applyDown(ctx context.Context, m Migration) error {
	b := NewBuilderWith(r.dialect)
	m.Down(b)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	for _, stmt := range b.Statements() {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}

	if err := RemoveMigration(ctx, tx, m.ID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("remove record: %w", err)
	}

	return tx.Commit()
}
