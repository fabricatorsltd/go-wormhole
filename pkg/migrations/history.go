package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const historyTable = "_wormhole_migrations_history"

// HistoryRecord represents a single applied migration.
type HistoryRecord struct {
	MigrationID string
	AppliedAt   time.Time
}

// EnsureHistoryTable creates the migrations history table if it doesn't exist.
func EnsureHistoryTable(ctx context.Context, db *sql.DB) error {
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		"migration_id" TEXT PRIMARY KEY,
		"applied_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`, historyTable)
	_, err := db.ExecContext(ctx, ddl)
	return err
}

// AppliedMigrations returns the set of migration IDs already applied.
func AppliedMigrations(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf(`SELECT "migration_id" FROM "%s" ORDER BY "migration_id"`, historyTable))
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		applied[id] = true
	}
	return applied, rows.Err()
}

// RecordMigration inserts a migration ID into the history table within a transaction.
func RecordMigration(ctx context.Context, tx *sql.Tx, migrationID string) error {
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO "%s" ("migration_id", "applied_at") VALUES (?, ?)`,
			historyTable),
		migrationID, time.Now().UTC())
	return err
}

// RemoveMigration deletes a migration ID from the history table (for rollback).
func RemoveMigration(ctx context.Context, tx *sql.Tx, migrationID string) error {
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM "%s" WHERE "migration_id" = ?`, historyTable),
		migrationID)
	return err
}
