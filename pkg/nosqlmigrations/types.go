package nosqlmigrations

import (
	"context"
	"time"
)

type StepType string

const (
	StepBackfillField StepType = "backfill_field"
	StepRenameField   StepType = "rename_field"
	StepSplitField    StepType = "split_field"
	StepMergeFields   StepType = "merge_fields"
	StepCreateIndex   StepType = "create_index"
	StepDropIndex     StepType = "drop_index"
)

type Step struct {
	Type       StepType       `json:"type"`
	Collection string         `json:"collection"`
	Field      string         `json:"field,omitempty"`
	From       string         `json:"from,omitempty"`
	To         string         `json:"to,omitempty"`
	Fields     []string       `json:"fields,omitempty"`
	Delimiter  string         `json:"delimiter,omitempty"`
	Value      any            `json:"value,omitempty"`
	IndexName  string         `json:"index_name,omitempty"`
	IndexKeys  map[string]int `json:"index_keys,omitempty"`
	Unique     bool           `json:"unique,omitempty"`
}

type Script struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	Steps     []Step    `json:"steps"`
}

type HistoryRecord struct {
	ID        string    `json:"id"`
	AppliedAt time.Time `json:"applied_at"`
}

type HistoryStore interface {
	AppliedSet(ctx context.Context) (map[string]bool, error)
	Record(ctx context.Context, id string) error
}

type Executor interface {
	BackfillField(ctx context.Context, collection, field string, value any) error
	RenameField(ctx context.Context, collection, from, to string) error
	SplitField(ctx context.Context, collection, field string, targets []string, delimiter string) error
	MergeFields(ctx context.Context, collection string, fields []string, target, delimiter string) error
	CreateIndex(ctx context.Context, collection, name string, keys map[string]int, unique bool) error
	DropIndex(ctx context.Context, collection, name string) error
}
