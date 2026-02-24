package nosqlmigrations

import "context"

type Runner struct {
	exec    Executor
	history HistoryStore
}

func NewRunner(exec Executor, history HistoryStore) *Runner {
	return &Runner{exec: exec, history: history}
}

func (r *Runner) ApplyPending(ctx context.Context, scripts []Script) (int, error) {
	applied, err := r.history.AppliedSet(ctx)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, s := range scripts {
		if applied[s.ID] {
			continue
		}
		for _, step := range s.Steps {
			if err := r.applyStep(ctx, step); err != nil {
				return count, err
			}
		}
		if err := r.history.Record(ctx, s.ID); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (r *Runner) applyStep(ctx context.Context, s Step) error {
	switch s.Type {
	case StepBackfillField:
		return r.exec.BackfillField(ctx, s.Collection, s.Field, s.Value)
	case StepRenameField:
		return r.exec.RenameField(ctx, s.Collection, s.From, s.To)
	case StepSplitField:
		return r.exec.SplitField(ctx, s.Collection, s.Field, s.Fields, s.Delimiter)
	case StepMergeFields:
		return r.exec.MergeFields(ctx, s.Collection, s.Fields, s.Field, s.Delimiter)
	case StepCreateIndex:
		return r.exec.CreateIndex(ctx, s.Collection, s.IndexName, s.IndexKeys, s.Unique)
	case StepDropIndex:
		return r.exec.DropIndex(ctx, s.Collection, s.IndexName)
	default:
		return nil
	}
}
