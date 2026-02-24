package nosqlmigrations

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type fakeExec struct{ calls []StepType }

func (f *fakeExec) BackfillField(context.Context, string, string, any) error {
	f.calls = append(f.calls, StepBackfillField)
	return nil
}
func (f *fakeExec) RenameField(context.Context, string, string, string) error {
	f.calls = append(f.calls, StepRenameField)
	return nil
}
func (f *fakeExec) SplitField(context.Context, string, string, []string, string) error {
	f.calls = append(f.calls, StepSplitField)
	return nil
}
func (f *fakeExec) MergeFields(context.Context, string, []string, string, string) error {
	f.calls = append(f.calls, StepMergeFields)
	return nil
}
func (f *fakeExec) CreateIndex(context.Context, string, string, map[string]int, bool) error {
	f.calls = append(f.calls, StepCreateIndex)
	return nil
}
func (f *fakeExec) DropIndex(context.Context, string, string) error {
	f.calls = append(f.calls, StepDropIndex)
	return nil
}

func TestScriptsSaveLoad(t *testing.T) {
	dir := t.TempDir()
	s := GenerateTemplate("Init")
	if _, err := SaveScript(dir, s); err != nil {
		t.Fatal(err)
	}
	list, err := LoadScripts(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != s.ID {
		t.Fatalf("unexpected scripts: %+v", list)
	}
}

func TestRunnerApplyPendingIdempotent(t *testing.T) {
	dir := t.TempDir()
	h := NewFileHistoryStore(filepath.Join(dir, ".history.json"))
	exec := &fakeExec{}
	r := NewRunner(exec, h)
	s := Script{ID: "001_init", Name: "init", Steps: []Step{{Type: StepBackfillField, Collection: "users", Field: "x", Value: 1}}}
	applied, err := r.ApplyPending(context.Background(), []Script{s})
	if err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("applied=%d", applied)
	}
	applied, err = r.ApplyPending(context.Background(), []Script{s})
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("applied second=%d", applied)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("calls=%d", len(exec.calls))
	}
	if _, err := os.Stat(filepath.Join(dir, ".history.json")); err != nil {
		t.Fatal(err)
	}
}
