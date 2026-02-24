package nosqlmigrations

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FileHistoryStore struct {
	path string
	mu   sync.Mutex
}

func NewFileHistoryStore(path string) *FileHistoryStore {
	return &FileHistoryStore{path: path}
}

func (h *FileHistoryStore) AppliedSet(_ context.Context) (map[string]bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	recs, err := h.load()
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(recs))
	for _, r := range recs {
		out[r.ID] = true
	}
	return out, nil
}

func (h *FileHistoryStore) Record(_ context.Context, id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	recs, err := h.load()
	if err != nil {
		return err
	}
	for _, r := range recs {
		if r.ID == id {
			return nil
		}
	}
	recs = append(recs, HistoryRecord{ID: id, AppliedAt: time.Now().UTC()})
	return h.save(recs)
}

func (h *FileHistoryStore) load() ([]HistoryRecord, error) {
	b, err := os.ReadFile(h.path)
	if err != nil {
		if os.IsNotExist(err) {
			return []HistoryRecord{}, nil
		}
		return nil, err
	}
	var recs []HistoryRecord
	if len(b) == 0 {
		return []HistoryRecord{}, nil
	}
	if err := json.Unmarshal(b, &recs); err != nil {
		return nil, err
	}
	return recs, nil
}

func (h *FileHistoryStore) save(recs []HistoryRecord) error {
	if err := os.MkdirAll(filepath.Dir(h.path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(h.path, b, 0o644)
}
