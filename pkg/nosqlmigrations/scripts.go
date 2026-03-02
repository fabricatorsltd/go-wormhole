package nosqlmigrations

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fabricatorsltd/go-wormhole/pkg/util"
)

func GenerateTemplate(name string) Script {
	id := time.Now().UTC().Format("20060102150405") + "_" + util.ToSnake(name)
	return Script{
		ID:        id,
		Name:      name,
		CreatedAt: time.Now().UTC(),
		Steps: []Step{{
			Type:       StepBackfillField,
			Collection: "users",
			Field:      "status",
			Value:      "active",
		}},
	}
}

func SaveScript(dir string, s Script) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, s.ID+".nosql.json")
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, b, 0o644)
}

func LoadScripts(dir string) ([]Script, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".nosql.json") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	out := make([]Script, 0, len(files))
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var s Script
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}


