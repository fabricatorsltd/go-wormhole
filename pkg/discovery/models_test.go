package discovery

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverModels(t *testing.T) {
	// Create a temporary directory with test Go files
	tmpDir := t.TempDir()

	// Create a test Go file with a model
	testFile := `package main

type User struct {
	ID   int    ` + "`db:\"primary_key;auto_increment\"`" + `
	Name string ` + "`db:\"column:name\"`" + `
	Email string ` + "`db:\"column:email;nullable\"`" + `
}

type Product struct {
	ID    int     ` + "`db:\"primary_key;auto_increment\"`" + `
	Name  string  ` + "`db:\"column:name\"`" + `
	Price float64 ` + "`db:\"column:price;type:decimal(10,2)\"`" + `
}

type NoTagsStruct struct {
	ID   int
	Name string
}
`

	testPath := filepath.Join(tmpDir, "models.go")
	if err := os.WriteFile(testPath, []byte(testFile), 0644); err != nil {
		t.Fatal(err)
	}

	// Test discovery
	models, err := DiscoverModels(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverModels failed: %v", err)
	}

	// Should find 2 models (User and Product), not NoTagsStruct
	if len(models) != 2 {
		t.Fatalf("Expected 2 models, got %d", len(models))
	}

	// Check that we found the expected models
	modelNames := make(map[string]bool)
	for _, model := range models {
		modelNames[model.Name] = true
	}

	if !modelNames["user"] || !modelNames["product"] {
		t.Fatalf("Expected to find 'user' and 'product' models, got: %v", modelNames)
	}
}

func TestDiscoverModelsEmptyDir(t *testing.T) {
	tmpDir := t.TempDir()

	models, err := DiscoverModels(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverModels failed: %v", err)
	}

	if len(models) != 0 {
		t.Fatalf("Expected 0 models in empty dir, got %d", len(models))
	}
}
