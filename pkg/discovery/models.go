package discovery

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

// DiscoverModels scans the current directory and its subdirectories for Go structs
// that have wormhole `db` tags and returns their EntityMeta.
func DiscoverModels(rootDir string) ([]*model.EntityMeta, error) {
	if rootDir == "" {
		rootDir = "."
	}

	var models []*model.EntityMeta
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip vendor, .git, and other common directories (but not root directory)
		if info.IsDir() {
			name := filepath.Base(path)
			if path != rootDir && (name == "vendor" || name == ".git" || name == "node_modules" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}

		// Only process Go files
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		fileModels, err := parseGoFile(path)
		if err != nil {
			// Log warning but don't fail the entire discovery
			fmt.Fprintf(os.Stderr, "Warning: failed to parse %s: %v\n", path, err)
			return nil
		}

		models = append(models, fileModels...)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory: %w", err)
	}

	return models, nil
}

// parseGoFile parses a single Go file and extracts structs with wormhole tags
func parseGoFile(filePath string) ([]*model.EntityMeta, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var models []*model.EntityMeta

	// Walk the AST to find structs with db tags
	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.TypeSpec:
			if structType, ok := x.Type.(*ast.StructType); ok {
				if hasWormholeTags(structType) {
					// Create a dummy reflect.Type for schema parsing
					// This is a simplified approach - in a more complete implementation,
					// we might need to actually compile and load the types
					meta := createMetaFromAST(x.Name.Name, structType)
					if meta != nil {
						models = append(models, meta)
					}
				}
			}
		}
		return true
	})

	return models, nil
}

// hasWormholeTags checks if a struct has any fields with `db` tags
func hasWormholeTags(structType *ast.StructType) bool {
	for _, field := range structType.Fields.List {
		if field.Tag != nil {
			tag := strings.Trim(field.Tag.Value, "`")
			if strings.Contains(tag, "db:") {
				return true
			}
		}
	}
	return false
}

// createMetaFromAST creates EntityMeta from AST (simplified version)
func createMetaFromAST(typeName string, structType *ast.StructType) *model.EntityMeta {
	meta := &model.EntityMeta{
		Name: toSnakeCase(typeName), // Use local snake_case function
	}

	for _, field := range structType.Fields.List {
		if field.Tag == nil {
			continue
		}

		tag := strings.Trim(field.Tag.Value, "`")
		if !strings.Contains(tag, "db:") {
			continue
		}

		// Extract field names and types
		var fieldNames []string
		for _, name := range field.Names {
			fieldNames = append(fieldNames, name.Name)
		}

		// Determine the Go type (and whether it is a pointer, i.e. nullable)
		goType, isPointer := extractGoTypeFromAST(field.Type)

		// Parse the db tag
		if len(fieldNames) > 0 {
			fieldMeta := parseFieldFromTagWithType(fieldNames[0], tag, goType, isPointer)
			if fieldMeta != nil {
				meta.Fields = append(meta.Fields, *fieldMeta)
				if fieldMeta.PrimaryKey {
					meta.PrimaryKey = &meta.Fields[len(meta.Fields)-1]
				}
			}
		}
	}

	if len(meta.Fields) > 0 {
		meta.BuildIndex()
		return meta
	}
	return nil
}

// extractGoTypeFromAST determines the underlying Go type from the AST and
// reports whether the field is a pointer (the nullability signal). Pointers
// are unwrapped to their element type; []byte is reported as a blob scalar and
// other slices/maps as composite types so they no longer collapse to "string".
func extractGoTypeFromAST(expr ast.Expr) (goType string, isPointer bool) {
	switch t := expr.(type) {
	case *ast.StarExpr:
		inner, _ := extractGoTypeFromAST(t.X)
		return inner, true
	case *ast.Ident:
		return t.Name, false
	case *ast.SelectorExpr:
		return fmt.Sprintf("%s.%s", t.X, t.Sel.Name), false
	case *ast.ArrayType:
		if id, ok := t.Elt.(*ast.Ident); ok && id.Name == "byte" {
			return "[]byte", false
		}
		return "slice", false
	case *ast.MapType:
		return "map", false
	default:
		return "string", false
	}
}

// parseFieldFromTagWithType is like parseFieldFromTag but maps the Go type to a
// SQL type and infers nullability from pointer-ness. A pointer field is marked
// nullable (a tag may also set it). The timestamp type is portable "TIMESTAMP";
// dialects render it natively (Postgres accepts it as a timestamp column).
func parseFieldFromTagWithType(fieldName, tag, goType string, isPointer bool) *model.FieldMeta {
	field := parseFieldFromTag(fieldName, tag)
	if field == nil {
		return nil
	}

	if isPointer {
		field.Nullable = true
	}

	// Set the default SQL type from the Go type when not explicitly tagged. The
	// AST path sees the type as a string, so this mirrors GoTypeToSQL (the
	// reflection path) by hand: 64-bit integers map to BIGINT and float64 to
	// DOUBLE PRECISION, so both generators emit the same DDL for a wide type.
	if _, hasType := field.Tags["type"]; !hasType {
		switch goType {
		case "int64", "uint64":
			field.Tags["type"] = "BIGINT"
		case "int", "int8", "int16", "int32",
			"uint", "uint8", "uint16", "uint32":
			field.Tags["type"] = "INTEGER"
		case "float64":
			field.Tags["type"] = "DOUBLE PRECISION"
		case "float32":
			field.Tags["type"] = "REAL"
		case "bool":
			field.Tags["type"] = "BOOLEAN"
		case "time.Time":
			field.Tags["type"] = "TIMESTAMP"
		case "[]byte":
			field.Tags["type"] = "BLOB"
		default:
			field.Tags["type"] = "TEXT"
		}
	}

	return field
}

// toSnakeCase converts PascalCase to snake_case (local implementation)
func toSnakeCase(s string) string {
	if s == "" {
		return ""
	}

	// Special case for common abbreviations
	if s == "ID" {
		return "id"
	}

	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteRune('_')
		}
		if r >= 'A' && r <= 'Z' {
			result.WriteRune(r - 'A' + 'a') // Convert uppercase to lowercase
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// parseFieldFromTag extracts field metadata from struct tag
func parseFieldFromTag(fieldName, tag string) *model.FieldMeta {
	// Simple tag parsing - extract db tag content
	parts := strings.Split(tag, " ")
	var dbTag string
	for _, part := range parts {
		if strings.HasPrefix(part, "db:") {
			dbTag = strings.Trim(strings.TrimPrefix(part, "db:"), `"`)
			break
		}
	}

	if dbTag == "" {
		return nil
	}

	field := &model.FieldMeta{
		FieldName: fieldName,
		Column:    toSnakeCase(fieldName), // Use local snake_case function
		Tags:      make(map[string]string),
	}

	// Parse tag options (simplified)
	options := strings.Split(dbTag, ";")
	for _, opt := range options {
		if opt == "primary_key" {
			field.PrimaryKey = true
		} else if opt == "auto_increment" {
			field.AutoIncr = true
		} else if opt == "nullable" {
			field.Nullable = true
		} else if strings.HasPrefix(opt, "column:") {
			field.Column = strings.TrimPrefix(opt, "column:")
		} else if strings.HasPrefix(opt, "type:") {
			field.Tags["type"] = strings.TrimPrefix(opt, "type:")
		} else if strings.HasPrefix(opt, "default:") {
			field.Tags["default"] = strings.TrimPrefix(opt, "default:")
		} else if opt == "unique" || opt == "unique_index" {
			field.Indexed = true
			field.Unique = true
			field.Indexes = append(field.Indexes, model.IndexRef{Unique: true})
		} else if opt == "index" {
			field.Indexed = true
			field.Indexes = append(field.Indexes, model.IndexRef{})
		} else if strings.HasPrefix(opt, "index:") {
			field.Indexed = true
			addIndexRefs(field, strings.TrimPrefix(opt, "index:"), false)
		} else if strings.HasPrefix(opt, "unique_index:") {
			field.Indexed = true
			field.Unique = true
			addIndexRefs(field, strings.TrimPrefix(opt, "unique_index:"), true)
		}
	}

	return field
}

// addIndexRefs appends one membership per comma-separated index name (each with
// an optional :N position) and keeps the primary single-index fields pointed at
// the first one, for back-compat with readers that predate FieldMeta.Indexes.
func addIndexRefs(field *model.FieldMeta, spec string, unique bool) {
	for _, v := range strings.Split(spec, ",") {
		name, order := model.ParseIndexSpec(v)
		field.Indexes = append(field.Indexes, model.IndexRef{Name: name, Order: order, Unique: unique})
		if field.Index == "" {
			field.Index, field.IndexOrder = name, order
		}
	}
}

// DiscoverModelsFromReflection tries to discover models that are already parsed/registered
// This works with the existing schema.Parse cache
func DiscoverModelsFromReflection() []*model.EntityMeta {
	// This would need access to the schema cache, which is currently private
	// For now, we'll return empty and rely on file-based discovery
	return nil
}
