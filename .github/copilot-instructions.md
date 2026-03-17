# Copilot Instructions for go-wormhole

## Build, Test, and Lint Commands

- **Build:**
  - `go build ./...`
  - CLI tool: `go build ./cmd/wormhole`
- **Test:**
  - All tests: `go test ./...`
  - Per-package: `go test ./pkg/context`
  - Single test: `go test ./pkg/context -run TestSetFind`
- **Lint:**
  - Formatting: `go fmt ./...`
  - Vetting: `go vet ./...`

## Key Architecture

- **NO CGO POLICY**: Uses `github.com/glebarez/sqlite` (pure Go) instead of CGO-dependent sqlite drivers
- **Global CLI Tool**: Install with `go install github.com/fabricatorsltd/go-wormhole/cmd/wormhole@latest`
- **Auto-Discovery**: CLI automatically discovers models with `db` tags in current directory
- **DbContext:** Entry point for all operations. Manages entity tracking, change detection, and transactions
- **EntitySet:** Fluent API for querying/manipulating entities
- **Provider:** Pluggable storage backend interface (SQL, NoSQL, custom)
- **DSL:** Type-safe pointer-tracking query builder
- **Migrations:** Code-first migrations with both Go and SQL file generation

## CLI Usage (Entity Framework-like)

```bash
# Install globally (NO CGO)
go install github.com/fabricatorsltd/go-wormhole/cmd/wormhole@latest

# Set environment
export WORMHOLE_DSN="./app.db"

# Generate migration from model changes
wormhole migrations add CreateUserTable

# Apply pending migrations
wormhole database update

# List migration status
wormhole migrations list
```

## Key Conventions

- **NO CGO**: Pure Go implementation, cross-platform compatible
- **Schema Source of Truth:** Go structs with `db` tags define schema
- **Auto-Discovery:** CLI scans for structs with `db` tags automatically
- **Dual Migration Files:** Generates both `.go` and `.sql` files for each migration
- **Provider Registration:** Providers implement the `Provider` interface
- **Resilience:** Use retry/circuit breaker options for DbContext as needed
- **Testing:** Use mock providers for unit tests

## Environment Variables (CLI)

- `WORMHOLE_DSN`: Database connection string (required for CLI operations)
- `WORMHOLE_DRIVER`: SQL driver name (default: sqlite)
- `WORMHOLE_DIR`: Migrations directory (default: ./migrations)

## Model Example

```go
type User struct {
    ID    int    `db:"primary_key;auto_increment"`
    Name  string `db:"column:name"`
    Email string `db:"column:email;nullable"`
}
```

## References
- [README.md](../README.md)
- [docs/01-getting-started.md](../docs/01-getting-started.md)
- [docs/04-migrations.md](../docs/04-migrations.md)
- [docs/09-global-cli.md](../docs/09-global-cli.md) - Global CLI tool usage

---

This file is intended for GitHub Copilot and other AI tools to provide accurate, context-aware assistance for the go-wormhole repository.
