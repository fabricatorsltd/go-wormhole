//go:build legacy_migrator

package migrations

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/tracker"
)

const (
	// DefaultBatchSize is the default number of records to process in a single batch.
	DefaultBatchSize = 1000
)

// DataMigrator handles streaming data between a source and destination provider.
type DataMigrator struct {
	sourceProvider      provider.Provider
	destinationProvider provider.Provider
	changeTracker       *tracker.ChangeTracker
	batchSize           int
}

// NewDataMigrator creates a new DataMigrator instance.
func NewDataMigrator(source, destination provider.Provider, changeTracker *tracker.ChangeTracker) *DataMigrator {
	return &DataMigrator{
		sourceProvider:      source,
		destinationProvider: destination,
		changeTracker:       changeTracker,
		batchSize:           DefaultBatchSize,
	}
}

// WithBatchSize sets the batch size for data migration.
func (dm *DataMigrator) WithBatchSize(size int) *DataMigrator {
	if size > 0 {
		dm.batchSize = size
	}
	return dm
}

// Sync synchronizes data for a given entity between source and destination.
func (dm *DataMigrator) Sync(ctx context.Context, entity *model.Entity) error {
	fmt.Printf("Starting synchronization for entity: %s\n", entity.Name)

	if entity.IsJunction {
		fmt.Printf("Skipping junction entity: %s\n", entity.Name)
		return nil
	}

	// 1. Disable identity insert (if applicable) on destination for auto-increment keys
	// This requires dialect specific implementation
	if err := dm.setIdentityInsert(ctx, entity, true); err != nil {
		return fmt.Errorf("failed to set identity insert for entity %s: %w", entity.Name, err)
	}
	// Ensure identity insert is re-enabled on function exit
	defer func() {
		if err := dm.setIdentityInsert(ctx, entity, false); err != nil {
			fmt.Printf("WARNING: Failed to unset identity insert for entity %s: %v\n", entity.Name, err)
		}
	}()

	// 2. Stream data from source to destination in batches
	if err := dm.streamAndBatchData(ctx, entity); err != nil {
		return fmt.Errorf("failed to stream and batch data for entity %s: %w", entity.Name, err)
	}

	fmt.Printf("Finished synchronization for entity: %s\n", entity.Name)
	return nil
}

// streamAndBatchData streams data from source, batches it, and writes to destination.
func (dm *DataMigrator) streamAndBatchData(ctx context.Context, entity *model.Entity) error {
	qb := builder.NewQueryBuilder()
	// Assuming a simple select all for now.
	// In a real scenario, this would involve change tracking or intelligent diffing.
	sel := qb.Select(entity.Name).All()

	cursor, err := dm.sourceProvider.Query(ctx, sel)
	if err != nil {
		return fmt.Errorf("failed to query source for entity %s: %w", entity.Name, err)
	}
	defer func() {
		if cerr := cursor.Close(); cerr != nil {
			fmt.Printf("WARNING: Failed to close source cursor for entity %s: %v\\n", entity.Name, cerr)
		}
	}()

	batch := make([]model.Document, 0, dm.batchSize)
	for cursor.Next(ctx) {
		doc, err := cursor.Decode()
		if err != nil {
			return fmt.Errorf("failed to decode document from source for entity %s: %w", entity.Name, err)
		}
		batch = append(batch, doc)

		if len(batch) == dm.batchSize {
			if err := dm.writeBatchToDestination(ctx, entity, batch); err != nil {
				return err
			}
			batch = make([]model.Document, 0, dm.batchSize) // Reset batch
		}
	}

	if cursor.Err() != nil {
		return fmt.Errorf("error during source cursor iteration for entity %s: %w", entity.Name, cursor.Err())
	}

	// Write any remaining documents in the last batch
	if len(batch) > 0 {
		if err := dm.writeBatchToDestination(ctx, entity, batch); err != nil {
			return err
		}
	}

	return nil
}

// writeBatchToDestination writes a batch of documents to the destination provider.
func (dm *DataMigrator) writeBatchToDestination(ctx context.Context, entity *model.Entity, batch []model.Document) error {
	// For simplicity, we'll use Upsert. In a real scenario, this might be Insert or Update based on change tracking.
	// We need to extract primary keys for upsert operations.
	// This assumes that the 'ID' field (or similar) is the primary key and is consistent.

	for _, doc := range batch {
		pkField := entity.PrimaryKeyField()
		if pkField == nil {
			return fmt.Errorf("entity %s has no primary key defined, cannot upsert", entity.Name)
		}

		pkValue, ok := doc.Fields[pkField.Name]
		if !ok {
			return fmt.Errorf("document for entity %s is missing primary key field '%s'", entity.Name, pkField.Name)
		}

		// Create a filter for the upsert operation based on the primary key
		filter := ast.NewBinaryExpr(
			ast.NewFieldSelector(pkField.Name),
			ast.OperatorEqual,
			ast.NewLiteral(pkValue),
		)

		// Create an upsert query
		upsert := query.NewUpsertQuery(entity.Name, doc, filter)

		_, err := dm.destinationProvider.Upsert(ctx, upsert)
		if err != nil {
			return fmt.Errorf("failed to upsert document for entity %s (PK: %v): %w", entity.Name, pkValue, err)
		}
	}
	return nil
}

// setIdentityInsert enables/disables identity insert on the destination provider for the given entity.
func (dm *DataMigrator) setIdentityInsert(ctx context.Context, entity *model.Entity, enable bool) error {
	dialect, ok := dm.destinationProvider.(provider.Dialect)
	if !ok {
		// Destination provider does not support Dialect interface, so no identity insert control is possible.
		// This is not an error, just means it's not applicable.
		return nil
	}

	for _, field := range entity.Fields {
		if field.IsPrimaryKey && field.IsAutoIncrement {
			fmt.Printf("Setting IdentityInsert for entity %s, primary key %s to %v\n", entity.Name, field.Name, enable)
			return dialect.SetIdentityInsert(ctx, entity.Name, enable)
		}
	}
	return nil
}

// FullSync performs a full synchronization of all entities from source to destination.
func (dm *DataMigrator) FullSync(ctx context.Context) error {
	fmt.Println("Starting full synchronization...")

	sourceSchema, err := dm.sourceProvider.Schema(ctx)
	if err != nil {
		return fmt.Errorf("failed to get source schema: %w", err)
	}

	// Determine entity processing order, e.g., by dependencies or alphabetical
	entities := make([]*model.Entity, 0, len(sourceSchema.Entities))
	for _, entity := range sourceSchema.Entities {
		entities = append(entities, entity)
	}

	// Simple alphabetical sort for now, consider dependency-based sorting for a robust solution
	// sort.Slice(entities, func(i, j int) bool {
	// 	return entities[i].Name < entities[j].Name
	// })

	for _, entity := range entities {
		if err := dm.Sync(ctx, entity); err != nil {
			return fmt.Errorf("failed to sync entity %s: %w", entity.Name, err)
		}
	}

	fmt.Println("Full synchronization completed.")
	return nil
}

// VerifySchema ensures that the schemas of the source and destination providers are compatible.
func (dm *DataMigrator) VerifySchema(ctx context.Context) error {
	fmt.Println("Verifying schema compatibility...")

	sourceSchema, err := dm.sourceProvider.Schema(ctx)
	if err != nil {
		return fmt.Errorf("failed to get source schema: %w", err)
	}

	destinationSchema, err := dm.destinationProvider.Schema(ctx)
	if err != nil {
		return fmt.Errorf("failed to get destination schema: %w", err)
	}

	// Basic verification: check if all source entities exist in destination with compatible primary keys.
	// More sophisticated checks would involve comparing all fields, types, indexes, etc.
	for _, srcEntity := range sourceSchema.Entities {
		destEntity, ok := destinationSchema.Entities[srcEntity.Name]
		if !ok {
			return fmt.Errorf("entity '%s' from source does not exist in destination", srcEntity.Name)
		}

		// Compare primary keys
		srcPK := srcEntity.PrimaryKeyField()
		destPK := destEntity.PrimaryKeyField()

		if srcPK == nil && destPK != nil {
			return fmt.Errorf("entity '%s': source has no primary key, but destination does", srcEntity.Name)
		}
		if srcPK != nil && destPK == nil {
			return fmt.Errorf("entity '%s': source has primary key '%s', but destination has none", srcEntity.Name, srcPK.Name)
		}
		if srcPK != nil && destPK != nil {
			if srcPK.Name != destPK.Name || srcPK.Type != destPK.Type {
				return fmt.Errorf("entity '%s': primary key mismatch. Source: %s (%s), Destination: %s (%s)",
					srcEntity.Name, srcPK.Name, srcPK.Type, destPK.Name, destPK.Type)
			}
		}

		// TODO: Add more comprehensive field and type comparison
	}

	fmt.Println("Schema verification completed successfully.")
	return nil
}

// DiffAndSync identifies changes using ChangeTracker and applies them to the destination.
func (dm *DataMigrator) DiffAndSync(ctx context.Context) error {
	fmt.Println("Starting differential synchronization...")

	if dm.changeTracker == nil {
		return fmt.Errorf("change tracker is not initialized for differential synchronization")
	}

	// Assuming ChangeTracker has already identified changes for relevant entities.
	// This method would iterate through the changes (inserts, updates, deletes)
	// and apply them to the destination provider.

	// Placeholder logic: This needs actual implementation based on how ChangeTracker exposes changes.
	// For a proof of concept, we'll assume the ChangeTracker provides a list of entities to resync.

	// Example of how it *might* work:
	// changedEntities, err := dm.changeTracker.GetChangedEntities(ctx)
	// if err != nil {
	// 	return fmt.Errorf("failed to get changed entities from tracker: %w", err)
	// }

	// for _, entity := range changedEntities {
	// 	fmt.Printf("Applying changes for entity: %s\n", entity.Name)
	// 	// Here, we would fetch specific changed records, not necessarily full sync.
	// 	// For simplicity, calling full Sync for now.
	// 	if err := dm.Sync(ctx, entity); err != nil {
	// 		return fmt.Errorf("failed to sync changed entity %s: %w", entity.Name, err)
	// 	}
	// }

	fmt.Println("Differential synchronization completed.")
	return nil
}

// MigrationHistory manages the migration history in the destination database.
type MigrationHistory struct {
	destinationProvider provider.Provider
}

// NewMigrationHistory creates a new MigrationHistory instance.
func NewMigrationHistory(destination provider.Provider) *MigrationHistory {
	return &MigrationHistory{
		destinationProvider: destination,
	}
}

// ApplyMigrationRecord records an applied migration to the destination.
func (mh *MigrationHistory) ApplyMigrationRecord(ctx context.Context, migration *AppliedMigration) error {
	doc := model.NewDocument(MigrationHistoryEntityName, migration)
	insert := query.NewInsertQuery(MigrationHistoryEntityName, doc)
	_, err := mh.destinationProvider.Insert(ctx, insert)
	if err != nil {
		return fmt.Errorf("failed to record migration '%s' in history: %w", migration.ID, err)
	}
	return nil
}

// GetAppliedMigrations retrieves all applied migrations from the destination.
func (mh *MigrationHistory) GetAppliedMigrations(ctx context.Context) ([]*AppliedMigration, error) {
	qb := builder.NewQueryBuilder()
	sel := qb.Select(MigrationHistoryEntityName).All()

	cursor, err := mh.destinationProvider.Query(ctx, sel)
	if err != nil {
		return nil, fmt.Errorf("failed to query migration history: %w", err)
	}
	defer func() {
		if cerr := cursor.Close(); cerr != nil {
			fmt.Printf("WARNING: Failed to close migration history cursor: %v\n", cerr)
		}
	}()

	var appliedMigrations []*AppliedMigration
	for cursor.Next(ctx) {
		doc, err := cursor.Decode()
		if err != nil {
			return nil, fmt.Errorf("failed to decode migration history document: %w", err)
		}
		var migration AppliedMigration
		if err := mapDocumentToStruct(doc, &migration); err != nil {
			return nil, fmt.Errorf("failed to map document to AppliedMigration: %w", err)
		}
		appliedMigrations = append(appliedMigrations, &migration)
	}

	if cursor.Err() != nil {
		return nil, fmt.Errorf("error during migration history cursor iteration: %w", cursor.Err())
	}

	return appliedMigrations, nil
}

// GetLatestAppliedMigration retrieves the latest applied migration.
func (mh *MigrationHistory) GetLatestAppliedMigration(ctx context.Context) (*AppliedMigration, error) {
	qb := builder.NewQueryBuilder()
	// Assuming migrations are ordered by ID (e.g., timestamp) or by an 'AppliedAt' field
	// This query needs to be more robust for ordering. For now, we'll fetch all and sort.
	sel := qb.Select(MigrationHistoryEntityName).All()

	cursor, err := mh.destinationProvider.Query(ctx, sel)
	if err != nil {
		return nil, fmt.Errorf("failed to query migration history for latest: %w", err)
	}
	defer func() {
		if cerr := cursor.Close(); cerr != nil {
			fmt.Printf("WARNING: Failed to close migration history cursor for latest: %v\n", cerr)
		}
	}()

	var latestMigration *AppliedMigration
	var latestAppliedAt time.Time

	for cursor.Next(ctx) {
		doc, err := cursor.Decode()
		if err != nil {
			return nil, fmt.Errorf("failed to decode migration history document for latest: %w", err)
		}
		var migration AppliedMigration
		if err := mapDocumentToStruct(doc, &migration); err != nil {
			return nil, fmt.Errorf("failed to map document to AppliedMigration for latest: %w", err)
		}

		if latestMigration == nil || migration.AppliedAt.After(latestAppliedAt) {
			latestMigration = &migration
			latestAppliedAt = migration.AppliedAt
		}
	}

	if cursor.Err() != nil {
		return nil, fmt.Errorf("error during migration history cursor iteration for latest: %w", cursor.Err())
	}

	return latestMigration, nil
}

// EnsureMigrationHistoryTableExists ensures the migration history table exists in the destination.
func (mh *MigrationHistory) EnsureMigrationHistoryTableExists(ctx context.Context) error {
	fmt.Printf("Ensuring migration history table '%s' exists...\n", MigrationHistoryEntityName)

	migrationHistorySchema := mh.getMigrationHistorySchema()
	_, err := mh.destinationProvider.CreateTable(ctx, migrationHistorySchema)
	if err != nil {
		return fmt.Errorf("failed to create migration history table '%s': %w", MigrationHistoryEntityName, err)
	}

	fmt.Printf("Migration history table '%s' ensured.\n", MigrationHistoryEntityName)
	return nil
}

func (mh *MigrationHistory) getMigrationHistorySchema() *model.Entity {
	return model.NewEntity(MigrationHistoryEntityName).
		AddStringPrimaryKey("ID").
		AddField(model.NewField("Description", model.FieldTypeString)).
		AddField(model.NewField("AppliedAt", model.FieldTypeDateTime)).
		AddField(model.NewField("SourceFile", model.FieldTypeString)).
		AddField(model.NewField("Checksum", model.FieldTypeString))
}

// AppliedMigration represents a record of an applied migration.
type AppliedMigration struct {
	ID          string    `json:"id"`
	Description string    `json:"description"`
	AppliedAt   time.Time `json:"applied_at"`
	SourceFile  string    `json:"source_file"`
	Checksum    string    `json:"checksum"`
}

const MigrationHistoryEntityName = "__wormhole_migration_history"

// mapDocumentToStruct maps a model.Document to a Go struct using reflection.
// This is a simplified version and might need more robust error handling and type conversion.
func mapDocumentToStruct(doc model.Document, target interface{}) error {
	val := reflect.ValueOf(target)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("target must be a pointer to a struct")
	}
	elem := val.Elem()
	typ := elem.Type()

	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		structField := typ.Field(i)
		jsonTag := structField.Tag.Get("json")

		if jsonTag == "" {
			jsonTag = structField.Name // Use field name if no json tag
		}

		if docValue, ok := doc.Fields[jsonTag]; ok {
			if docValue == nil {
				continue
			}
			// Basic type conversion; more complex types (e.g., custom time formats) might need specific handling
			switch field.Kind() {
			case reflect.String:
				if v, ok := docValue.(string); ok {
					field.SetString(v)
				}
			case reflect.Struct:
				// Handle time.Time specifically
				if field.Type() == reflect.TypeOf(time.Time{}) {
					if v, ok := docValue.(time.Time); ok {
						field.Set(reflect.ValueOf(v))
					} else if vStr, ok := docValue.(string); ok {
						// Attempt to parse if it's a string, e.g., from some databases
						t, err := time.Parse(time.RFC3339, vStr) // Common format
						if err == nil {
							field.Set(reflect.ValueOf(t))
						} else {
							// Try another common format
							t, err = time.Parse("2006-01-02 15:04:05", vStr)
							if err == nil {
								field.Set(reflect.ValueOf(t))
							}
						}
					}
				}
				// Add more type conversions as needed (int, bool, float, etc.)
			}
		}
	}
	return nil
}

// Synchronizer orchestrates the full data synchronization process.
type Synchronizer struct {
	sourceProvider      provider.Provider
	destinationProvider provider.Provider
	changeTracker       *tracker.ChangeTracker
}

// NewSynchronizer creates a new Synchronizer.
func NewSynchronizer(source, destination provider.Provider, changeTracker *tracker.ChangeTracker) *Synchronizer {
	return &Synchronizer{
		sourceProvider:      source,
		destinationProvider: destination,
		changeTracker:       changeTracker,
	}
}

// Run performs a full data synchronization.
func (s *Synchronizer) Run(ctx context.Context) error {
	// First, verify schema compatibility
	dataMigrator := NewDataMigrator(s.sourceProvider, s.destinationProvider, s.changeTracker)
	if err := dataMigrator.VerifySchema(ctx); err != nil {
		return fmt.Errorf("schema verification failed: %w", err)
	}

	// Get source schema to iterate over entities
	sourceSchema, err := s.sourceProvider.Schema(ctx)
	if err != nil {
		return fmt.Errorf("failed to get source schema: %w", err)
	}

	// Use a wait group to potentially parallelize entity synchronization if safe
	// For now, process sequentially. Consider dependencies for parallel execution.
	var wg sync.WaitGroup
	errChan := make(chan error, len(sourceSchema.Entities))

	for _, entity := range sourceSchema.Entities {
		wg.Add(1)
		go func(e *model.Entity) {
			defer wg.Done()
			if err := dataMigrator.Sync(ctx, e); err != nil {
				errChan <- fmt.Errorf("failed to sync entity %s: %w", e.Name, err)
			}
		}(entity)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return fmt.Errorf("synchronization completed with errors: %w", err)
		}
	}

	fmt.Println("All entities synchronized successfully.")
	return nil
}
