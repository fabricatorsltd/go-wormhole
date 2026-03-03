package migrations

import (
	"context"
	"fmt"

	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/tracker"
)

// DefaultBatchSize is the default number of records to process in a single batch.
const DefaultBatchSize = 1000

// DataMigrator is a placeholder implementation for the CLI "database sync" command.
//
// NOTE: The provider.Provider interface in this repository does not expose schema
// discovery or cursor-based streaming, so full data synchronization cannot be
// implemented generically without additional inputs (entity metas, types, etc.).
type DataMigrator struct {
	sourceProvider      provider.Provider
	destinationProvider provider.Provider
	changeTracker       *tracker.Tracker
	batchSize           int
}

func NewDataMigrator(source, destination provider.Provider, changeTracker *tracker.Tracker) *DataMigrator {
	return &DataMigrator{
		sourceProvider:      source,
		destinationProvider: destination,
		changeTracker:       changeTracker,
		batchSize:           DefaultBatchSize,
	}
}

func (dm *DataMigrator) WithBatchSize(size int) *DataMigrator {
	if size > 0 {
		dm.batchSize = size
	}
	return dm
}

func (dm *DataMigrator) FullSync(ctx context.Context) error {
	_ = ctx
	return fmt.Errorf("migrations.DataMigrator.FullSync: not implemented")
}
