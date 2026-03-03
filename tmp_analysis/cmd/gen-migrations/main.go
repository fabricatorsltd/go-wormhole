package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	"github.com/ristocalldevs/backend/internal/models"
)

func main() {
	// 1. Get all entities
	entities := []any{
		models.User{},
		models.UserAck{},
		models.PromoCode{},
		models.Service{},
		models.StripeRecord{},
		models.Transaction{},
		models.UserSubscription{},
		models.Adv{},
		models.Badge{},
		models.Course{},
		models.Instructor{},
		models.JobOffer{},
		models.LegalEntity{},
		models.POS{},
		models.JobCandidate{},
		models.Chat{},
		models.ChatMessage{},
		models.Notification{},
		models.UserDevice{},
		models.EmailLog{},
		models.NotificationLog{},
		models.SystemLog{},
		models.FeaturesToggle{},
		models.Setting{},
	}

	var metas []*model.EntityMeta
	for _, e := range entities {
		metas = append(metas, schema.Parse(e))
	}

	// 2. Compute diff against empty schema
	ops := migrations.ComputeDiff(metas, migrations.DatabaseSchema{})

	// 3. Generate migration file
	name := "InitialMigration"
	source := migrations.GenerateMigrationFile(name, ops)
	fileName := migrations.MigrationFileName(name)

	dir := "internal/migrations"
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}

	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, []byte(source), 0644); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Successfully generated migration: %s\n", path)
}
