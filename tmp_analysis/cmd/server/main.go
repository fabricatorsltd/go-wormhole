package main

import (
	"context"
	"database/sql"
	"log"
	"log/slog"

	_ "github.com/glebarez/sqlite"
	wh "github.com/fabricatorsltd/go-wormhole/pkg/context"
	wormholesql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
	"github.com/mirkobrombin/go-module-router/v2/pkg/logger"
	"github.com/mirkobrombin/go-module-router/v2/pkg/router"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/ristocalldevs/backend/internal/models"
	_ "github.com/ristocalldevs/backend/internal/migrations"
	"github.com/ristocalldevs/backend/internal/modules/auth"
	"github.com/ristocalldevs/backend/internal/modules/rc"
	fb "github.com/ristocalldevs/backend/internal/pkg/firebase"
	"github.com/ristocalldevs/backend/internal/pkg/middleware"
)

func main() {
	ctx := context.Background()

	// 1. Initialize Firebase
	firebase, err := fb.NewClient(ctx, "googleServiceAccountKey.json")
	if err != nil {
		log.Fatalf("failed to initialize firebase: %v", err)
	}

	// 2. Initialize go-wormhole
	models.RegisterAll()

	db, err := sql.Open("sqlite", "ristocall.db")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	wormholesql.RegisterDefault(db)

	// 3. Run migrations
	if err := migrations.EnsureHistoryTable(ctx, db); err != nil {
		log.Fatalf("failed to ensure history table: %v", err)
	}
	runner := migrations.NewRunner(db)
	if err := runner.Up(ctx); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}

	// 2. Initialize router
	r := router.New()
	r.SetLogger(logger.NewSlog(slog.Default()))

	// 4. Initialize Services
	whCtx := wh.NewDefault()
	defer whCtx.Close()
	authService := auth.NewAuthService(whCtx, firebase)
	rcService := rc.NewRCService(whCtx)

	// 5. Register Dependencies (matching field names in endpoints)
	r.Provide("AuthService", authService)
	r.Provide("RCService", rcService)

	// 6. Register Endpoints
	r.Register(&auth.RegisterEndpoint{})
	r.Register(&auth.EmailAvailabilityEndpoint{})
	r.Register(&auth.SetSessionCookieEndpoint{})

	r.Register(&rc.AvailableJobsEndpoint{})

	// Authenticated endpoints
	authGroup := r.HTTP.Group("")
	authGroup.Use(middleware.FirebaseAuth(firebase))
	authGroup.Register(&auth.ChangePasswordEndpoint{})
	authGroup.Register(&auth.LogoutEndpoint{})
	authGroup.Register(&rc.CreateJobOfferEndpoint{})
	authGroup.Register(&rc.GetJobOfferDetailsEndpoint{})
	authGroup.Register(&rc.GetMyApplicationsEndpoint{})
	authGroup.Register(&rc.GetMyJobOffersEndpoint{})
	authGroup.Register(&rc.ApplyToJobEndpoint{})
	authGroup.Register(&rc.GetLegalEntityEndpoint{})
	authGroup.Register(&rc.CreateLegalEntityEndpoint{})
	authGroup.Register(&rc.GetMyLegalEntitiesEndpoint{})
	authGroup.Register(&rc.GetPOSEndpoint{})
	authGroup.Register(&rc.CreatePOSEndpoint{})
	authGroup.Register(&rc.GetMyPOSesEndpoint{})
	authGroup.Register(&rc.GetAvailableCoursesEndpoint{})
	authGroup.Register(&rc.GetCourseDetailsEndpoint{})
	authGroup.Register(&rc.GetMeEndpoint{})
	authGroup.Register(&rc.UpdateMeEndpoint{})

	// 7. Start server
	slog.Info("🚀 Ristocall Backend (Go) listening on :8080")
	if err := r.Listen(":8080"); err != nil {
		slog.Error("server terminated", "err", err)
	}
}
