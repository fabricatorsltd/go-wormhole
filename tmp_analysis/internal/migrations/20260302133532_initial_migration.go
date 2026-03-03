package migrations

import "github.com/fabricatorsltd/go-wormhole/pkg/migrations"

func init() {
	migrations.Register(migrations.Migration{
		ID: "20260302133532_initial_migration",		Up: func(b *migrations.SchemaBuilder) {
			b.CreateTable("user",
				migrations.ColumnDef{Name: "id", PrimaryKey: true},
				migrations.ColumnDef{Name: "external_id"},
				migrations.ColumnDef{Name: "name"},
				migrations.ColumnDef{Name: "surname"},
				migrations.ColumnDef{Name: "email"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "country_code"},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "preferences"},
				migrations.ColumnDef{Name: "address"},
				migrations.ColumnDef{Name: "badges"},
				migrations.ColumnDef{Name: "credit"},
				migrations.ColumnDef{Name: "birthday", SQLType: "date"},
				migrations.ColumnDef{Name: "phone", Nullable: true},
				migrations.ColumnDef{Name: "experience_years"},
				migrations.ColumnDef{Name: "bio", Nullable: true},
				migrations.ColumnDef{Name: "has_haccp"},
				migrations.ColumnDef{Name: "usage_reason", Nullable: true},
				migrations.ColumnDef{Name: "vat_owner"},
				migrations.ColumnDef{Name: "acks"},
			)
			b.CreateIndex("idx_users_external_id", "user", false, "external_id")
			b.CreateIndex("idx_users_email", "user", false, "email")
			b.CreateTable("user_ack",
				migrations.ColumnDef{Name: "id", PrimaryKey: true},
				migrations.ColumnDef{Name: "user_id", PrimaryKey: true},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "status"},
			)
			b.CreateTable("promo_code",
				migrations.ColumnDef{Name: "id", PrimaryKey: true},
				migrations.ColumnDef{Name: "code_type"},
				migrations.ColumnDef{Name: "value"},
				migrations.ColumnDef{Name: "valid_from"},
				migrations.ColumnDef{Name: "valid_until", Nullable: true},
				migrations.ColumnDef{Name: "max_usage_per_user"},
				migrations.ColumnDef{Name: "max_usage"},
				migrations.ColumnDef{Name: "used_times"},
			)
			b.CreateTable("service",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "name"},
				migrations.ColumnDef{Name: "description"},
				migrations.ColumnDef{Name: "price"},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "service_type"},
				migrations.ColumnDef{Name: "promo_price", Nullable: true},
				migrations.ColumnDef{Name: "in_promotion_until", Nullable: true},
			)
			b.CreateTable("stripe_record",
				migrations.ColumnDef{Name: "transaction_id", PrimaryKey: true},
				migrations.ColumnDef{Name: "stripe_id", PrimaryKey: true},
				migrations.ColumnDef{Name: "stripe_customer_id"},
				migrations.ColumnDef{Name: "created_at"},
			)
			b.CreateTable("transaction",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "user_id"},
				migrations.ColumnDef{Name: "value"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "service_id"},
				migrations.ColumnDef{Name: "price", Nullable: true},
				migrations.ColumnDef{Name: "promocode", Nullable: true},
				migrations.ColumnDef{Name: "status"},
			)
			b.CreateIndex("idx_transactions_user", "transaction", false, "user_id")
			b.CreateIndex("idx_transactions_service", "transaction", false, "service_id")
			b.CreateTable("user_subscription",
				migrations.ColumnDef{Name: "id", PrimaryKey: true},
				migrations.ColumnDef{Name: "user_id"},
				migrations.ColumnDef{Name: "service_id"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "renewal_days"},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "stripe_subscription_id"},
			)
			b.CreateIndex("idx_subscriptions_user", "user_subscription", false, "user_id")
			b.CreateIndex("idx_subscriptions_service", "user_subscription", false, "service_id")
			b.CreateTable("adv",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "title"},
				migrations.ColumnDef{Name: "sub_title"},
				migrations.ColumnDef{Name: "description"},
				migrations.ColumnDef{Name: "owner_name"},
				migrations.ColumnDef{Name: "address", Nullable: true},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "cover_url"},
			)
			b.CreateTable("badge",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "name"},
				migrations.ColumnDef{Name: "picture"},
				migrations.ColumnDef{Name: "status"},
			)
			b.CreateTable("course",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "title"},
				migrations.ColumnDef{Name: "description"},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "price"},
				migrations.ColumnDef{Name: "cost"},
				migrations.ColumnDef{Name: "badges"},
				migrations.ColumnDef{Name: "instructors"},
				migrations.ColumnDef{Name: "cover_url", Nullable: true},
				migrations.ColumnDef{Name: "video_url", Nullable: true},
				migrations.ColumnDef{Name: "external_url", Nullable: true},
			)
			b.CreateTable("instructor",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "name"},
				migrations.ColumnDef{Name: "role"},
				migrations.ColumnDef{Name: "description"},
				migrations.ColumnDef{Name: "status"},
			)
			b.CreateTable("job_offer",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "owner_id"},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "start_at"},
				migrations.ColumnDef{Name: "crew_id"},
				migrations.ColumnDef{Name: "role_id"},
				migrations.ColumnDef{Name: "title"},
				migrations.ColumnDef{Name: "description"},
				migrations.ColumnDef{Name: "notes"},
				migrations.ColumnDef{Name: "badges"},
				migrations.ColumnDef{Name: "data"},
				migrations.ColumnDef{Name: "on_top_until", Nullable: true},
				migrations.ColumnDef{Name: "closed_on", Nullable: true},
				migrations.ColumnDef{Name: "closing_reason"},
			)
			b.CreateTable("legal_entity",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "owner_id"},
				migrations.ColumnDef{Name: "name"},
				migrations.ColumnDef{Name: "address"},
				migrations.ColumnDef{Name: "vat"},
				migrations.ColumnDef{Name: "fiscal_code"},
				migrations.ColumnDef{Name: "status"},
			)
			b.CreateTable("pos",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "owner_id", Nullable: true},
				migrations.ColumnDef{Name: "legal_entity_id", Nullable: true},
				migrations.ColumnDef{Name: "name"},
				migrations.ColumnDef{Name: "description"},
				migrations.ColumnDef{Name: "type"},
				migrations.ColumnDef{Name: "cover_link", Nullable: true},
				migrations.ColumnDef{Name: "address"},
				migrations.ColumnDef{Name: "status"},
			)
			b.CreateTable("job_candidate",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "offer_id"},
				migrations.ColumnDef{Name: "user_id"},
				migrations.ColumnDef{Name: "cover_letter"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "status"},
			)
			b.CreateTable("chat",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "participants"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "last_activity"},
			)
			b.CreateTable("chat_message",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "chat_id"},
				migrations.ColumnDef{Name: "sender_id"},
				migrations.ColumnDef{Name: "reactions"},
			)
			b.CreateTable("notification",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "user_id"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "target_type"},
				migrations.ColumnDef{Name: "title"},
				migrations.ColumnDef{Name: "message"},
				migrations.ColumnDef{Name: "payload"},
			)
			b.CreateTable("user_device",
				migrations.ColumnDef{Name: "user_id"},
				migrations.ColumnDef{Name: "platform"},
				migrations.ColumnDef{Name: "token"},
			)
			b.CreateIndex("idx_user_devices_user", "user_device", false, "user_id")
			b.CreateIndex("idx_user_devices_token", "user_device", false, "token")
			b.CreateTable("email_log",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "user_id"},
				migrations.ColumnDef{Name: "address"},
				migrations.ColumnDef{Name: "subject"},
				migrations.ColumnDef{Name: "message"},
			)
			b.CreateTable("notification_log",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "external_id"},
				migrations.ColumnDef{Name: "target_type"},
				migrations.ColumnDef{Name: "target"},
			)
			b.CreateTable("system_log",
				migrations.ColumnDef{Name: "id", PrimaryKey: true, AutoIncr: true},
				migrations.ColumnDef{Name: "status"},
				migrations.ColumnDef{Name: "created_at"},
				migrations.ColumnDef{Name: "user_id"},
				migrations.ColumnDef{Name: "severity"},
				migrations.ColumnDef{Name: "source"},
				migrations.ColumnDef{Name: "message"},
			)
			b.CreateTable("features_toggle",
				migrations.ColumnDef{Name: "id", PrimaryKey: true},
				migrations.ColumnDef{Name: "user_id", PrimaryKey: true},
				migrations.ColumnDef{Name: "status"},
			)
			b.CreateTable("setting",
				migrations.ColumnDef{Name: "id", PrimaryKey: true},
				migrations.ColumnDef{Name: "description"},
				migrations.ColumnDef{Name: "category"},
				migrations.ColumnDef{Name: "value"},
				migrations.ColumnDef{Name: "default_value"},
			)
		},
		Down: func(b *migrations.SchemaBuilder) {
			b.DropTable("user")
			b.DropIndex("idx_users_external_id")
			b.DropIndex("idx_users_email")
			b.DropTable("user_ack")
			b.DropTable("promo_code")
			b.DropTable("service")
			b.DropTable("stripe_record")
			b.DropTable("transaction")
			b.DropIndex("idx_transactions_user")
			b.DropIndex("idx_transactions_service")
			b.DropTable("user_subscription")
			b.DropIndex("idx_subscriptions_user")
			b.DropIndex("idx_subscriptions_service")
			b.DropTable("adv")
			b.DropTable("badge")
			b.DropTable("course")
			b.DropTable("instructor")
			b.DropTable("job_offer")
			b.DropTable("legal_entity")
			b.DropTable("pos")
			b.DropTable("job_candidate")
			b.DropTable("chat")
			b.DropTable("chat_message")
			b.DropTable("notification")
			b.DropTable("user_device")
			b.DropIndex("idx_user_devices_user")
			b.DropIndex("idx_user_devices_token")
			b.DropTable("email_log")
			b.DropTable("notification_log")
			b.DropTable("system_log")
			b.DropTable("features_toggle")
			b.DropTable("setting")
		},
	})
}
