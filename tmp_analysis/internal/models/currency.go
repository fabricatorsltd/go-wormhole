package models

import (
	"time"
)

type PromoCode struct {
	Id              string        `db:"primary_key; column:id"`
	CodeType        PromoCodeType `db:"column:code_type"`
	Value           int16         `db:"column:value"`
	ValidFrom       time.Time     `db:"column:valid_from"`
	ValidUntil      *time.Time    `db:"column:valid_until; nullable"`
	MaxUsagePerUser int16         `db:"column:max_usage_per_user"`
	MaxUsage        int16         `db:"column:max_usage"`
	UsedTimes       int16         `db:"column:used_times"`
}

type Service struct {
	Id                string      `db:"primary_key; auto_increment; column:id"`
	Name              string      `db:"column:name"`
	Description       string      `db:"column:description"`
	Price             int16       `db:"column:price"`
	Status            int16       `db:"column:status"`
	ServiceType       ServiceType `db:"column:service_type"`
	PromoPrice        *int16      `db:"column:promo_price; nullable"`
	InPromotionUntil  *time.Time  `db:"column:in_promotion_until; nullable"`
}

type StripeRecord struct {
	TransactionId    string    `db:"primary_key; column:transaction_id"`
	StripeId         string    `db:"primary_key; column:stripe_id"`
	StripeCustomerId string    `db:"column:stripe_customer_id"`
	CreatedAt        time.Time `db:"column:created_at"`
}

type Transaction struct {
	Id         string     `db:"primary_key; auto_increment; column:id"`
	UserId     string     `db:"column:user_id; index:idx_transactions_user"`
	Value      int16      `db:"column:value"`
	CreatedAt  time.Time  `db:"column:created_at"`
	ServiceId  string     `db:"column:service_id; index:idx_transactions_service"`
	Price      *float64   `db:"column:price; nullable"`
	Promocode  *string    `db:"column:promocode; nullable"`
	Status     int16      `db:"column:status"`
}

type UserSubscription struct {
	Id                   string    `db:"primary_key; column:id"`
	UserId               string    `db:"column:user_id; index:idx_subscriptions_user"`
	ServiceId            string    `db:"column:service_id; index:idx_subscriptions_service"`
	CreatedAt            time.Time `db:"column:created_at"`
	RenewalDays          int16     `db:"column:renewal_days"`
	Status               int16     `db:"column:status"`
	StripeSubscriptionId string    `db:"column:stripe_subscription_id"`
}
