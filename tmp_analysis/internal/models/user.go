package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type Address struct {
	CountryCode string `json:"country_code" db:"column:country_code"`
	Region      string `json:"region" db:"column:region"`
	Province    string `json:"province" db:"column:province"`
	City        string `json:"city" db:"column:city"`
	ZipCode     string `json:"zip_code" db:"column:zip_code"`
	Street      string `json:"street" db:"column:street"`
	Coords      string `json:"coords" db:"column:coords"`
}

func (a *Address) Scan(value any) error {
	if value == nil {
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type assertion to []byte failed")
	}
	return json.Unmarshal(b, a)
}

func (a Address) Value() (driver.Value, error) {
	return json.Marshal(a)
}

type UserPreferences struct {
	ReceiveNotifications bool `json:"receive_notifications" db:"column:receive_notifications"`
}

func (p *UserPreferences) Scan(value any) error {
	if value == nil {
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type assertion to []byte failed")
	}
	return json.Unmarshal(b, p)
}

func (p UserPreferences) Value() (driver.Value, error) {
	return json.Marshal(p)
}

type User struct {
	Id              string          `db:"primary_key; column:id"`
	ExternalId      string          `db:"column:external_id; index:idx_users_external_id"`
	Name            string          `db:"column:name"`
	Surname         string          `db:"column:surname"`
	Email           string          `db:"column:email; index:idx_users_email"`
	CreatedAt       time.Time       `db:"column:created_at"`
	CountryCode     string          `db:"column:country_code"`
	Status          EntityStatus    `db:"column:status"`
	Preferences     UserPreferences `db:"column:preferences"`
	Address         Address         `db:"column:address"`
	Badges          string          `db:"column:badges"` // Stored as comma-separated or JSON string in C#
	Credit          int16           `db:"column:credit"`
	Birthday        time.Time       `db:"column:birthday; type:date"`
	Phone           *string         `db:"column:phone; nullable"`
	ExperienceYears int             `db:"column:experience_years"`
	Bio             *string         `db:"column:bio; nullable"`
	HasHaccp        bool            `db:"column:has_haccp"`
	UsageReason     *int            `db:"column:usage_reason; nullable"`
	VatOwner        bool            `db:"column:vat_owner"`

	// Relations (not automatically handled by go-wormhole for now)
	Acks []UserAck `db:"-"`
}

type UserAck struct {
	Id        string       `db:"primary_key; column:id"`
	UserId    string       `db:"primary_key; column:user_id"`
	CreatedAt time.Time    `db:"column:created_at"`
	Status    EntityStatus `db:"column:status"`
}
