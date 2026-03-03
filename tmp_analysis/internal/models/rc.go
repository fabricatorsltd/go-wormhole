package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type Adv struct {
	Id          int16    `db:"primary_key; auto_increment; column:id"`
	Title       string   `db:"column:title"`
	SubTitle    string   `db:"column:sub_title"`
	Description string   `db:"column:description"`
	OwnerName   string   `db:"column:owner_name"`
	Address     *Address `db:"column:address; nullable"`
	Status      int16    `db:"column:status"`
	CoverUrl    string   `db:"column:cover_url"`
}

type Badge struct {
	Id      int16  `db:"primary_key; auto_increment; column:id"`
	Name    string `db:"column:name"`
	Picture string `db:"column:picture"`
	Status  int16  `db:"column:status"`
}

type Course struct {
	Id          int16    `db:"primary_key; auto_increment; column:id"`
	Title       string   `db:"column:title"`
	Description string   `db:"column:description"`
	Status      int16    `db:"column:status"`
	Price       float64  `db:"column:price"`
	Cost        float64  `db:"column:cost"`
	Badges      string   `db:"column:badges"`      // JSON or comma-separated
	Instructors string   `db:"column:instructors"` // JSON or comma-separated
	CoverUrl    *string  `db:"column:cover_url; nullable"`
	VideoUrl    *string  `db:"column:video_url; nullable"`
	ExternalUrl *string  `db:"column:external_url; nullable"`
}

type Instructor struct {
	Id          int16  `db:"primary_key; auto_increment; column:id"`
	Name        string `db:"column:name"`
	Role        string `db:"column:role"`
	Description string `db:"column:description"`
	Status      int16  `db:"column:status"`
}

type JobData struct {
	SalaryType     uint16  `json:"salary_type"`
	MinSalary      float64 `json:"min_salary"`
	MaxSalary      float64 `json:"max_salary"`
	MinExperience  string  `json:"min_experience"`
	MaxExperience  string  `json:"max_experience"`
	MinAge         string  `json:"min_age"`
	MaxAge         string  `json:"max_age"`
	FoodAndLodging uint16  `json:"food_and_lodging"`
	Apprenticeship uint16  `json:"apprenticeship"`
	SchoolStage    uint16  `json:"school_stage"`
	ContractType   string  `json:"contract_type"`
	ContractLenght string  `json:"contract_length"`
}

func (j *JobData) Scan(value any) error {
	if value == nil {
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type assertion to []byte failed")
	}
	return json.Unmarshal(b, j)
}

func (j JobData) Value() (driver.Value, error) {
	return json.Marshal(j)
}

type JobOffer struct {
	Id            int64         `db:"primary_key; auto_increment; column:id"`
	OwnerId       int64         `db:"column:owner_id"`
	Status        int16         `db:"column:status"`
	CreatedAt     time.Time     `db:"column:created_at"`
	StartAt       time.Time     `db:"column:start_at"`
	CrewId        int16         `db:"column:crew_id"`
	RoleId        int16         `db:"column:role_id"`
	Title         string        `db:"column:title"`
	Description   string        `db:"column:description"`
	Notes         string        `db:"column:notes"`
	Badges        string        `db:"column:badges"`
	Data          JobData       `db:"column:data"`
	OnTopUntil    *time.Time    `db:"column:on_top_until; nullable"`
	ClosedOn      *time.Time    `db:"column:closed_on; nullable"`
	ClosingReason ClosingReason `db:"column:closing_reason"`
}

type LegalEntity struct {
	Id         int64   `db:"primary_key; auto_increment; column:id"`
	OwnerId    string  `db:"column:owner_id"`
	Name       string  `db:"column:name"`
	Address    Address `db:"column:address"`
	VAT        string  `db:"column:vat"`
	FiscalCode string  `db:"column:fiscal_code"`
	Status     int16   `db:"column:status"`
}

type POS struct {
	Id            int64    `db:"primary_key; auto_increment; column:id"`
	OwnerId       *string  `db:"column:owner_id; nullable"`
	LegalEntityId *int64   `db:"column:legal_entity_id; nullable"`
	Name          string   `db:"column:name"`
	Description   string   `db:"column:description"`
	Type          string   `db:"column:type"`
	CoverLink     *string  `db:"column:cover_link; nullable"`
	Address       Address  `db:"column:address"`
	Status        int16    `db:"column:status"`
}

type JobCandidate struct {
	Id          int32     `db:"primary_key; auto_increment; column:id"`
	OfferId     int64     `db:"column:offer_id"`
	UserId      string    `db:"column:user_id"`
	CoverLetter string    `db:"column:cover_letter"`
	CreatedAt   time.Time `db:"column:created_at"`
	Status      int16     `db:"column:status"`
}
