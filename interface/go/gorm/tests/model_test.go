package tests_test

import (
	"database/sql"
	"time"
)

type Model struct {
	ID        int `gorm:"primarykey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

type User struct {
	ID int `gorm:"primarykey"`
	// gorm.Model
	Name     string
	Age      uint
	Birthday time.Time
	Active   bool
}

type Account struct {
	Model
	UserID sql.NullInt64
	Number string
}

type Pet struct {
	Model
	UserID *uint
	Name   string
}

type Toy struct {
	Model
	Name      string
	OwnerID   string
	OwnerType string
}

type Company struct {
	ID   int
	Name string
}

type Language struct {
	Code string `gorm:"primarykey"`
	Name string
}

type Coupon struct {
	ID               int              `gorm:"primarykey; size:255"`
	AppliesToProduct []*CouponProduct `gorm:"foreignKey:CouponId;constraint:OnDelete:CASCADE"`
	AmountOff        uint32           `gorm:"column:amount_off"`
	PercentOff       float32          `gorm:"column:percent_off"`
}

type CouponProduct struct {
	CouponId  int    `gorm:"primarykey;size:255"`
	ProductId string `gorm:"primarykey;size:255"`
	Desc      string
}

type Order struct {
	Model
	Num      string
	Coupon   *Coupon
	CouponID string
}

type Parent struct {
	Model
	FavChildID uint
	FavChild   *Child
	Children   []*Child
}

type Child struct {
	Model
	Name     string
	ParentID *uint
}
