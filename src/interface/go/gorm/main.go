/*
* Copyright (c) GBA-NCTI-ISDC. 2022-2024.
*
* openGauss embedded is licensed under Mulan PSL v2.
* You can use this software according to the terms and conditions of the Mulan PSL v2.
* You may obtain a copy of Mulan PSL v2 at:
*
* http://license.coscl.org.cn/MulanPSL2
*
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
* See the Mulan PSL v2 for more details.
* -------------------------------------------------------------------------
*
* main.go
*
* IDENTIFICATION
* openGauss-embedded/src/interface/go/gorm/main.go
*
* -------------------------------------------------------------------------
 */

package main

import (
	"fmt"
	"go-api/orm-driver/intarkdb"
	"log"
	"os"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Left struct {
	RightCode int32
	Name      string
}

type Right struct {
	Code    int32
	Content string
}

type LeftRight struct {
	Left
	Right
}

func runSqlite() {
	db, _ := gorm.Open(sqlite.Open("mydatabase.db"), &gorm.Config{})

	setLog(db)

	initTableAndData(db)

	joinTable(db)

	limitOffset(db)

	orderBy(db)

	groupByAndFunc(db)

	operator(db)
}

func runIntarkDB() {
	db, err := gorm.Open(intarkdb.Open("."))
	if err != nil {
		panic("failed to connect database")
	}

	setLog(db)

	initTableAndData(db)

	joinTable(db)

	limitOffset(db)

	orderBy(db)

	groupByAndFunc(db)

	operator(db)

	tagAndType(db)

	compoundIndex(db)
}

func setLog(db *gorm.DB) {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			LogLevel: logger.Info, // 设置日志级别为 Info，打印 SQL 语句
		},
	)
	db.Logger = newLogger
}

func initTableAndData(db *gorm.DB) {
	db.Migrator().DropTable("lefts")
	db.Migrator().DropTable("rights")

	db.AutoMigrate(&Left{})
	db.AutoMigrate(&Right{})

	left := Left{RightCode: 1, Name: "l1"}
	right := Right{Code: 1, Content: "r1"}

	db.Create(&left)

	left.RightCode = 2
	db.Create(&left)
	left.RightCode = 3
	db.Create(&left)

	left.RightCode = 4
	left.Name = "l2"
	db.Create(&left)

	left.RightCode = 5
	db.Create(&left)
	left.RightCode = 6
	db.Create(&left)

	left.RightCode = 7
	left.Name = "l3"
	db.Create(&left)

	left.RightCode = 8
	db.Create(&left)
	left.RightCode = 9
	db.Create(&left)

	db.Create(&right)
	right.Code = 2
	right.Content = "r2"
	db.Create(&right)
}

// 连表（inner join、left join）
// count计数
func joinTable(db *gorm.DB) {
	var count int64
	var leftRights []LeftRight
	// left join
	db.Table("lefts").Select("*").Joins("LEFT JOIN rights ON lefts.right_code = rights.code").Find(&leftRights).Count(&count)
	fmt.Printf("count = %d, LeftRight len = %d\n", count, len(leftRights))
	for _, v := range leftRights {
		fmt.Println(v)
	}

	db.Table("lefts").Select("*").Joins("LEFT JOIN rights ON lefts.right_code = ?", 1).Find(&leftRights).Count(&count)
	fmt.Printf("count = %d, LeftRight len = %d\n", count, len(leftRights))
	for _, v := range leftRights {
		fmt.Println(v)
	}

	db.Table("lefts").Select("*").Joins("LEFT JOIN rights ON lefts.name = ?", "l1").Find(&leftRights).Count(&count)
	fmt.Printf("count = %d, LeftRight len = %d\n", count, len(leftRights))
	for _, v := range leftRights {
		fmt.Println(v)
	}

	// inner join
	db.Model(&Left{}).Select("*").InnerJoins("INNER JOIN rights ON lefts.right_code = rights.code").Find(&leftRights).Count(&count)
	fmt.Printf("count = %d, LeftRight len = %d\n", count, len(leftRights))
	for _, v := range leftRights {
		fmt.Println(v)
	}

	db.Model(&Left{}).Select("*").InnerJoins("INNER JOIN rights ON rights.code = ?", 1).Find(&leftRights).Count(&count)
	fmt.Printf("count = %d, LeftRight len = %d\n", count, len(leftRights))
	for _, v := range leftRights {
		fmt.Println(v)
	}

	db.Model(&Left{}).Select("*").InnerJoins("INNER JOIN rights ON rights.content = ?", "r1").Find(&leftRights).Count(&count)
	fmt.Printf("count = %d, LeftRight len = %d\n", count, len(leftRights))
	for _, v := range leftRights {
		fmt.Println(v)
	}
}

// 分页（limit、offset）
func limitOffset(db *gorm.DB) {
	var lefts []Left
	db.Table("lefts").Select("*").Limit(2).Offset(1).Find(&lefts)
	for _, v := range lefts {
		fmt.Println(v)
	}
}

// 排序（order by）
func orderBy(db *gorm.DB) {
	var lefts []Left
	db.Table("lefts").Select("*").Order("right_code desc").Order("name").Find(&lefts)
	for _, v := range lefts {
		fmt.Println(v)
	}

	db.Table("lefts").Select("*").Order("right_code asc").Order("name").Find(&lefts)
	for _, v := range lefts {
		fmt.Println(v)
	}
}

type Result struct {
	Name    string
	Total   int64
	Maximum int64
	Minimum int64
	Average float64
}

// 分组（group by）
// 内置函数：sum、avg、max、min
func groupByAndFunc(db *gorm.DB) {
	var results []Result
	db.Table("lefts").Select("name, sum(right_code) as total, max(right_code) as maximum, min(right_code) as minimum, avg(right_code) as average").Group("name").Find(&results)
	for _, v := range results {
		fmt.Printf("name = %s, sum = %d, max = %d, min = %d, avg = %f\n", v.Name, v.Total, v.Maximum, v.Minimum, v.Average)
	}
}

// 运算符：=、<>、>、<、>=、<=、in、not in、and、or、( )
func operator(db *gorm.DB) {
	var lefts []Left
	rightCodes := []uint{1, 4, 7}
	names := []string{"l2", "l3"}
	db.Table("lefts").Select("*").Where("right_code in (?) and name not in (?)", rightCodes, names).Find(&lefts)
	for _, v := range lefts {
		fmt.Println(v)
	}

	db.Table("lefts").Select("*").Where("(right_code > ? and right_code < ?) or (right_code >= ? and right_code <= ?)", 1, 3, 4, 6).Find(&lefts)
	for _, v := range lefts {
		fmt.Println(v)
	}

	db.Table("lefts").Select("*").Where("right_code <> ? and name = ? ", 1, "l1").Find(&lefts)
	for _, v := range lefts {
		fmt.Println(v)
	}
}

type TagAndType struct {
	ID              int32  `gorm:"primaryKey;autoIncrement"`
	BigIntField     int64  `gorm:"column:big_int_field;default:100;unique"`
	UintField       uint32 `gorm:"column:uint_field"`
	BigUintField    uint64 `gorm:"column:big_uint_field"`
	CharField       string `gorm:"column:char_field;type:char(20);not null"`
	VarcharField    string `gorm:"column:varchar_field;type:varchar(255);default:null"`
	TextField       string `gorm:"column:text_field;type:text"`
	CreateTimestamp time.Time
	Gender          bool
	Price           float64
}

func tagAndType(db *gorm.DB) {
	db.Migrator().DropTable("tag_and_types")
	db.AutoMigrate(&TagAndType{})
	data := TagAndType{BigIntField: -9223372036854775808,
		CharField: "test1", Price: 10.22, Gender: true, CreateTimestamp: time.Now().Local()}
	db.Create(&data)

	where := "id = ?"
	// 查询记录
	var result TagAndType
	tx := db.First(&result, where, 1)
	if tx.Error != nil {
		panic("Failed to retrieve record: " + tx.Error.Error())
	}
	fmt.Printf("Retrieved : %+v\n", result)

	// 更新记录
	tx = db.Model(&TagAndType{}).Where(where, 1).Update("Price", 280.22)
	if tx.Error != nil {
		panic("Failed to update record: " + tx.Error.Error())
	}

	data.VarcharField = "default:null"
	data.BigIntField = -2000000000
	db.Create(&data)
	data.TextField = "text"
	data.BigIntField = -3000000000
	db.Create(&data)

	// 删除记录
	tx = db.Delete(&TagAndType{}, where, 3)
	if tx.Error != nil {
		panic("Failed to delete record: " + tx.Error.Error())
	}

	// 查询全部数据
	var results []TagAndType
	tx = db.Model(&TagAndType{}).Select("*").Find(&results)
	if tx.Error != nil {
		panic("Failed to select * record: " + tx.Error.Error())
	}
	for i, v := range results {
		fmt.Printf("Retrieved %d : %+v\n", i, v)
	}
}

type CompoundIndex struct {
	OneIndex int32
	TwoIndex string
}

func compoundIndex(db *gorm.DB) {
	db.Migrator().DropTable("compound_indices")
	db.AutoMigrate(&CompoundIndex{})
	tx := db.Exec("create index one_two on compound_indices (one_index, two_index);")
	if tx.Error != nil {
		panic("Failed to create compoundIndex: " + tx.Error.Error())
	}
}

func demonstration() {
	db, err := gorm.Open(intarkdb.Open("."))
	if err != nil {
		panic("failed to connect database")
	}

	db.Logger = logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			LogLevel: logger.Info, // 设置日志级别为 Info，打印 SQL 语句
		},
	)

	// db.Migrator().DropTable("compounds")

	// db.AutoMigrate(&Compound{})

	db.Create(&Compound{ID: 3, TwoIndex: "123"})
	db.Create(&Compound{ID: 4, TwoIndex: "1234567890000"})

	var results []Compound
	tx := db.Model(&Compound{}).Select("*").Find(&results)
	if tx.Error != nil {
		panic("Failed to select * record: " + tx.Error.Error())
	}
	for i, v := range results {
		fmt.Printf("Retrieved %d : %+v\n", i, v)
	}
}

type Compound struct {
	ID       int `gorm:"primaryKey"`
	TwoIndex string
}

func main() {
	// runSqlite()

	runIntarkDB()
	// demonstration()
}
