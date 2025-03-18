package tests_test

import (
	"flag"
	"go-api/orm-driver/intarkdb"
	"log"
	"os"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	DB       *gorm.DB
	dsn      string
	dbName   string
	logLevel int
)

func init() {
	if flag.Parsed() {
		return
	}

	flag.StringVar(&dsn, "dsn", ".", "dsn param")
	flag.StringVar(&dbName, "dbName", "intarkdb", "db name : intarkdb, sqlite")
	flag.IntVar(&logLevel, "logLevel", 4, "logLevel 1:Silent, 2:Error, 3:Warn, 4:Info")
}

func TestMain(m *testing.M) {
	flag.Parse()

	var err error
	DB, err = OpenTestConnection(&gorm.Config{})
	if err != nil {
		log.Printf("failed to connect database, got error %v", err)
		os.Exit(1)
	}

	sqlDB, err := DB.DB()
	if err != nil {
		log.Printf("failed to connect database, got error %v", err)
		os.Exit(1)
	}

	err = sqlDB.Ping()
	if err != nil {
		log.Printf("failed to ping sqlDB, got error %v", err)
		os.Exit(1)
	}

	RunMigrations()

	exitCode := m.Run()
	os.Exit(exitCode)
}

func OpenTestConnection(cfg *gorm.Config) (db *gorm.DB, err error) {
	switch dbName {
	case "intarkdb":
		log.Println("testing intarkdb...")
		db, err = gorm.Open(intarkdb.Open("../."), cfg)
	case "sqlite":
		log.Println("testing sqlite...")
		db, err = gorm.Open(sqlite.Open("../mydatabase.db"), cfg)
	default:
		log.Printf("Unsupported database %s", dbName)
		os.Exit(1)
	}

	if err != nil {
		log.Printf("failed to open %s, got error %v", dbName, err)
		return
	}

	switch logLevel {
	case int(logger.Silent):
		db.Logger = db.Logger.LogMode(logger.Silent)
	case int(logger.Error):
		db.Logger = db.Logger.LogMode(logger.Error)
	case int(logger.Warn):
		db.Logger = db.Logger.LogMode(logger.Warn)
	case int(logger.Info):
		db.Logger = db.Logger.LogMode(logger.Info)
	}

	return
}

func RunMigrations() {
	var err error
	allModels := []interface{}{&User{}, &Company{}}
	// rand.Seed(time.Now().UnixNano())
	// rand.Shuffle(len(allModels), func(i, j int) { allModels[i], allModels[j] = allModels[j], allModels[i] })

	// DB.Migrator().DropTable("user_friends", "user_speaks")

	if err = DB.Migrator().DropTable(allModels...); err != nil {
		log.Printf("Failed to drop table, got error %v\n", err)
		os.Exit(1)
	}

	if err = DB.AutoMigrate(allModels...); err != nil {
		log.Printf("Failed to auto migrate, but got error %v\n", err)
		os.Exit(1)
	}

	for _, m := range allModels {
		if !DB.Migrator().HasTable(m) {
			log.Printf("Failed to create table for %#v\n", m)
			os.Exit(1)
		}
	}
}
