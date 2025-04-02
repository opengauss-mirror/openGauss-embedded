package tests_test

import (
	"errors"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestCreate(t *testing.T) {
	user := *GetUser("create", Config{})

	if results := DB.Create(&user); results.Error != nil {
		t.Fatalf("errors happened when create: %v", results.Error)
	} else if results.RowsAffected != 1 {
		t.Fatalf("rows affected expects: %v, got %v", 1, results.RowsAffected)
	}

	if user.ID == 0 {
		t.Errorf("user's primary key should has value after create, got : %v", user.ID)
	}

	var newUser User
	if err := DB.Where("id = ?", user.ID).First(&newUser).Error; err != nil {
		t.Fatalf("errors happened when query: %v", err)
	} else {
		CheckUser(t, newUser, user)
	}
}

func TestCreateInBatches(t *testing.T) {
	users := []User{
		*GetUser("create_in_batches_1", Config{}),
		*GetUser("create_in_batches_2", Config{}),
		*GetUser("create_in_batches_3", Config{}),
		*GetUser("create_in_batches_4", Config{}),
		*GetUser("create_in_batches_5", Config{}),
		*GetUser("create_in_batches_6", Config{}),
	}

	result := DB.CreateInBatches(&users, 2)
	if result.RowsAffected != int64(len(users)) {
		t.Errorf("affected rows should be %v, but got %v", len(users), result.RowsAffected)
	}

	for _, user := range users {
		if user.ID == 0 {
			t.Fatalf("failed to fill user's ID, got %v", user.ID)
		} else {
			var newUser User
			if err := DB.Where("id = ?", user.ID).Preload(clause.Associations).First(&newUser).Error; err != nil {
				t.Fatalf("errors happened when query: %v", err)
			} else {
				CheckUser(t, newUser, user)
			}
		}
	}
}

func TestCreateInBatchesWithDefaultSize(t *testing.T) {
	users := []User{
		*GetUser("create_with_default_batch_size_1", Config{}),
		*GetUser("create_with_default_batch_sizs_2", Config{}),
		*GetUser("create_with_default_batch_sizs_3", Config{}),
		*GetUser("create_with_default_batch_sizs_4", Config{}),
		*GetUser("create_with_default_batch_sizs_5", Config{}),
		*GetUser("create_with_default_batch_sizs_6", Config{}),
	}

	result := DB.Session(&gorm.Session{CreateBatchSize: 2}).Create(&users)
	if result.RowsAffected != int64(len(users)) {
		t.Errorf("affected rows should be %v, but got %v", len(users), result.RowsAffected)
	}

	for _, user := range users {
		if user.ID == 0 {
			t.Fatalf("failed to fill user's ID, got %v", user.ID)
		} else {
			var newUser User
			if err := DB.Where("id = ?", user.ID).Preload(clause.Associations).First(&newUser).Error; err != nil {
				t.Fatalf("errors happened when query: %v", err)
			} else {
				CheckUser(t, newUser, user)
			}
		}
	}
}

func TestCreateFromMap(t *testing.T) {
	if err := DB.Model(&User{}).Create(map[string]interface{}{"Name": "create_from_map", "Age": 18}).Error; err != nil {
		t.Fatalf("failed to create data from map, got error: %v", err)
	}

	var result User
	if err := DB.Where("name = ?", "create_from_map").First(&result).Error; err != nil || result.Age != 18 {
		t.Fatalf("failed to create from map, got error %v", err)
	}

	if err := DB.Model(&User{}).Create(map[string]interface{}{"name": "create_from_map_1", "age": 18}).Error; err != nil {
		t.Fatalf("failed to create data from map, got error: %v", err)
	}

	var result1 User
	if err := DB.Where("name = ?", "create_from_map_1").First(&result1).Error; err != nil || result1.Age != 18 {
		t.Fatalf("failed to create from map, got error %v", err)
	}

	datas := []map[string]interface{}{
		{"Name": "create_from_map_2", "Age": 19},
		{"name": "create_from_map_3", "Age": 20},
	}

	if err := DB.Model(&User{}).Create(&datas).Error; err != nil {
		t.Fatalf("failed to create data from slice of map, got error: %v", err)
	}

	var result2 User
	if err := DB.Where("name = ?", "create_from_map_2").First(&result2).Error; err != nil || result2.Age != 19 {
		t.Fatalf("failed to query data after create from slice of map, got error %v", err)
	}

	var result3 User
	if err := DB.Where("name = ?", "create_from_map_3").First(&result3).Error; err != nil || result3.Age != 20 {
		t.Fatalf("failed to query data after create from slice of map, got error %v", err)
	}

	syncID()
}

// not support
// 1. uint64 Cannot create index on column with datatype uint64
// 2. INSERT INTO empty_structs DEFAULT VALUES; DEFAULT VALUES clause is not supported
// func TestCreateEmptyStruct(t *testing.T) {
// 	type EmptyStruct struct {
// 		ID uint
// 	}
// 	DB.Migrator().DropTable(&EmptyStruct{})

// 	if err := DB.AutoMigrate(&EmptyStruct{}); err != nil {
// 		t.Errorf("no error should happen when auto migrate, but got %v", err)
// 	}

// 	if err := DB.Create(&EmptyStruct{}).Error; err != nil {
// 		t.Errorf("No error should happen when creating user, but got %v", err)
// 	}
// }

func TestCreateEmptySlice(t *testing.T) {
	data := []User{}
	if err := DB.Create(&data).Error; err != gorm.ErrEmptySlice {
		t.Errorf("no data should be created, got %v", err)
	}

	sliceMap := []map[string]interface{}{}
	if err := DB.Model(&User{}).Create(&sliceMap).Error; err != gorm.ErrEmptySlice {
		t.Errorf("no data should be created, got %v", err)
	}
}

func TestCreateInvalidSlice(t *testing.T) {
	users := []*User{
		GetUser("invalid_slice_1", Config{}),
		GetUser("invalid_slice_2", Config{}),
		nil,
	}

	if err := DB.Create(&users).Error; !errors.Is(err, gorm.ErrInvalidData) {
		t.Errorf("should returns error invalid data when creating from slice that contains invalid data")
	}
}

func TestCreateWithNoGORMPrimaryKey(t *testing.T) {
	type JoinTable struct {
		UserID   uint32
		FriendID uint32
	}

	DB.Migrator().DropTable(&JoinTable{})
	if err := DB.AutoMigrate(&JoinTable{}); err != nil {
		t.Errorf("no error should happen when auto migrate, but got %v", err)
	}

	jt := JoinTable{UserID: 1, FriendID: 2}
	err := DB.Create(&jt).Error
	if err != nil {
		t.Errorf("No error should happen when create a record without a GORM primary key. But in the database this primary key exists and is the union of 2 or more fields\n But got: %s", err)
	}
}

func TestSelectWithCreate(t *testing.T) {
	user := *GetUser("select_create", Config{})
	DB.Select("ID", "Name", "Age", "Birthday").Create(&user)

	var user2 User
	DB.First(&user2, user.ID)

	CheckUser(t, user2, user)
}

func TestFirstOrCreateNotExistsTable(t *testing.T) {
	company := Company{Name: "first_or_create_if_not_exists_table"}
	if err := DB.Table("not_exists").FirstOrCreate(&company).Error; err == nil {
		t.Errorf("not exists table, but err is nil")
	}
}

func TestFirstOrCreateWithPrimaryKey(t *testing.T) {
	company := Company{ID: 100, Name: "company100_with_primarykey"}
	DB.FirstOrCreate(&company)

	if company.ID != 100 {
		t.Errorf("invalid primary key after creating, got %v", company.ID)
	}

	companies := []Company{
		{ID: 101, Name: "company101_with_primarykey"},
		{ID: 102, Name: "company102_with_primarykey"},
	}
	DB.Create(&companies)

	if companies[0].ID != 101 || companies[1].ID != 102 {
		t.Errorf("invalid primary key after creating, got %v, %v", companies[0].ID, companies[1].ID)
	}
}

func TestCreateNilPointer(t *testing.T) {
	var user *User

	err := DB.Create(user).Error
	if err == nil || err != gorm.ErrInvalidValue {
		t.Fatalf("it is not ErrInvalidValue")
	}
}

func TestFirstOrCreateRowsAffected(t *testing.T) {
	user := User{Name: "TestFirstOrCreateRowsAffected"}

	res := DB.FirstOrCreate(&user, "name = ?", user.Name)
	if res.Error != nil || res.RowsAffected != 1 {
		t.Fatalf("first or create rows affect err:%v rows:%d", res.Error, res.RowsAffected)
	}

	res = DB.FirstOrCreate(&user, "name = ?", user.Name)
	if res.Error != nil || res.RowsAffected != 0 {
		t.Fatalf("first or create rows affect err:%v rows:%d", res.Error, res.RowsAffected)
	}

	syncID()
}

func TestCreateWithAutoIncrementCompositeKey(t *testing.T) {
	type CompositeKeyProduct struct {
		ProductID    int `gorm:"primaryKey;autoIncrement:true;"` // primary key
		LanguageCode int `gorm:"primaryKey;"`                    // primary key
		Code         string
		Name         string
	}

	if err := DB.Migrator().DropTable(&CompositeKeyProduct{}); err != nil {
		t.Fatalf("failed to migrate, got error %v", err)
	}
	if err := DB.AutoMigrate(&CompositeKeyProduct{}); err != nil {
		t.Fatalf("failed to migrate, got error %v", err)
	}

	prod := &CompositeKeyProduct{
		LanguageCode: 56,
		Code:         "Code56",
		Name:         "ProductName56",
	}
	if err := DB.Create(&prod).Error; err != nil {
		t.Fatalf("failed to create, got error %v", err)
	}

	newProd := &CompositeKeyProduct{}
	if err := DB.First(&newProd).Error; err != nil {
		t.Fatalf("errors happened when query: %v", err)
	} else {
		prod.ProductID = newProd.ProductID
		AssertObjEqual(t, newProd, prod, "ProductID", "LanguageCode", "Code", "Name")
	}
}

// not support
// 1. default string 在gorm中默认双引号，目前我们只能单引号
// 2. OnConflict 目前还不支持
// func TestCreateOnConfilctWithDefalutNull(t *testing.T) {
// 	type OnConfilctUser struct {
// 		ID     string
// 		Name   string `gorm:"default:null"`
// 		Email  string
// 		Mobile string `gorm:"default:'133xxxx'"`
// 	}

// 	err := DB.Migrator().DropTable(&OnConfilctUser{})
// 	AssertEqual(t, err, nil)
// 	err = DB.AutoMigrate(&OnConfilctUser{})
// 	AssertEqual(t, err, nil)

// 	u := OnConfilctUser{
// 		ID:     "on-confilct-user-id",
// 		Name:   "on-confilct-user-name",
// 		Email:  "on-confilct-user-email",
// 		Mobile: "on-confilct-user-mobile",
// 	}
// 	err = DB.Create(&u).Error
// 	AssertEqual(t, err, nil)

// 	u.Name = "on-confilct-user-name-2"
// 	u.Email = "on-confilct-user-email-2"
// 	u.Mobile = ""
// 	err = DB.Clauses(clause.OnConflict{UpdateAll: true}).Create(&u).Error
// 	AssertEqual(t, err, nil)

// 	var u2 OnConfilctUser
// 	err = DB.Where("id = ?", u.ID).First(&u2).Error
// 	AssertEqual(t, err, nil)
// 	AssertEqual(t, u2.Name, "on-confilct-user-name-2")
// 	AssertEqual(t, u2.Email, "on-confilct-user-email-2")
// 	AssertEqual(t, u2.Mobile, "133xxxx")
// }
