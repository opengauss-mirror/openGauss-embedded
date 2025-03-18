package tests_test

import "testing"

func TestGroupBy(t *testing.T) {
	users := []User{
		*GetUser("groupby", Config{}),
		*GetUser("groupby", Config{}),
		*GetUser("groupby", Config{}),
		*GetUser("groupby1", Config{}),
		*GetUser("groupby1", Config{}),
		*GetUser("groupby1", Config{}),
	}
	users[0].Age = 10
	users[0].Active = true
	users[1].Age = 20
	users[2].Age = 30
	users[2].Active = true

	users[3].Age = 110
	users[4].Age = 220
	users[4].Active = true
	users[5].Age = 330
	users[5].Active = true

	if err := DB.Create(&users).Error; err != nil {
		t.Errorf("errors happened when create: %v", err)
	}

	var name string
	var total int
	if err := DB.Model(&User{}).Select("name, sum(age)").Where("name = ?", "groupby").Group("name").Row().Scan(&name, &total); err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if name != "groupby" || total != 60 {
		t.Errorf("name should be groupby, but got %v, total should be 60, but got %v", name, total)
	}

	if err := DB.Model(&User{}).Select("name, sum(age)").Where("name = ?", "groupby").Group("users.name").Row().Scan(&name, &total); err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if name != "groupby" || total != 60 {
		t.Errorf("name should be groupby, but got %v, total should be 60, but got %v", name, total)
	}

	if err := DB.Model(&User{}).Select("name, sum(age) as total").Where("name LIKE ?", "groupby%").Group("name").Having("name = ?", "groupby1").Row().Scan(&name, &total); err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if name != "groupby1" || total != 660 {
		t.Errorf("name should be groupby, but got %v, total should be 660, but got %v", name, total)
	}

	result := struct {
		Name  string
		Total int64
	}{}

	if err := DB.Model(&User{}).Select("name, sum(age) as total").Where("name LIKE ?", "groupby%").Group("name").Having("name = ?", "groupby1").Find(&result).Error; err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if result.Name != "groupby1" || result.Total != 660 {
		t.Errorf("name should be groupby, total should be 660, but got %+v", result)
	}

	if err := DB.Model(&User{}).Select("name, sum(age) as total").Where("name LIKE ?", "groupby%").Group("name").Having("name = ?", "groupby1").Scan(&result).Error; err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if result.Name != "groupby1" || result.Total != 660 {
		t.Errorf("name should be groupby, total should be 660, but got %+v", result)
	}

	var active bool
	if err := DB.Model(&User{}).Select("name, active, sum(age)").Where("name = ? and active = ?", "groupby", true).Group("name").Group("active").Row().Scan(&name, &active, &total); err != nil {
		t.Errorf("no error should happen, but got %v", err)
	}

	if name != "groupby" || active != true || total != 40 {
		t.Errorf("group by two columns, name %v, age %v, active: %v", name, total, active)
	}
}
