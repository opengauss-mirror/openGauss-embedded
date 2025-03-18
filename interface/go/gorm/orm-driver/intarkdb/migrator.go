/*
* Copyright (c) GBA-NCTI-ISDC. 2022-2024.
* Copyright (c) 2013-NOW  Jinzhu <wosmvp@gmail.com>
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
* migrator.go
*
* IDENTIFICATION
* openGauss-embedded/interface/go/gorm/orm-driver/intarkdb/migrator.go
*
* -------------------------------------------------------------------------
 */

package intarkdb

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
}

type printSQLLogger struct {
	logger.Interface
}

func (m Migrator) CurrentDatabase() (name string) {
	return "intarkdb"
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int
	m.Migrator.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw("SELECT count(*) FROM 'SYS_TABLES' WHERE NAME=?", stmt.Table).Row().Scan(&count)
	})
	return count > 0
}

// AutoMigrate auto migrate values
func (m Migrator) AutoMigrate(values ...interface{}) error {
	for _, value := range m.ReorderModels(values, true) {
		queryTx := m.DB.Session(&gorm.Session{})
		execTx := queryTx
		if m.DB.DryRun {
			queryTx.DryRun = false
			execTx = m.DB.Session(&gorm.Session{Logger: &printSQLLogger{Interface: m.DB.Logger}})
		}
		if !queryTx.Migrator().HasTable(value) {
			if err := execTx.Migrator().CreateTable(value); err != nil {
				return err
			}
		} else {
			if err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
				return errors.New("table already exists")
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

// TODO
// func (m Migrator) HasIndex(value interface{}, name string) bool {
// 	var count int
// 	m.RunWithValue(value, func(stmt *gorm.Statement) error {
// 		if idx := stmt.Schema.LookIndex(name); idx != nil {
// 			name = idx.Name
// 		}

// 		if name != "" {
// 			m.DB.Raw(
// 				"SELECT count(*) FROM 'SYS_INDEXES' WHERE type = ? AND tbl_name = ? AND name = ?", "index", stmt.Table, name,
// 			).Row().Scan(&count)
// 		}
// 		return nil
// 	})
// 	return count > 0
// }
