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
* intarkdb.go
*
* IDENTIFICATION
* openGauss-embedded/interface/go/gorm/orm-driver/intarkdb/intarkdb.go
*
* -------------------------------------------------------------------------
 */

package intarkdb

import (
	"database/sql"
	"strconv"
	"strings"

	_ "go-api/sql-driver/intarkdb"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

const DriverName = "intarkdb"

type Dialector struct {
	DriverName string
	DSN        string
	Conn       gorm.ConnPool
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{DSN: dsn}
}

func (dialector Dialector) Name() string {
	return "intarkdb"
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	if dialector.DriverName == "" {
		dialector.DriverName = DriverName
	}

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		conn, err := sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return err
		}
		db.ConnPool = conn
	}

	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		LastInsertIDReversed: true,
	})

	for k, v := range dialector.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}

	return
}

func (dialector Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"INSERT": func(c clause.Clause, builder clause.Builder) {
			if insert, ok := c.Expression.(clause.Insert); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					stmt.WriteString("INSERT ")
					if insert.Modifier != "" {
						stmt.WriteString(insert.Modifier)
						stmt.WriteByte(' ')
					}

					stmt.WriteString("INTO ")
					if insert.Table.Name == "" {
						stmt.WriteQuoted(stmt.Table)
					} else {
						stmt.WriteQuoted(insert.Table)
					}
					return
				}
			}

			c.Build(builder)
		},
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			if limit, ok := c.Expression.(clause.Limit); ok {
				var lmt = -1
				if limit.Limit != nil && *limit.Limit >= 0 {
					lmt = *limit.Limit
				}
				if lmt >= 0 || limit.Offset > 0 {
					builder.WriteString("LIMIT ")
					builder.WriteString(strconv.Itoa(lmt))
				}
				if limit.Offset > 0 {
					builder.WriteString(" OFFSET ")
					builder.WriteString(strconv.Itoa(limit.Offset))
				}
			}
		},
		// "FOR": func(c clause.Clause, builder clause.Builder) {
		// 	if _, ok := c.Expression.(clause.Locking); ok {
		// 		// instardb does not support row-level locking.
		// 		return
		// 	}
		// 	c.Build(builder)
		// },
	}
}

// 返回字段的默认值表达式
func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	if field.AutoIncrement {
		return clause.Expr{SQL: "NULL"}
	}

	// doesn't work, will raise error
	return clause.Expr{SQL: "DEFAULT"}
}

// 创建Migrator
func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:        db,
		Dialector: dialector,
		// CreateIndexAfterCreateTable: true,
	}}}
}

// 绑定参数
func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

// 对特殊标识符解析成当前数据库的标准
func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	// writer.WriteByte('`')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(".")
			}
			writer.WriteString(str)
			// writer.WriteByte('`')
		}
	} else {
		writer.WriteString(str)
		// writer.WriteByte('`')
	}
}

// 执行 SQL 语句的解释器，返回最终要执行的sql
func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `"`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int:
		var result string
		if field.Size <= 32 {
			result = "integer"
		} else {
			result = "bigint"
		}

		if field.AutoIncrement {
			result = result + " autoincrement"
		}
		return result
	case schema.Uint:
		var result string
		if field.Size <= 32 {
			result = "uint32"
		} else {
			result = "ubigint"
		}

		if field.AutoIncrement {
			result = result + " autoincrement"
		}
		return result
	case schema.Float:
		return "real"
	case schema.String:
		return "varchar"
	case schema.Time:
		return "timestamp"
	case schema.Bytes:
		return "blob"
	}

	return string(field.DataType)
}
