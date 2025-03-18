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
* openGauss-embedded/interface/go/apiv2/main.go
*
* -------------------------------------------------------------------------
 */

package main

import (
	"flag"
	"fmt"

	"apiv2/intarkdb_interface"
)

var path string

func init() {
	flag.StringVar(&path, "path", ".", "DB path")
}

func main() {
	flag.Parse()
	fmt.Println("[go] DB path:", path)

	var intarkdb intarkdb_interface.Intarkdb
	err := intarkdb.IntarkdbOpen(path)
	if err != nil {
		fmt.Println(err)
		return
	}

	done := make(chan bool, 2)

	go kv(intarkdb, done)
	go sql(intarkdb, done)

	<-done
	<-done

	intarkdb.IntarkdbClose()
}

func kv(intarkdb intarkdb_interface.Intarkdb, done chan<- bool) {
	defer func() {
		done <- true
	}()

	var intarkdbKV intarkdb_interface.IntarkdbKV
	intarkdbKV.DB = intarkdb
	var err error

	err = intarkdbKV.Connect()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer intarkdbKV.Disconnect()

	// 调用OpenTable可生成指定表，否则默认系统表
	err = intarkdbKV.OpenTable("t_test")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	key1 := "one"
	value1 := "111"

	err = intarkdbKV.Set(key1, value1)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	var str string
	str, err = intarkdbKV.Get(key1)
	if err != nil {
		fmt.Println(err.Error())
		return
	} else {
		fmt.Println("get ", key1, " value ", str)
	}

	err = intarkdbKV.Del(key1)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	intarkdbKV.TransactionBegin()
	intarkdbKV.Set("begin1", "begin1")
	intarkdbKV.Set("begin2", "begin2")
	err = intarkdbKV.TransactionCommit()
	if err != nil {
		fmt.Println("commit err")
		intarkdbKV.TransactionRollback()
	}

	intarkdbKV.TransactionBegin()
	intarkdbKV.Set("begin3", "begin3")
	intarkdbKV.Set("begin4", "begin4")
	intarkdbKV.TransactionRollback()
}

func sql(intarkdb intarkdb_interface.Intarkdb, done chan<- bool) {
	defer func() {
		done <- true
	}()

	var intarkdbSQL intarkdb_interface.IntarkdbSQL
	intarkdbSQL.DB = intarkdb

	err := intarkdbSQL.IntarkdbConnect()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer intarkdbSQL.IntarkdbDisconnect()

	err = intarkdbSQL.IntarkdbInitResult()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer intarkdbSQL.IntarkdbDestroyResult()

	create := "create table sql_test(id integer, name varchar(20));"
	insert := "insert into sql_test values(1,'one'),(2,'two');"
	query := "select * from sql_test;"

	err = intarkdbSQL.IntarkdbQuery(create)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = intarkdbSQL.IntarkdbQuery(insert)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = intarkdbSQL.IntarkdbQuery(query)
	if err != nil {
		fmt.Println(err.Error())
		return
	} else {
		rowCount := intarkdbSQL.IntarkdbRowCount()
		for row := int64(0); row < rowCount; row++ {
			columnCount := intarkdbSQL.IntarkdbColumnCount()
			if row == 0 {
				fmt.Print(" ")
				for col := int64(0); col < columnCount; col++ {
					colName := intarkdbSQL.IntarkdbColumnName(col)
					fmt.Print(colName, " ")
				}
				fmt.Println()
			}
			fmt.Print(row, "  ")
			for col := int64(0); col < columnCount; col++ {
				colValue := intarkdbSQL.IntarkdbValueVarchar(row, col)
				fmt.Print(colValue, "  ")
			}
			fmt.Println()
		}
	}

}
