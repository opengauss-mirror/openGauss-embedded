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
* openGauss-embedded/examples/example-go/main.go
*
* -------------------------------------------------------------------------
 */

package main

import (
	"fmt"

	"example-go/intarkdb_interface"
)

func main() {
	var intarkdb = intarkdb_interface.IntarkdbSQL{}

	err := intarkdb.IntarkdbOpen(".")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer intarkdb.IntarkdbClose()

	err = intarkdb.IntarkdbConnect()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer intarkdb.IntarkdbDisconnect()

	err = intarkdb.IntarkdbInitResult()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer intarkdb.IntarkdbDestroyResult()

	intarkdb.IntarkdbQuery("CREATE TABLE if not exists example_go(id int, name varchar(20), money bigint)")
	intarkdb.IntarkdbQuery("INSERT INTO example_go VALUES (1, '张三', 100000000)")
    intarkdb.IntarkdbQuery("INSERT INTO example_go VALUES (2, '李四', 8000000000)")

	err = intarkdb.IntarkdbQuery("SELECT * FROM example_go")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	rowCount := intarkdb.IntarkdbRowCount()
	for row := int64(0); row < rowCount; row++ {
		columnCount := intarkdb.IntarkdbColumnCount()
		if row == 0 {
			fmt.Print(" ")
			for col := int64(0); col < columnCount; col++ {
				colName := intarkdb.IntarkdbColumnName(col)
				fmt.Print(colName, " ")
			}
			fmt.Println()
		}
		fmt.Print(row, "  ")
		for col := int64(0); col < columnCount; col++ {
			colValue := intarkdb.IntarkdbValueVarchar(row, col)
			fmt.Print(colValue, "  ")
		}
		fmt.Println()
	}
	intarkdb.IntarkdbFreeRow()
}
