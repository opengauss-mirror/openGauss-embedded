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
* openGauss-embedded/src/interface/go/api/main.go
*
* -------------------------------------------------------------------------
 */

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"api/intarkdb_interface"
)

var path string
var operatorType string

func init() {
	flag.StringVar(&path, "path", ".", "DB path")
	flag.StringVar(&operatorType, "type", "sql", "DB type : sql, kv")
}

func main() {
	flag.Parse()
	fmt.Println("[go] DB path:", path)

	switch operatorType {
	case intarkdb_interface.SQL:
		sql(path)
	case intarkdb_interface.KV:
		kv(path)
	default:
		fmt.Println("operatorType error, operatorType = ", operatorType)
	}
}

func kv(path string) {
	var intarkdb = intarkdb_interface.IntarkdbKV{}
	err := intarkdb.OpenDB(intarkdb_interface.Intarkdb, path)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer intarkdb.CloseDB()

	err = intarkdb.AllocKVHandle()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer intarkdb.FreeKVHandle()

	intarkdb.CreateOrOpenKVTable("SYS_KV")

	fmt.Println("example : set key value")
	fmt.Println("example : get key")
	fmt.Println("example : del key")
	fmt.Println("example : quit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">> ")
		scanner.Scan()
		query := scanner.Text()
		query = strings.TrimLeft(query, " ")
		query = strings.TrimRight(query, " ")

		if query == intarkdb_interface.Quit {
			break
		}
		querys := strings.Split(query, " ")
		if len(querys) > 1 {
			temp := querys[0]
			switch temp {
			case "set":
				if len(querys) == 3 {
					intarkdb.Set(querys[1], querys[2])
				}
			case "get":
				if len(querys) == 2 {
					value, err := intarkdb.Get(querys[1])
					if err == nil && value != "" {
						fmt.Println("get key =", querys[1], ", value =", value)
					}
				}
			case "del":
				if len(querys) == 2 {
					intarkdb.Del(querys[1], 0)
				}
			}
		}
	}
}

func sql(path string) {
	var intarkdb = intarkdb_interface.IntarkdbSQL{}

	err := intarkdb.IntarkdbOpen(path)
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

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(">> ")
		scanner.Scan()
		query := scanner.Text()
		query = strings.TrimLeft(query, " ")
		query = strings.TrimRight(query, " ")

		if query == intarkdb_interface.Quit {
			break
		}

		if !strings.HasSuffix(query, ";") {
			fmt.Println("sql needs ; end")
			continue
		}
		// if !(strings.HasPrefix(query, "CREATE") || strings.HasPrefix(query, "INSERT") || strings.HasPrefix(query, "SELECT") ||
		// 	strings.HasPrefix(query, "DELETE") || strings.HasPrefix(query, "UPDATE")) {
		// 	fmt.Println("sql supports CREATE,INSERT,DELETE,UPDATE,SELECT only")
		// 	continue
		// }

		querys := strings.Split(query, ";")
		for index := range querys {
			if querys[index] == "" {
				continue
			}
			err = intarkdb.IntarkdbQuery(querys[index])
			if err != nil {
				fmt.Println(err.Error())
				break
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
	}
}
