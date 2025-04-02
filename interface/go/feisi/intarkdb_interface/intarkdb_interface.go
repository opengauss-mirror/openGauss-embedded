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
* intarkdb_interface.go
*
* IDENTIFICATION
* openGauss-embedded/interface/go/feisi/intarkdb_interface/intarkdb_interface.go
*
* -------------------------------------------------------------------------
 */

package intarkdb_interface

/*
// 头文件的位置，相对于源文件是当前目录，所以是 .，头文件在多个目录时写多个  #cgo CFLAGS: ...
#cgo CFLAGS: -I./include
// 从哪里加载动态库，位置与文件名，-ladd 加载 libadd.so 文件
// #cgo LDFLAGS: -L${SRCDIR}/lib -lintarkdb -Wl,-rpath=${SRCDIR}/lib
#cgo LDFLAGS: -L${SRCDIR}/../../../../output/release/lib -lintarkdb -Wl,-rpath=${SRCDIR}/../../../../output/release/lib

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "include/intarkdb.h"
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type DBStatus int

const (
	dbError            DBStatus = -1
	dbSuccess          DBStatus = 0
	dbTimeout          DBStatus = 1
	ignoreObjectExists DBStatus = 2
	fullConn           DBStatus = 3
	notExist           DBStatus = 10
)

var dbStatusTag = map[DBStatus]string{
	dbError:            "error",
	dbSuccess:          "success",
	dbTimeout:          "timeout",
	ignoreObjectExists: "ignore object exists",
	fullConn:           "full conn",
	notExist:           "not exist",
}

func StatusMessage(dbStatus DBStatus) string {
	return dbStatusTag[dbStatus]
}

const (
	SQL    string = "sql"
	KV     string = "kv"
	Quit   string = "quit"
	OK     string = "ok"
	Failed string = "failed"
)

type Intarkdb struct {
	db C.intarkdb_database
}

type IntarkdbInterface interface {
	IntarkdbOpen(path string) (err error)
	IntarkdbClose()
}

func (g *Intarkdb) IntarkdbOpen(path string) (err error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_open(cPath, &g.db))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("DB open %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *Intarkdb) IntarkdbClose() {
	C.intarkdb_close(&g.db)
	fmt.Println("DB close")
}

type IntarkdbSQL struct {
	DB         Intarkdb
	connection C.intarkdb_connection
	result     C.intarkdb_result
}

type SQLInterface interface {
	IntarkdbConnect() (err error)
	IntarkdbDisconnect()
	IntarkdbInitResult() (err error)
	IntarkdbQuery(query string) (err error)
	IntarkdbRowCount() uint64
	IntarkdbColumnCount() uint64
	IntarkdbColumnName(col uint8) string
	IntarkdbValueVarchar(row, col uint8) string
	IntarkdbDestroyResult()
}

func (g *IntarkdbSQL) IntarkdbConnect() (err error) {
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_connect(g.DB.db, &g.connection))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("sql connection %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbSQL) IntarkdbDisconnect() {
	C.intarkdb_disconnect(&g.connection)
	fmt.Println("sql connection close")
}

func (g *IntarkdbSQL) IntarkdbInitResult() (err error) {
	g.result = C.intarkdb_init_result()
	if g.result == nil {
		err = fmt.Errorf("intarkdb init result fail")
	}
	return
}

func (g *IntarkdbSQL) IntarkdbQuery(query string) (err error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_query(g.connection, cQuery, g.result))
	if dbStatus != dbSuccess {
		err = fmt.Errorf("intarkdb query err : %s", C.GoString(C.intarkdb_result_msg(g.result)))
	}
	return
}

func (g *IntarkdbSQL) IntarkdbRowCount() int64 {
	return int64(C.intarkdb_row_count(g.result))
}

func (g *IntarkdbSQL) IntarkdbColumnCount() int64 {
	return int64(C.intarkdb_column_count(g.result))
}

func (g *IntarkdbSQL) IntarkdbColumnName(col int64) string {
	return C.GoString(C.intarkdb_column_name(g.result, C.long(col)))
}

func (g *IntarkdbSQL) IntarkdbValueVarchar(row, col int64) string {
	return C.GoString(C.intarkdb_value_varchar(g.result, C.long(row), C.long(col)))
}

func (g *IntarkdbSQL) IntarkdbDestroyResult() {
	C.intarkdb_destroy_result(g.result)
	fmt.Println("result destroy success")
}

type IntarkdbKV struct {
	DB         Intarkdb
	connection C.intarkdb_connection_kv
}

type KVInterface interface {
	Connect() (err error)
	Disconnect()
	OpenTable(name string) (err error)
	Set(key, value string) (err error)
	Get(key string) (value string, err error)
	Del(key string) (err error)
	TransactionBegin() (err error)
	TransactionCommit() (err error)
	TransactionRollback() (err error)
}

func (g *IntarkdbKV) Connect() (err error) {
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_connect_kv(g.DB.db, &g.connection))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("kv connection %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbKV) Disconnect() {
	C.intarkdb_disconnect_kv(&g.connection)
	fmt.Println("kv connection close")
}

// 默认系统表，可通过OpenTable创建新的表
func (g *IntarkdbKV) OpenTable(name string) (err error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_open_table_kv(g.connection, cName))
	if dbStatus != dbSuccess {
		err = fmt.Errorf("open kv table %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbKV) Set(key, value string) (err error) {
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))

	ptr := (*C.KvReply)(C.intarkdb_set(g.connection, cKey, cValue))

	dbStatus := dbError
	dbStatus = (DBStatus)(ptr._type)
	if dbStatus == dbSuccess {
		fmt.Println("set key success")
	} else {
		err = fmt.Errorf("set key %s %s", key, dbStatusTag[dbStatus])
	}

	return
}

func (g *IntarkdbKV) Get(key string) (value string, err error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	ptr := (*C.KvReply)(C.intarkdb_get(g.connection, cKey))

	value = C.GoString(ptr.str)
	dbStatus := dbError
	dbStatus = (DBStatus)(ptr._type)
	if dbStatus == dbSuccess && value != "" {
		fmt.Println("get key success")
	} else {
		if value == "" {
			dbStatus = notExist
		}
		err = fmt.Errorf("get key %s %s", key, dbStatusTag[dbStatus])
	}

	return
}

func (g *IntarkdbKV) Del(key string) (err error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	ptr := (*C.KvReply)(C.intarkdb_del(g.connection, cKey))

	dbStatus := dbError
	dbStatus = (DBStatus)(ptr._type)
	if dbStatus == dbSuccess {
		fmt.Println("del key success")
	} else {
		err = fmt.Errorf("del key %s %s", key, dbStatusTag[dbStatus])
	}

	return
}

func (g *IntarkdbKV) TransactionBegin() (err error){
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_begin(g.connection))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("kv TransactionBegin %s", dbStatusTag[dbStatus])
	}

	return
}

func (g *IntarkdbKV) TransactionCommit() (err error){
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_commit(g.connection))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("kv TransactionCommit %s", dbStatusTag[dbStatus])
	}

	return
}

func (g *IntarkdbKV) TransactionRollback() (err error){
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_rollback(g.connection))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("kv TransactionRollback %s", dbStatusTag[dbStatus])
	}

	return
}
