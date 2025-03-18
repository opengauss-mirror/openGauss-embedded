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
* openGauss-embedded/src/interface/go/api/intarkdb_interface/intarkdb_interface.go
*
* -------------------------------------------------------------------------
 */

package intarkdb_interface

/*
// 头文件的位置，相对于源文件是当前目录，所以是 .，头文件在多个目录时写多个  #cgo CFLAGS: ...
#cgo CFLAGS: -I./include
// 从哪里加载动态库，位置与文件名，-ladd 加载 libadd.so 文件
// #cgo LDFLAGS: -L${SRCDIR}/lib -lintarkdb -Wl,-rpath=${SRCDIR}/lib
#cgo LDFLAGS: -L${SRCDIR}/../../../../../output/release/lib -lintarkdb -Wl,-rpath=${SRCDIR}/../../../../../output/release/lib

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
)

var dbStatusTag = map[DBStatus]string{
	dbError:            "error",
	dbSuccess:          "success",
	dbTimeout:          "timeout",
	ignoreObjectExists: "ignore object exists",
	fullConn:           "full conn",
}

func StatusMessage(dbStatus DBStatus) string {
	return dbStatusTag[dbStatus]
}

type DBType int

const (
	Intarkdb DBType = 0
	CEIL     DBType = 1
)

const (
	SQL    string = "sql"
	KV     string = "kv"
	Quit   string = "quit"
	OK     string = "ok"
	Failed string = "failed"
)

type IntarkdbSQL struct {
	db         C.intarkdb_database
	connection C.intarkdb_connection
	result     C.intarkdb_result
}

type SQLInterface interface {
	IntarkdbOpen(path string) (err error)
	IntarkdbClose()
	IntarkdbConnect() (err error)
	IntarkdbDisconnect()
	IntarkdbInitResult() (err error)
	IntarkdbQuery(query string) (err error)
	IntarkdbRowCount() uint64
	IntarkdbColumnCount() uint64
	IntarkdbColumnName(col uint8) string
	IntarkdbValueVarchar(row, col uint8) string
	IntarkdbFreeRow()
	IntarkdbDestroyResult()
}

func (g *IntarkdbSQL) IntarkdbOpen(path string) (err error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_open(cPath, &g.db))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("DB open %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbSQL) IntarkdbClose() {
	C.intarkdb_close(&g.db)
	fmt.Println("DB close")
}

func (g *IntarkdbSQL) IntarkdbConnect() (err error) {
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_connect(g.db, &g.connection))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("connection %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbSQL) IntarkdbDisconnect() {
	C.intarkdb_disconnect(&g.connection)
	fmt.Println("connection close")
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
		err = fmt.Errorf("intarkdb query %s", dbStatusTag[dbStatus])
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

func (g *IntarkdbSQL) IntarkdbFreeRow() {
	C.intarkdb_free_row(g.result)
	fmt.Println("result free success")
}

func (g *IntarkdbSQL) IntarkdbDestroyResult() {
	C.intarkdb_destroy_result(g.result)
	fmt.Println("result destroy success")
}

type IntarkdbKV struct {
	handle unsafe.Pointer
}

type KVInterface interface {
	OpenDB(dbtype DBType, path string) (err error)
	CloseDB()
	AllocKVHandle() (err error)
	FreeKVHandle()
	CreateOrOpenKVTable(name string) (err error)
	Free(reply *C.intarkdbReply)
	Set(key, value string) (err error)
	Get(key string) (value string, err error)
	Del(key string, prefix int32) (err error)
	TransactionBegin() (err error)
	TransactionCommit() (err error)
	TransactionRollback() (err error)
}

func (g *IntarkdbKV) OpenDB(dbtype DBType, path string) (err error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_startup_db((C.int)(dbtype), cPath))
	if dbStatus != dbSuccess {
		err = fmt.Errorf("open db %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbKV) CloseDB() {
	C.intarkdb_shutdown_db()
	fmt.Println("close db success!")
}

func (g *IntarkdbKV) AllocKVHandle() (err error) {
	dbStatus := dbError
	dbStatus = (DBStatus)(C.alloc_kv_handle(&(g.handle)))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("alloc kv handle %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbKV) FreeKVHandle() {
	C.free_kv_handle(g.handle)
	fmt.Println("alloc kv handle free success")
}

func (g *IntarkdbKV) CreateOrOpenKVTable(name string) (err error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	dbStatus := dbError
	dbStatus = (DBStatus)(C.create_or_open_kv_table(g.handle, cName))
	if dbStatus != dbSuccess {
		err = fmt.Errorf("create or open kv table %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbKV) Set(key, value string) (err error) {
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	g.TransactionBegin()
	ptr := (*C.intarkdbReply)(C.intarkdb_command_set(g.handle, cKey, cValue))
	defer g.free(ptr)

	result := C.GoString(ptr.str)
	dbStatus := dbError
	dbStatus = (DBStatus)(ptr._type)
	if dbStatus == dbSuccess {
		if result == OK {
			fmt.Println("set key success")
			g.TransactionCommit()
		} else {
			dbStatus = dbError
		}
	}

	if dbStatus != dbSuccess {
		err = fmt.Errorf("set key %s", dbStatusTag[dbStatus])

	}
	return
}

func (g *IntarkdbKV) Get(key string) (value string, err error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	g.TransactionBegin()
	ptr := (*C.intarkdbReply)(C.intarkdb_command_get(g.handle, cKey))
	defer g.free(ptr)

	value = C.GoString(ptr.str)
	dbStatus := dbError
	dbStatus = (DBStatus)(ptr._type)
	if dbStatus == dbSuccess {
		if value != "" {
			fmt.Println("get key success")
			g.TransactionCommit()
		} else {
			dbStatus = dbError
		}
	}

	if dbStatus != dbSuccess {
		err = fmt.Errorf("get key %s", dbStatusTag[dbStatus])
	}

	return
}

func (g *IntarkdbKV) Del(key string, prefix int32) (err error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var count int32 = -1
	cCount := (*C.int)(unsafe.Pointer(&count))

	g.TransactionBegin()
	ptr := (*C.intarkdbReply)(C.intarkdb_command_del(g.handle, cKey, C.int(prefix), cCount))
	defer g.free(ptr)

	dbStatus := dbError
	dbStatus = (DBStatus)(ptr._type)
	if dbStatus == dbSuccess {
		if count > 0 {
			fmt.Println("del key success")
			g.TransactionCommit()
		} else {
			dbStatus = dbError
		}
	}

	if dbStatus != dbSuccess {
		err = fmt.Errorf("del key %s", dbStatusTag[dbStatus])
	}

	return
}

func (g *IntarkdbKV) free(reply *C.intarkdbReply) {
	C.intarkdb_freeReplyObject(reply)
}

func (g *IntarkdbKV) TransactionBegin() (err error) {
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_kv_begin(g.handle))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("transaction begin %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbKV) TransactionCommit() (err error) {
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_kv_commit(g.handle))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("transaction commit %s", dbStatusTag[dbStatus])
	}
	return
}

func (g *IntarkdbKV) TransactionRollback() (err error) {
	dbStatus := dbError
	dbStatus = (DBStatus)(C.intarkdb_kv_rollback(g.handle))

	if dbStatus != dbSuccess {
		err = fmt.Errorf("transaction rollback %s", dbStatusTag[dbStatus])
	}
	return
}
