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
* intarkdb.go
*
* IDENTIFICATION
* openGauss-embedded/interface/go/gorm/sql-driver/intarkdb/intarkdb.go
*
* -------------------------------------------------------------------------
 */

package intarkdb

/*
// 头文件的位置，相对于源文件是当前目录，所以是 .，头文件在多个目录时写多个  #cgo CFLAGS: ...
#cgo CFLAGS: -I./include
// 从哪里加载动态库，位置与文件名，-ladd 加载 libadd.so 文件
// 支持绝对路径
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
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func init() {
	if driverName != "" {
		sql.Register(driverName, &IntarkdbDriver{})
	}
}

var driverName = "intarkdb"

type DBStatus int

const (
	dbError   DBStatus = -1
	dbSuccess DBStatus = 0
	dbTimeout DBStatus = 1
)

var dbStatusTag = map[DBStatus]string{
	dbError:   "error",
	dbSuccess: "success",
	dbTimeout: "timeout",
}

var IntarkdbTimestampFormats = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

type IntarkdbDriver struct {
}

var (
	db    C.intarkdb_database
	mutex sync.Mutex
	once  sync.Once
)

type IntarkdbConn struct {
	mu      sync.Mutex
	conn    C.intarkdb_connection
	closed  bool
	closeDB bool
}

// IntarkdbTx implements driver.Tx.
type IntarkdbTx struct {
	c *IntarkdbConn
}

// IntarkdbStmt implements driver.Stmt.
type IntarkdbStmt struct {
	mu sync.Mutex
	c  *IntarkdbConn
	s  C.intarkdb_prepared_statement
	// t      string
	closed bool
	// cls    bool
}

// IntarkdbResult implements sql.Result.
type IntarkdbResult struct {
	// id      int64
	changes int64
}

// IntarkdbRows implements driver.Rows.
type IntarkdbRows struct {
	s        *IntarkdbStmt
	colCount int64
	cols     []string
	// decltype []string
	// cls      bool
	closed bool
	ctx    context.Context // no better alternative to pass context into Next() method
	result C.intarkdb_result
	// colType []
}

func (tx *IntarkdbTx) Commit() error {
	query := "COMMIT"
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	dbStatus := (DBStatus)(C.intarkdb_query(tx.c.conn, cQuery, nil))
	if dbStatus != dbSuccess {
		tx.Rollback()
		return fmt.Errorf("intarkdb commit %s", dbStatusTag[dbStatus])
	}

	return nil
}

func (tx *IntarkdbTx) Rollback() error {
	query := "ROLLBACK"
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	dbStatus := (DBStatus)(C.intarkdb_query(tx.c.conn, cQuery, nil))
	if dbStatus != dbSuccess {
		return fmt.Errorf("intarkdb rollback %s", dbStatusTag[dbStatus])
	}

	return nil
}

func (d *IntarkdbDriver) Open(dsn string) (driver.Conn, error) {
	pos := strings.IndexRune(dsn, '?')
	var closeDB string
	if pos >= 1 {
		params, err := url.ParseQuery(dsn[pos+1:])
		if err != nil {
			return nil, err
		}

		if val := params.Get("closeDB"); val != "" {
			closeDB = val
		}
	}

	cPath := C.CString(dsn)
	defer C.free(unsafe.Pointer(cPath))

	var dbStatus DBStatus
	once.Do(func() {
		dbStatus = (DBStatus)(C.intarkdb_open(cPath, &db))
	})

	if dbStatus != dbSuccess {
		return nil, fmt.Errorf("intarkdb open %s", dbStatusTag[dbStatus])
	}

	var connection C.intarkdb_connection
	dbStatus = (DBStatus)(C.intarkdb_connect(db, &connection))
	if dbStatus != dbSuccess {
		return nil, fmt.Errorf("intarkdb connection %s", dbStatusTag[dbStatus])
	}

	conn := &IntarkdbConn{conn: connection}
	if closeDB == "true" {
		conn.closeDB = true
	}

	return conn, nil
}

func (c *IntarkdbConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(context.Background(), query)
}

func (c *IntarkdbConn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	var s C.intarkdb_prepared_statement

	dbStatus := (DBStatus)(C.intarkdb_prepare(c.conn, cQuery, &s))
	if dbStatus != dbSuccess {
		return nil, fmt.Errorf("intarkdb prepare %s", C.GoString(C.intarkdb_prepare_errmsg(s)))
	}

	ss := &IntarkdbStmt{c: c, s: s}

	return ss, nil
}

// func (c *IntarkdbConn) Query(query string, args []driver.Value) (driver.Rows, error) {
// 	return nil,nil
// 	// return c.query(context.Background(), query, args)
// }

func (c *IntarkdbConn) Close() error {
	C.intarkdb_disconnect(&(c.conn))
	// c.mu.Lock()
	c.closed = true
	// c.mu.Unlock()
	if c.closeDB {
		// mutex.Lock()
		// defer mutex.Unlock()
		C.intarkdb_close(&db)
	}
	return nil
}

func (c *IntarkdbConn) Begin() (driver.Tx, error) {
	return c.begin(context.Background())
}

func (c *IntarkdbConn) begin(ctx context.Context) (driver.Tx, error) {
	query := "Begin"
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	dbStatus := (DBStatus)(C.intarkdb_query(c.conn, cQuery, nil))
	if dbStatus != dbSuccess {
		return nil, fmt.Errorf("intarkdb begin %s", dbStatusTag[dbStatus])
	}
	return &IntarkdbTx{c}, nil
}

func (c *IntarkdbConn) dbConnOpen() bool {
	if c == nil {
		return false
	}
	// c.mu.Lock()
	// defer c.mu.Unlock()
	return !c.closed
}

func (s *IntarkdbStmt) NumInput() int {
	return int(C.intarkdb_prepare_nparam(s.s))
}

// Close the statement.
func (s *IntarkdbStmt) Close() error {
	// s.mu.Lock()
	// defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if !s.c.dbConnOpen() {
		return errors.New("intarkdb statement with already closed database connection")
	}
	C.intarkdb_destroy_prepare(&s.s)
	return nil
}

func (s *IntarkdbStmt) bind(args []driver.Value) error {
	if len(args) != s.NumInput() {
		return fmt.Errorf("the number of args does not match")
	}

	var dbStatus DBStatus
	for i, arg := range args {
		cI := C.__uint32_t(i + 1)
		switch v := arg.(type) {
		case nil:
			dbStatus = (DBStatus)(C.intarkdb_bind_null(s.s, cI))
		case string:
			cQuery := C.CString(v)
			defer C.free(unsafe.Pointer(cQuery))
			dbStatus = (DBStatus)(C.intarkdb_bind_varchar(s.s, cI, cQuery))
		case int32:
			dbStatus = (DBStatus)(C.intarkdb_bind_int32(s.s, cI, C.int(v)))
		case int64:
			dbStatus = (DBStatus)(C.intarkdb_bind_int64(s.s, cI, C.long(v)))
		case uint32:
			dbStatus = (DBStatus)(C.intarkdb_bind_uint32(s.s, cI, C.__uint32_t(v)))
		case uint64:
			dbStatus = (DBStatus)(C.intarkdb_bind_uint64(s.s, cI, C.__uint64_t(v)))
		case bool:
			dbStatus = (DBStatus)(C.intarkdb_bind_boolean(s.s, cI, C.bool(v)))
		case float64:
			dbStatus = (DBStatus)(C.intarkdb_bind_double(s.s, cI, C.double(v)))
		case []byte:
			if v == nil {
				dbStatus = (DBStatus)(C.intarkdb_bind_null(s.s, cI))
			} else {
				return fmt.Errorf("not support byte")
			}
		case time.Time:
			b := []byte(v.Format(IntarkdbTimestampFormats[4]))
			dbStatus = (DBStatus)(C.intarkdb_bind_date(s.s, cI, (*C.char)(unsafe.Pointer(&b[0]))))
		default:
			dbStatus = dbError
		}
		if dbStatus != dbSuccess {
			return fmt.Errorf("bind arg err %s", dbStatusTag[dbStatus])
		}
	}

	return nil
}

func (s *IntarkdbStmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}

	result := C.intarkdb_init_result()
	dbStatus := (DBStatus)(C.intarkdb_execute_prepared(s.s, result))

	if dbStatus != dbSuccess {
		return nil, fmt.Errorf("intarkdb exec %s", C.GoString(C.intarkdb_result_msg(result)))
	}

	count := int64(C.intarkdb_row_count(result))
	C.intarkdb_destroy_result(result)

	return &IntarkdbResult{changes: count}, nil
}

func (s *IntarkdbStmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}

	result := C.intarkdb_init_result()
	dbStatus := (DBStatus)(C.intarkdb_execute_prepared(s.s, result))
	if dbStatus != dbSuccess {
		return nil, fmt.Errorf("intarkdb exec %s", C.GoString(C.intarkdb_result_msg(result)))
	}

	colCount := int64(C.intarkdb_column_count(result))

	return &IntarkdbRows{result: result, colCount: colCount}, nil
}

func (rc *IntarkdbRows) Next(dest []driver.Value) error {
	if !bool(C.intarkdb_next_row(rc.result)) {
		return io.EOF
	}
	for i := range dest {
		value := C.GoString(C.intarkdb_column_value(rc.result, C.long(i)))
		ctype := C.intarkdb_column_type(rc.result, C.long(i))
		switch ctype {
		case C.GS_TYPE_VARCHAR:
			dest[i] = value
		case C.GS_TYPE_REAL, C.GS_TYPE_DECIMAL:
			dest[i], _ = strconv.ParseFloat(value, 64)
		case C.GS_TYPE_BOOLEAN:
			dest[i], _ = strconv.ParseBool(value)
		case C.GS_TYPE_INTEGER:
			dest[i], _ = strconv.ParseInt(value, 0, 32)
		case C.GS_TYPE_BIGINT:
			dest[i], _ = strconv.ParseInt(value, 0, 64)
		case C.GS_TYPE_UINT32:
			dest[i], _ = strconv.ParseUint(value, 0, 32)
		case C.GS_TYPE_UINT64:
			dest[i], _ = strconv.ParseUint(value, 0, 64)
		case C.GS_TYPE_TIMESTAMP:
			if timeVal, err := time.ParseInLocation(IntarkdbTimestampFormats[4], value, time.Local); err == nil {
				dest[i] = timeVal
			} else {
				// The column is a time value, so return the zero time on parse failure.
				dest[i] = time.Time{}
			}
		}
		// dest[i] = C.GoString(C.intarkdb_column_value(rc.result, C.long(i)))
	}

	return nil
}

func (rc *IntarkdbRows) declTypes() []string {

	return nil
}

func (rc *IntarkdbRows) Close() error {
	// rc.s.mu.Lock()
	// if rc.s.closed || rc.closed {
	if rc.closed {
		// rc.s.mu.Unlock()
		return nil
	}
	rc.closed = true
	C.intarkdb_destroy_result(rc.result)
	// rc.s.mu.Unlock()
	return nil
}

func (rc *IntarkdbRows) Columns() []string {
	// rc.s.mu.Lock()
	// defer rc.s.mu.Unlock()
	rc.cols = make([]string, rc.colCount)
	for i := 0; i < int(rc.colCount); i++ {
		rc.cols[i] = C.GoString(C.intarkdb_column_name(rc.result, C.long(i)))
	}
	return rc.cols
}

func (r *IntarkdbResult) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected return how many rows affected.
func (r *IntarkdbResult) RowsAffected() (int64, error) {
	return r.changes, nil
}
