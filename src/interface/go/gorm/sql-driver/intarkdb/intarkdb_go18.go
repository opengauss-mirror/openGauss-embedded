/*
* Copyright (c) GBA-NCTI-ISDC. 2022-2024.
* Copyright (c) 2014 Yasuhiro Matsumoto
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
* intarkdb_go18.go
*
* IDENTIFICATION
* openGauss-embedded/src/interface/go/gorm/sql-driver/intarkdb/intarkdb_go18.go
*
* -------------------------------------------------------------------------
 */

package intarkdb

import (
	"database/sql/driver"

	"context"
)

// Ping implement Pinger.
func (c *IntarkdbConn) Ping(ctx context.Context) error {
	if c.conn == nil {
		// must be ErrBadConn for sql to close the database
		return driver.ErrBadConn
	}
	return nil
}

// QueryContext implement QueryerContext.
// func (c *IntarkdbConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
// 	return c.query(ctx, query, args)
// }

// // ExecContext implement ExecerContext.
// func (c *IntarkdbConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
// 	return c.exec(ctx, query, args)
// }

// PrepareContext implement ConnPrepareContext.
func (c *IntarkdbConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prepare(ctx, query)
}

// BeginTx implement ConnBeginTx.
func (c *IntarkdbConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.begin(ctx)
}

// QueryContext implement QueryerContext.
// func (s *IntarkdbStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
// 	return s.query(ctx, args)
// }

// // ExecContext implement ExecerContext.
// func (s *IntarkdbStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
// 	return s.exec(ctx, args)
// }
