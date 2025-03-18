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
* pool.go
*
* IDENTIFICATION
* openGauss-embedded/src/interface/go/prometheus-remote-database/pool/pool.go
*
* -------------------------------------------------------------------------
 */

package pool

import (
	"fmt"
	"os"
	"prometheus-remote-database/config"
	"prometheus-remote-database/intarkdb_interface"
	"sync"
	"time"

	"math/rand"
)

var writePoolSize uint = 10
var readPoolSize uint = 2
var writeTimeout time.Duration = 60 * time.Second
var readTimeout time.Duration = 30 * time.Second
var writePool *ResourcePool
var readPool *ResourcePool
var intarkdb intarkdb_interface.Intarkdb

type OperationType int

const (
	WriteOperation OperationType = 1
	ReadOperation  OperationType = 2
)

type Resource struct {
	ID          uint
	IntarkdbSQL intarkdb_interface.IntarkdbSQL
}

type ResourcePool struct {
	mu        sync.Mutex
	resources []*Resource
}

func init() {
	err := intarkdb.IntarkdbOpen(".")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func InitValue(p config.Pool) {
	writePoolSize = p.WritePoolSize
	readPoolSize = p.ReadPoolSize
	writeTimeout = time.Duration(p.WriteTimeout) * time.Second
	readTimeout = time.Duration(p.ReadTimeout) * time.Second
}

func NewResourcePool() (err error) {
	writePool = &ResourcePool{
		resources: make([]*Resource, writePoolSize),
	}

	err = initPool(WriteOperation)
	if err != nil {
		return fmt.Errorf("write pool err : %v", err)
	}

	readPool = &ResourcePool{
		resources: make([]*Resource, readPoolSize),
	}

	err = initPool(ReadOperation)
	if err != nil {
		return fmt.Errorf("read pool err : %v", err)
	}

	return
}

func initPool(operation OperationType) (err error) {
	var pool *ResourcePool
	var size uint
	switch operation {
	case WriteOperation:
		pool = writePool
		size = writePoolSize
	case ReadOperation:
		pool = readPool
		size = readPoolSize
	}

	for i := uint(0); i < size; i++ {
		pool.resources[i] = &Resource{ID: i}
		var intarkdbSQL intarkdb_interface.IntarkdbSQL
		intarkdbSQL.DB = intarkdb

		err = intarkdbSQL.IntarkdbConnect()
		if err != nil {
			return
		}

		err = intarkdbSQL.IntarkdbInitResult()
		if err != nil {
			return
		}
		pool.resources[i].IntarkdbSQL = intarkdbSQL
	}

	return
}

func Acquire(operation OperationType) (*Resource, error) {
	var pool *ResourcePool
	var timeout time.Duration
	switch operation {
	case WriteOperation:
		pool = writePool
		timeout = writeTimeout
	case ReadOperation:
		pool = readPool
		timeout = readTimeout
	}

	startTime := time.Now()
	rand.NewSource(time.Now().UnixNano())
	for {
		pool.mu.Lock()
		if len(pool.resources) > 0 {
			res := pool.resources[0]
			pool.resources = pool.resources[1:]
			pool.mu.Unlock()
			return res, nil
		}
		pool.mu.Unlock()

		if time.Since(startTime) >= timeout {
			return nil, fmt.Errorf("%s operation %d timeout: no resources available",
				time.Now().Format("2006-01-02 15:04:05"), operation)
		}

		randomInt := rand.Intn(500)
		time.Sleep(time.Duration(randomInt) * time.Millisecond) // 等待一段时间后重试
	}
}

func Release(operation OperationType, resource *Resource) {
	var pool *ResourcePool
	switch operation {
	case WriteOperation:
		pool = writePool
	case ReadOperation:
		pool = readPool
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.resources = append(pool.resources, resource)
}

func CleanUp() {
	cleanPool(ReadOperation)
	cleanPool(WriteOperation)

	intarkdb.IntarkdbClose()
}

func cleanPool(operation OperationType) {
	var pool *ResourcePool
	switch operation {
	case WriteOperation:
		pool = writePool
	case ReadOperation:
		pool = readPool
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, res := range pool.resources {
		res.IntarkdbSQL.IntarkdbDestroyResult()
		res.IntarkdbSQL.IntarkdbDisconnect()
	}

	pool.resources = nil
}
