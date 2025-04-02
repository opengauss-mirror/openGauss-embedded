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
* write.go
*
* IDENTIFICATION
* openGauss-embedded/interface/go/prometheus-remote-database/write_read/write.go
*
* -------------------------------------------------------------------------
 */

package write_read

import (
	"fmt"
	"math"
	"prometheus-remote-database/config"
	"prometheus-remote-database/log"
	"prometheus-remote-database/pool"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"golang.org/x/sync/singleflight"
)

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

var tableMap = make(map[string]struct{})
var scrapeNameMap = make(map[string]struct{})
var scrapeNameWithQueryMap = make(map[string]struct{})
var interval, retention string
var g singleflight.Group
var queryLabel string = "query"

// 初始化指标map
func InitValue(config config.Table) {
	interval = config.Interval
	retention = config.Retention

	tables := getPartitionTable()

	for _, v := range tables {
		tableMap[v] = struct{}{}
	}

	for _, v := range config.Names {
		scrapeNameMap[v] = struct{}{}
	}
	for _, v := range config.NameWithQuery {
		scrapeNameMap[v] = struct{}{}
		scrapeNameWithQueryMap[v] = struct{}{}
	}
	log.Debug("scrapeNameMap len : ", len(scrapeNameMap))
	log.Debug("scrapeNameWithQueryMap len : ", len(scrapeNameWithQueryMap))

	log.Debug("tableMap len : ", len(tableMap))

	initQueryParam(config)
}

func Write(samples model.Samples) error {
	var insertSQL []*string

	for _, s := range samples {
		tableName := string(s.Metric[model.MetricNameLabel])
		if tableName == "" {
			log.Warning("Metric Name Label is empty, sample : %v", s)
			continue
		}

		_, exists := scrapeNameMap[tableName]
		if !exists {
			continue
		}

		_, exists = scrapeNameWithQueryMap[tableName]
		if exists {
			if _, labelExists := s.Metric[model.LabelName(queryLabel)]; labelExists {
				s.Metric[model.LabelName(queryLabel)] = ""
			}
		}

		value := float64(s.Value)
		var isNaNOrInf bool
		if math.IsNaN(value) || math.IsInf(value, 0) {
			// log.Warning("Cannot send to intarkdb, skipping sample, sample : %v", s)
			isNaNOrInf = true
			// continue
		}

		timestamp := s.Timestamp.Time().Local().Format(IntarkdbTimestampFormats[4])

		var columns, values []string
		for l, v := range s.Metric {
			if l != model.MetricNameLabel {
				columns = append(columns, string(l))
				values = append(values, ("'" + string(v) + "'"))
			}
		}

		_, exists = tableMap[tableName]
		if !exists {
			log.Debug("tableName : ", tableName)
			log.Debug("tableMap : ", tableMap)
			_, err, _ := g.Do(tableName, func() (i interface{}, e error) {
				return nil, createTable(tableName, columns)
			})

			if err != nil {
				log.Error("create table %s err : %s", tableName, err.Error())
				continue
			}
		}

		columns = append(columns, "date", "value")
		values = append(values, "'"+timestamp+"'")
		if isNaNOrInf {
			values = append(values, fmt.Sprintf("'%g'", value))
		} else {
			values = append(values, fmt.Sprintf("%g", value))
		}

		query := fmt.Sprintf("insert into \"%s\"(%s) values (%s);", tableName, strings.Join(columns, ","), strings.Join(values, ","))

		insertSQL = append(insertSQL, &query)
	}

	resource, err := pool.Acquire(pool.WriteOperation)
	if err != nil {
		log.Warning(err.Error())
		return err
	}
	defer pool.Release(pool.WriteOperation, resource)

	for _, v := range insertSQL {
		err := resource.IntarkdbSQL.IntarkdbQuery(*v)
		if err != nil {
			log.Error(err.Error())
		}
	}

	log.Info("%s write %d success", time.Now().Local().Format(IntarkdbTimestampFormats[4]), len(insertSQL))
	return nil
}

func createTable(tableName string, columns []string) error {
	sql := fmt.Sprintf("CREATE TABLE \"%s\" (", tableName)
	for _, v := range columns {
		sql += (v + " string, ")
	}
	sql += "date timestamp, value float64) PARTITION BY RANGE(date) "
	sql += fmt.Sprintf("timescale interval '%s' retention '%s' autopart;", interval, retention)

	log.Info("createTable sql : ", sql)
	resource, err := pool.Acquire(pool.WriteOperation)
	if err != nil {
		return err
	}
	defer pool.Release(pool.WriteOperation, resource)

	err = resource.IntarkdbSQL.IntarkdbQuery(sql)
	if err == nil {
		tableMap[tableName] = struct{}{}
	}

	return err
}
