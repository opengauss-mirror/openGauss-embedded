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
* read.go
*
* IDENTIFICATION
* openGauss-embedded/interface/go/prometheus-remote-database/write_read/read.go
*
* -------------------------------------------------------------------------
 */

package write_read

import (
	"errors"
	"fmt"
	"prometheus-remote-database/config"
	"prometheus-remote-database/intarkdb_interface"
	"prometheus-remote-database/log"
	"prometheus-remote-database/pool"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
)

var separator string = "\xff"
var limitValue uint32 = 5000
var maxCount uint32 = 50000
var maxDayTimestamp uint64 = 7 * 24 * 60 * 60 * 1000
var rowCountEmptyError error = errors.New("The query result is empty")

func initQueryParam(config config.Table) {
	limitValue = config.LimitCount
	maxCount = config.MaxCount
	maxDayTimestamp = uint64(config.MaxDay) * 24 * 60 * 60 * 1000
}

func Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	resource, err := pool.Acquire(pool.ReadOperation)
	if err != nil {
		return nil, err
	}
	defer pool.Release(pool.ReadOperation, resource)
	defer resource.IntarkdbSQL.IntarkdbInitResult()
	defer resource.IntarkdbSQL.IntarkdbDestroyResult()

	labelsToSeries := map[string]*prompb.TimeSeries{}
	for _, q := range req.Queries {
		if uint64(q.EndTimestampMs-q.StartTimestampMs) > maxDayTimestamp {
			log.Warning("The maximum query interval is exceeded, Querie : %v", q)
			return nil, err
		}
		sql, sqlcount, tableName, err := buildSQL(q)
		if err != nil {
			log.Error("%s, Querie : %v", err.Error(), q)
			return nil, err
		}

		_, exists := scrapeNameMap[tableName]
		if !exists {
			continue
		}

		_, exists = tableMap[tableName]
		if !exists {
			continue
		}

		if tableName == "" {
			log.Warning("Metric Name Label is empty, Querie : %v", q)
			return nil, fmt.Errorf("Metric Name Label is empty, Querie : %v", q)
		}

		log.Debug("sql : %s", sql)

		err = resource.IntarkdbSQL.IntarkdbQuery(sqlcount)
		if err != nil {
			return nil, err
		}

		count := getCount(resource.IntarkdbSQL)
		if count == 0 {
			log.Warning("result is empty, Querie : %v", q)
			continue
		} else if count > maxCount {
			log.Warning("The maximum number of results was exceeded, sql : %s", sqlcount)
			continue
		}
		log.Debug("count : %d", count)

		startTime := time.Now()
		err = resource.IntarkdbSQL.IntarkdbQuery(sql)
		log.Debug("%s:%d IntarkdbQuery time : ", tableName, count, time.Since(startTime))
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
		startTime = time.Now()
		if err = mergeResult(labelsToSeries, tableName, resource.IntarkdbSQL); err != nil {
			if err == rowCountEmptyError {
				log.Warning("mergeResult err : %s, sql : %s", err.Error(), sql)
				break
			}
			log.Error("mergeResult err : %s, sql : %s", err.Error(), sql)
			return nil, err
		}
		log.Debug("%d mergeResult time : ", count, time.Since(startTime))

		// var offset uint32
		// for offset = 0; offset < count; offset += limitValue {
		// 	query := fmt.Sprintf("%s offset %d limit %d", sql, offset, limitValue)
		// 	startTime := time.Now()
		// 	err = resource.IntarkdbSQL.IntarkdbQuery(query)
		// 	log.Debug("IntarkdbQuery time : ", time.Since(startTime))
		// 	if err != nil {
		// 		log.Error(err.Error())
		// 		return nil, err
		// 	}
		// 	startTime = time.Now()
		// 	if err = mergeResult(labelsToSeries, tableName, resource.IntarkdbSQL); err != nil {
		// 		if err == rowCountEmptyError {
		// 			log.Warning("mergeResult err : %s, sql : %s", err.Error(), query)
		// 			break
		// 		}
		// 		log.Error("mergeResult err : %s, sql : %s", err.Error(), query)
		// 		return nil, err
		// 	}
		// 	log.Debug("mergeResult time : ", time.Since(startTime))
		// }
	}

	if len(labelsToSeries) == 0 {
		return nil, nil
	}

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{Timeseries: make([]*prompb.TimeSeries, 0, len(labelsToSeries))},
		},
	}
	startTime := time.Now()
	var count int
	for _, ts := range labelsToSeries {
		count += len(ts.Samples)
		for i := 1; i < len(ts.Samples); i++ {
			if ts.Samples[i-1].Timestamp > ts.Samples[i].Timestamp {
				log.Warning("Not in order")
				sort.Sort(ByTimestamp(ts.Samples))
				if ts.Samples[i-1].Timestamp > ts.Samples[i].Timestamp {
					log.Warning("sort not work")
				}
				break
			}
		}
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}
	log.Debug("%d labelsToSeries time : ", count, time.Since(startTime))
	return &resp, nil
}

type ByTimestamp []prompb.Sample

func (a ByTimestamp) Len() int           { return len(a) }
func (a ByTimestamp) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }
func (a ByTimestamp) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func buildSQL(q *prompb.Query) (sql, sqlCount, tableName string, err error) {
	matchers := make([]string, 0, len(q.Matchers))
	for _, m := range q.Matchers {
		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				tableName = m.Value
			default:
				err = errors.New("non-equal or regex matchers are not supported on the metric name yet")
				return
			}
			continue
		}

		switch m.Type {
		case prompb.LabelMatcher_EQ:
			matchers = append(matchers, fmt.Sprintf("%s = '%s'", m.Name, m.Value))
		case prompb.LabelMatcher_NEQ:
			matchers = append(matchers, fmt.Sprintf("%s != '%s'", m.Name, m.Value))
		default:
			err = fmt.Errorf("unknown match type %v or regex matchers are not supported", m.Type)
			return
		}
	}

	if tableName == "" {
		err = errors.New("No a metric name matcher found")
		return
	}

	if q.StartTimestampMs == q.EndTimestampMs {
		matchers = append(matchers, fmt.Sprintf("date = '%s'", model.Time(q.StartTimestampMs).Time().Format(IntarkdbTimestampFormats[4])))
	} else {
		matchers = append(matchers, fmt.Sprintf("date >= '%s'", model.Time(q.StartTimestampMs).Time().Format(IntarkdbTimestampFormats[4])))
		matchers = append(matchers, fmt.Sprintf("date <= '%s'", model.Time(q.EndTimestampMs).Time().Format(IntarkdbTimestampFormats[4])))
	}

	// sql = fmt.Sprintf("SELECT * FROM \"%s\" WHERE %v order by date asc", tableName, strings.Join(matchers, " AND "))
	sql = fmt.Sprintf("SELECT * FROM \"%s\" WHERE %v", tableName, strings.Join(matchers, " AND "))
	sqlCount = fmt.Sprintf("SELECT count(*) FROM \"%s\" WHERE %v", tableName, strings.Join(matchers, " AND "))

	return
}

func getCount(intarkdbSQL intarkdb_interface.IntarkdbSQL) uint32 {
	numStr := intarkdbSQL.IntarkdbValueVarchar(0, 0)
	num, err := strconv.ParseUint(numStr, 10, 32)
	if err != nil {
		log.Error("getCount err : %v", err)
		return 0
	}

	return uint32(num)
}

// 按所有列值group by
func mergeResult(labelsToSeries map[string]*prompb.TimeSeries, tableName string, intarkdbSQL intarkdb_interface.IntarkdbSQL) error {
	rowCount := intarkdbSQL.IntarkdbRowCount()
	log.Debug("rowCount = %d", rowCount)
	if rowCount > 0 {
		columnCount := intarkdbSQL.IntarkdbColumnCount()
		if columnCount < 2 {
			return errors.New("The number of resulting columns must be greater than or equal to two")
		}

		var columnName []string
		// 最后两列固定为date，value
		for col := int64(0); col < columnCount-2; col++ {
			columnName = append(columnName, intarkdbSQL.IntarkdbColumnName(col))
		}

		for row := int64(0); row < rowCount; row++ {
			col := int64(0)
			var columnValue []string
			for ; col < columnCount-2; col++ {
				columnValue = append(columnValue, intarkdbSQL.IntarkdbValueVarchar(row, col))
			}

			k := tableName + separator + concatLabels(columnName, columnValue)
			ts, ok := labelsToSeries[k]
			if !ok {
				ts = &prompb.TimeSeries{
					Labels: columnToLabelPairs(tableName, columnName, columnValue),
				}
				labelsToSeries[k] = ts
			}

			var sample prompb.Sample
			date := intarkdbSQL.IntarkdbValueVarchar(row, col)
			if timeVal, err := time.ParseInLocation(IntarkdbTimestampFormats[4], date, time.Local); err == nil {
				sample.Timestamp = timestamp.FromTime(timeVal)
			} else {
				continue
			}

			value := intarkdbSQL.IntarkdbValueVarchar(row, col+1)
			if valueFloat, err := strconv.ParseFloat(value, 64); err == nil {
				sample.Value = valueFloat
			} else {
				continue
			}

			ts.Samples = append(ts.Samples, sample)
			// ts.Samples = mergeSamples(ts.Samples, sample)
		}
	} else {
		return rowCountEmptyError
	}

	return nil
}

func concatLabels(name, value []string) string {
	// 0xff cannot occur in valid UTF-8 sequences, so use it
	// as a separator here.
	if len(name) != len(value) {
		return ""
	}

	pairs := make([]string, 0, len(name))
	for k, _ := range name {
		pairs = append(pairs, name[k]+separator+value[k])
	}
	return strings.Join(pairs, separator)
}

func columnToLabelPairs(name string, columnName, columnValue []string) []prompb.Label {
	if len(columnName) != len(columnValue) {
		return nil
	}

	pairs := make([]prompb.Label, 0, len(columnName)+1)

	pairs = append(pairs, prompb.Label{
		Name:  model.MetricNameLabel,
		Value: name,
	})

	for k, _ := range columnName {
		// if v == "" {
		// 	continue
		// }
		pairs = append(pairs, prompb.Label{
			Name:  columnName[k],
			Value: columnValue[k],
		})
	}

	return pairs
}

// 按顺序合入数据
func mergeSamples(a []prompb.Sample, b prompb.Sample) []prompb.Sample {
	// 使用二分查找找到插入位置
	index := sort.Search(len(a), func(i int) bool { return a[i].Timestamp >= b.Timestamp })

	// 在插入位置插入新值
	a = append(a, prompb.Sample{}) // 添加一个元素
	copy(a[index+1:], a[index:])   // 后移元素
	a[index] = b

	return a
}

func getPartitionTable() (tables []string) {
	resource, _ := pool.Acquire(pool.WriteOperation)

	defer pool.Release(pool.WriteOperation, resource)

	resource.IntarkdbSQL.IntarkdbQuery("select NAME from 'SYS_TABLES' where PARTITIONED = 1;")
	rowCount := resource.IntarkdbSQL.IntarkdbRowCount()
	log.Debug("getPartitionTable rowCount = %d", rowCount)
	if rowCount > 0 {
		for row := int64(0); row < rowCount; row++ {
			tables = append(tables, resource.IntarkdbSQL.IntarkdbValueVarchar(row, 0))
		}
	}

	return tables
}
