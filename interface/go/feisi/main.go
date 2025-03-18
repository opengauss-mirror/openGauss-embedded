package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"feisi/intarkdb_interface"
)

var testNumber int
var queryConcurrency int
var intarkdb intarkdb_interface.Intarkdb
var intarkdbSQL []intarkdb_interface.IntarkdbSQL
var createSQL string = "CREATE TABLE if not exists sql_test_%d (id int, date timestamp,value int) PARTITION BY RANGE(date) TIMESCALE INTERVAL '1h' RETENTION '30d' AUTOPART;"
var insertSQL string = "INSERT INTO sql_test_%d VALUES "
var timestampFormats string = "2006-01-02 15:04:05.999999"

func init() {
	flag.IntVar(&testNumber, "test_number", 0, "压测序号, 1、2、3选取对应压测")
	flag.IntVar(&queryConcurrency, "query_concurrency", 10, "并发数")
	flag.Parse()
	fmt.Println("test number:", testNumber)
	fmt.Println("query concurrency:", queryConcurrency)

	err := intarkdb.IntarkdbOpen(".")
	if err != nil {
		log.Panic(err)
	}

	for i := 0; i < queryConcurrency; i++ {
		var conn intarkdb_interface.IntarkdbSQL
		conn.DB = intarkdb

		err = conn.IntarkdbConnect()
		if err != nil {
			release()
			log.Panic(err)
		}

		err = conn.IntarkdbInitResult()
		if err != nil {
			release()
			log.Panic(err)
		}

		intarkdbSQL = append(intarkdbSQL, conn)
	}
}

func main() {
	defer release()

	if len(intarkdbSQL) == 0 {
		log.Panic("intarkdbSQL is empty")
	}

	switch testNumber {
	case 1:
		multipleTableInsert()
	case 2:
		singleTableInsert()
	case 3:
		periodInsert()
	default:
		fmt.Println("unknown parameter")
	}

}

func multipleTableInsert() {
	var sql string
	var err error
	for i := 1; i <= 1500; i++ {
		sql = fmt.Sprintf(createSQL, i)
		err = intarkdbSQL[0].IntarkdbQuery(sql)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		// fmt.Println(i)
	}

	var query []string
	for i := 1; i <= 1500; i++ {
		sql = fmt.Sprintf(insertSQL, i)
		sql += fmt.Sprintf("(%d, '%s',%d)", i, time.Now().Local().Format(timestampFormats), i)
		query = append(query, sql)
	}

	segLen := 1500 / queryConcurrency
	startTime := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < queryConcurrency; i++ {
		wg.Add(1)
		go func(i int, query []string) {
			defer wg.Done()

			for _, v := range query {
				err := intarkdbSQL[i].IntarkdbQuery(v)
				if err != nil {
					fmt.Println(i, v, err)
				}
			}

		}(i, query[i*segLen:(i+1)*segLen])
	}
	wg.Wait()
	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	fmt.Printf("query 条数： %d, 代码执行时间为: %s\n", len(query), elapsedTime)
}

func singleTableInsert() {
	err := intarkdbSQL[0].IntarkdbQuery(fmt.Sprintf(createSQL, 1))
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	var values []string
	for i := 1; i <= 1500; i++ {
		sql := fmt.Sprintf("(%d,'%s',%d)", i, time.Now().Local().Format(timestampFormats), i)
		values = append(values, sql)
	}

	segLen := 1500 / queryConcurrency
	query := fmt.Sprintf(insertSQL, 1)

	var querys []string
	for i := 0; i < queryConcurrency; i++ {
		querys = append(querys, (query + strings.Join(values[i*segLen:(i+1)*segLen], ",") + ";"))
	}

	startTime := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < queryConcurrency; i++ {
		wg.Add(1)
		go func(i int, query string) {
			defer wg.Done()

			err := intarkdbSQL[i].IntarkdbQuery(query)
			if err != nil {
				fmt.Println(i, query, err)
			}

		}(i, querys[i])
	}
	wg.Wait()
	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	fmt.Printf("query 条数： %d, 代码执行时间为: %s\n", len(values), elapsedTime)
}

func periodInsert() {
	var sql string
	var err error
	for i := 1; i <= queryConcurrency; i++ {
		sql = fmt.Sprintf(createSQL, i)
		err = intarkdbSQL[0].IntarkdbQuery(sql)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	stopChan := make(chan struct{}, queryConcurrency)
	segLen := 1500 / queryConcurrency

	var wg sync.WaitGroup
	for number := 1; number <= queryConcurrency; number++ {
		wg.Add(1)
		go func(number int) {
			defer wg.Done()

			var count int
			var err error
			var elapsedTime, totalTime time.Duration
			timer := time.NewTicker(500 * time.Millisecond)
			for {
				select {
				case <-timer.C:
					elapsedTime, err = insert(number, segLen)
					if err == nil {
						count++
						totalTime += elapsedTime
					}
				case <-stopChan:
					timer.Stop()
					fmt.Printf("insert count: %d, total time: %s, avg time: %s\n",
						count, totalTime, totalTime/time.Duration(count))
					return
				}
			}

		}(number)
	}

	time.Sleep(12 * time.Hour)
	close(stopChan)
	wg.Wait()
}

func insert(number, segLen int) (elapsedTime time.Duration, err error) {
	var values []string
	for i := 1; i <= segLen; i++ {
		sql := fmt.Sprintf("(%d,'%s',%d)", i, time.Now().Local().Format(timestampFormats), i)
		values = append(values, sql)
	}

	query := fmt.Sprintf(insertSQL, number)
	query += (strings.Join(values, ",") + ";")

	startTime := time.Now()
	err = intarkdbSQL[number-1].IntarkdbQuery(query)
	if err != nil {
		fmt.Println(number, query, err)
	}
	endTime := time.Now()
	elapsedTime = endTime.Sub(startTime)

	return elapsedTime, err
}

func release() {
	for i := 0; i < len(intarkdbSQL); i++ {
		intarkdbSQL[i].IntarkdbDestroyResult()
		intarkdbSQL[i].IntarkdbDisconnect()
	}

	intarkdb.IntarkdbClose()
}
