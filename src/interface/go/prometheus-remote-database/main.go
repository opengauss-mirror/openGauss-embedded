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
* openGauss-embedded/src/interface/go/prometheus-remote-database/main.go
*
* -------------------------------------------------------------------------
 */

package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"

	"prometheus-remote-database/config"
	"prometheus-remote-database/log"
	"prometheus-remote-database/pool"
	"prometheus-remote-database/write_read"
	_ "prometheus-remote-database/write_read"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

func main() {
	// 加载配置文件
	config, err := config.LoadConfig()
	if err != nil {
		os.Exit(1)
	}

	// 初始化log
	log.NewFlieLogger(config.LogFile)
	log.Info("log init success!")
	defer log.Log.Close()

	log.Debug("config : %v", config)

	// 初始化资源池
	pool.InitValue(config.Pool)
	err = pool.NewResourcePool()
	if err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
	log.Info("NewResourcePool success!")
	defer pool.CleanUp()

	// 初始化write_read
	write_read.InitValue(config.Table)

	http.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    ":" + strconv.FormatUint(uint64(config.Port), 10),
		Handler: http.DefaultServeMux,
	}

	go func() {
		if err := serve(config.ReadURL, config.WriteURL, server); err != nil {
			log.Warning("Failed to listen, %s", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-c
	log.Warning("Received signal, shutting down...")

	// 创建一个带有超时的Context，用于优雅关闭服务
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()

	// 关闭HTTP服务
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("Error shutting down server: %s\n", err)
	}
}

func serve(readURL, writeURL string, server *http.Server) (err error) {
	http.HandleFunc(writeURL, func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		samples := protoToSamples(req)

		err = write_read.Write(samples)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	http.HandleFunc(readURL, func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var resp *prompb.ReadResponse
		resp, err = write_read.Read(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if resp != nil {
			log.Debug("prompb.ReadRequest : %v", req)
			log.Debug("prompb.ReadResponse : %v", len(resp.Results))
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Error("Error writing response, err : %s", err)
		}
	})

	if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("Error starting server: %s", err)
	}

	return
}

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}

func forWrite() {
	var samples model.Samples
	myMap := make(map[model.LabelName]model.LabelValue)
	myMap[model.MetricNameLabel] = "agent_cpu_seconds_total"
	myMap["cpu"] = "80"
	myMap["host"] = "1813466887472824322"
	myMap["instance"] = "142a8d06-02e1-4fd0-b1fa-ecdf6b45171f"
	myMap["job"] = "142a8d06-02e1-4fd0-b1fa-ecdf6b45171f_5s"
	myMap["mode"] = "nice"
	myMap["type"] = "exporter"
	sample := &model.Sample{
		Metric:    model.Metric(model.LabelSet(myMap)),
		Value:     model.SampleValue(math.Inf(1)),
		Timestamp: model.Now(),
	}
	samples = append(samples, sample)
	for i := 1; i <= 10; i++ {
		time.Sleep(1 * 100 * time.Millisecond)
		samples[0].Timestamp = model.Now()
		go write_read.Write(samples)
	}

	time.Sleep(90 * 1000 * time.Millisecond)
}

func forRead() {
	var req prompb.ReadRequest
	var query prompb.Query
	var labels prompb.LabelMatcher

	labels.Type = prompb.LabelMatcher_EQ
	labels.Name = model.MetricNameLabel
	labels.Value = "agent_cpu_seconds_total"

	query.Matchers = append(query.Matchers, &labels)
	query.EndTimestampMs = 1728718643000
	query.StartTimestampMs = 1728700643000
	req.Queries = append(req.Queries, &query)
	resp, _ := write_read.Read(&req)
	fmt.Println(resp)
}
