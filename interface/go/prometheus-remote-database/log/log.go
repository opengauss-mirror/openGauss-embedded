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
* log.go
*
* IDENTIFICATION
* openGauss-embedded/interface/go/prometheus-remote-database/log/log.go
*
* -------------------------------------------------------------------------
 */

package log

import (
	"errors"
	"fmt"
	"os"
	"path"
	"prometheus-remote-database/config"
	"strings"
)

type LogLevel uint16

var Log *FileLogger

const (
	UNKNOWN LogLevel = iota
	DEBUG
	INFO
	WARNING
	ERROR
)

var logLevelToString = map[LogLevel]string{
	UNKNOWN: "unknown",
	DEBUG:   "debug",
	INFO:    "info",
	WARNING: "warning",
	ERROR:   "error",
}

var stringToLogLevel = map[string]LogLevel{
	// "unknown": UNKNOWN,
	"debug":   DEBUG,
	"info":    INFO,
	"warning": WARNING,
	"error":   ERROR,
}

func parseLogLevel(s string) (LogLevel, error) {
	s = strings.ToLower(s)
	level, ok := stringToLogLevel[s]
	if !ok {
		return UNKNOWN, errors.New("Invalid log level")
	}
	return level, nil
}

func getLogStr(level LogLevel) string {
	return logLevelToString[level]
}

type FileLogger struct {
	Level       LogLevel
	filePath    string
	fileName    string
	fileObj     *os.File
	maxFileSize uint64
}

func NewFlieLogger(l config.LogFile) {
	level, err := parseLogLevel(l.Level)
	if err != nil {
		panic(err)
	}
	Log = &FileLogger{
		Level:       level,
		filePath:    l.Path,
		fileName:    l.Name,
		maxFileSize: l.MaxFileSize,
	}
	err = Log.initFile(l.IsFile)
	if err != nil {
		panic(err)
	}
}

func (f *FileLogger) initFile(isFile bool) error {
	if isFile {
		join := path.Join(f.filePath, f.fileName)
		fileObj, err := os.OpenFile(join, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Printf("open log fail ,err: %v\n", err)
			return err
		}

		f.fileObj = fileObj
	} else {
		f.fileObj = os.Stdout
	}

	return nil
}

func (f *FileLogger) enable(level LogLevel) bool {
	return level >= f.Level
}

func (f *FileLogger) log(leve LogLevel, msg string) {
	fmt.Fprintf(f.fileObj, "[%s] %s\n", getLogStr(leve), msg)
}

func Debug(msg string, a ...interface{}) {
	msg = fmt.Sprintf(msg, a...)
	if Log.enable(DEBUG) {
		Log.log(DEBUG, msg)
	}
}

func Info(msg string, a ...interface{}) {
	msg = fmt.Sprintf(msg, a...)
	if Log.enable(INFO) {
		Log.log(INFO, msg)
	}
}

func Warning(msg string, a ...interface{}) {
	msg = fmt.Sprintf(msg, a...)
	if Log.enable(WARNING) {
		Log.log(WARNING, msg)
	}
}

func Error(msg string, a ...interface{}) {
	msg = fmt.Sprintf(msg, a...)
	if Log.enable(ERROR) {
		Log.log(ERROR, msg)
	}
}

func (f *FileLogger) Close() {
	Info("log close!")
	f.fileObj.Close()
}
