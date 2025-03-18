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
* config.go
*
* IDENTIFICATION
* openGauss-embedded/src/interface/go/prometheus-remote-database/config/config.go
*
* -------------------------------------------------------------------------
 */

package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Table      `yaml:"table"`
	LogFile    `yaml:"log_file"`
	Pool       `yaml:"pool"`
	HttpServer `yaml:"http_server"`
}

type Table struct {
	Interval      string   `yaml:"interval"`
	Retention     string   `yaml:"retention"`
	LimitCount    uint32   `yaml:"limit_count"`
	MaxCount      uint32   `yaml:"max_count"`
	MaxDay        uint32   `yaml:"max_day"`
	Names         []string `yaml:"name"`
	NameWithQuery []string `yaml:"name_with_query"`
}

type LogFile struct {
	Path        string `yaml:"path"`
	Name        string `yaml:"name"`
	IsFile      bool   `yaml:"is_file"`
	MaxFileSize uint64 `yaml:"max_file_size"`
	Level       string `yaml:"level"`
}

type Pool struct {
	WritePoolSize uint `yaml:"write_pool_size"`
	ReadPoolSize  uint `yaml:"read_pool_size"`
	WriteTimeout  uint `yaml:"write_timeout"`
	ReadTimeout   uint `yaml:"read_timeout"`
}

type HttpServer struct {
	Port     uint   `yaml:"port"`
	ReadURL  string `yaml:"read_url"`
	WriteURL string `yaml:"write_url"`
}

func init() {
	_, errOne := os.Stat("config.yml")      // 需要存在
	_, errTwo := os.Stat("config_temp.yml") // 需要不存在
	if errOne != nil || !os.IsNotExist(errTwo) {
		fmt.Println("config file err")
		os.Exit(1)
	}
}

func LoadConfig() (config Config, err error) {
	var content []byte
	content, err = os.ReadFile("config.yml")
	if err != nil {
		fmt.Println("error : ", err)
		return
	}

	err = yaml.Unmarshal(content, &config)
	if err != nil {
		fmt.Println("error : ", err)
		return
	}

	return
}

func (config *Config) UpdateConfig() error {
	// config.Tables = append(config.Tables, config.CreateTables...)
	// config.CreateTables = nil

	var output []byte
	output, err := yaml.Marshal(&config)
	if err != nil {
		return fmt.Errorf("yaml.Marshal err : %v", err)
	}

	err = os.WriteFile("config_temp.yml", output, 0644)
	if err != nil {
		return fmt.Errorf("WriteFile err : %v", err)
	}

	err = os.Remove("config.yml")
	if err != nil {
		return fmt.Errorf("Remove err : %v", err)
	}

	err = os.Rename("config_temp.yml", "config.yml")
	if err != nil {
		return fmt.Errorf("Rename err : %v", err)
	}

	return nil
}
