package config

import (
	"fmt"
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

// config is the data model for deserialization the config yaml file

type Config struct {
	DBAcc DBAccount
	MPAcc MPAccount
}

type DBAccount struct {
	Username string `yaml:"db-user"`
	Password string `yaml:"db-pass"`
}

type MPAccount struct {
	APIKey string `yaml:"mp-key"`
}

func (c *Config) Load(file string) {
	if data, err := ioutil.ReadFile(file); err == nil {
		yaml.Unmarshal(data, &c)
	} else {
		fmt.Printf("[Config]Fail to load file %s", file)
	}
}
