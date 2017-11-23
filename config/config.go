package config

import (
	"fmt"
	"io/ioutil"
	"path"

	yaml "gopkg.in/yaml.v2"
)

// config is the data model for deserialization the config yaml file

type Config struct {
	MP MP
	DB DB
}

type DB struct {
	Address  string `yaml:"db-addr"`
	Username string `yaml:"db-user"`
	Password string `yaml:"db-pass"`
}

type MP struct {
	APIKey   string `yaml:"mp-key"`
	APIToken string `yaml:"mp-token"`
}

type Environment int

const (
	OneBox = Environment(iota)
	Dev
	Prod
)

func NewConfig(dir string, env Environment) *Config {
	var file string
	switch env {
	case OneBox:
		file = path.Join(dir, "1box.yaml")
	case Dev:
		file = path.Join(dir, "dev.yaml")
	case Prod:
		file = path.Join(dir, "prod.yaml")
	}

	if len(file) == 0 {
		return nil
	}

	c := &Config{}
	c.Load(file)
	return c
}

func (c *Config) Load(file string) error {
	if data, err := ioutil.ReadFile(file); err == nil {
		yaml.Unmarshal(data, &c)
		return nil
	} else {
		return fmt.Errorf("[Config]Fail to load file %s", file)
	}
}
