package config

import (
	"io/ioutil"
	"path/filepath"

	yaml "gopkg.in/yaml.v2"
)

const (
	filePath = "./config/config.yaml"
)

type configuration struct {
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

var Config configuration

func init() {
	var fileName string
	var yamlFile []byte
	var err error

	if fileName, err = filepath.Abs(filePath); err != nil {
		panic(err)
	}

	if yamlFile, err = ioutil.ReadFile(fileName); err != nil {
		panic(err)
	}
	Config = configuration{}
	if err = yaml.Unmarshal(yamlFile, &Config); err != nil {
		panic(err)
	}

}
