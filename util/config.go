/* vim: set tabstop=4 softtabstop=4 shiftwidth=4 noexpandtab : */
package util

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Conf struct {
	S3  ConnectionParams
	Hsm HsmInfo
}

type ConnectionParams struct {
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	UseSSL    bool   `yaml:"ssl"`
	UseEnc    bool   `yaml:"enc"`
	Trace     bool   `yaml:"trace"`
	S3Version uint64 `yaml:"s3version"`
}

type HsmInfo struct {
	Instance string `yaml:"instance"`
	Type     string `yaml:"type"`
}

func GetConfig(opts map[string]string) *Conf {

	conf := &Conf{}

	// use config file if provided
	file, ok := opts["s3config"]
	if ok {
		yamlFile, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatalf("Failed to read config file: %v\n", err)
		}

		err = yaml.Unmarshal(yamlFile, conf)
		if err != nil {
			log.Fatalf("Failed to parse config: %v\n", err)
		}

	}

	return conf
}
