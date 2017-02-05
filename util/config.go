/* vim: set tabstop=4 softtabstop=4 shiftwidth=4 noexpandtab : */
package util

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"strconv"
)

type Conf struct {
	S3  ConnectionParams
	Hsm HsmInfo
}

type ConnectionParams struct {
	Endpoint  string `yaml:"endpoint"`
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

	params := conf.S3

	// allow overwrite with option
	_, ok = opts["s3endpoint"]
	if ok {
		params.Endpoint = opts["s3endpoint"]
	}

	_, ok = opts["s3key"]
	if ok {
		params.AccessKey = opts["s3key"]
	}

	_, ok = opts["s3secret"]
	if ok {
		params.SecretKey = opts["s3secret"]
	}

	_, ok = opts["s3usessl"]
	if ok {
		params.UseSSL = true
	}

	_, ok = opts["enc"]
	if ok {
		params.UseEnc = true
	}

	_, ok = opts["trace"]
	if ok {
		params.Trace = true
	}

	_, ok = opts["s3version"]
	if ok {
		version, err := strconv.ParseUint(opts["s3version"], 10, 8)
		if err != nil {
			log.Fatalf("Invalid value for s3version: %v\n", err)
		}
		params.S3Version = version
	}

	return conf
}
