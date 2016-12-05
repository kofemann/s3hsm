/* vim: set tabstop=4 softtabstop=4 shiftwidth=4 noexpandtab : */
package util

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Conf struct {
	S3 ConnectionParams
}

type ConnectionParams struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	UseSSL    bool   `yaml:"ssl"`
	UseEnc    bool   `yaml:"enc"`
}

func GetConnectionParams(opts map[string]string) *ConnectionParams {

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

	return &params

}
