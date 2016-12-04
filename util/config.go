/* vim: set tabstop=2 softtabstop=2 shiftwidth=2 noexpandtab : */
package util

type ConnectionParams struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

func GetConnectionParams(opts map[string]string) *ConnectionParams {

	params := &ConnectionParams{}
	params.Endpoint = opts["s3endpoint"]
	params.AccessKey = opts["s3key"]
	params.SecretKey = opts["s3secret"]
	_, params.UseSSL = opts["s3usessl"]
	return params

}
