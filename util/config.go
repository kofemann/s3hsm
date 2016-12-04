/* vim: set tabstop=4 softtabstop=4 shiftwidth=4 noexpandtab : */
package util

type ConnectionParams struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
	UseEnc    bool
}

func GetConnectionParams(opts map[string]string) *ConnectionParams {

	params := &ConnectionParams{}
	params.Endpoint = opts["s3endpoint"]
	params.AccessKey = opts["s3key"]
	params.SecretKey = opts["s3secret"]
	_, params.UseSSL = opts["s3usessl"]
	_, params.UseEnc = opts["enc"]
	return params

}
