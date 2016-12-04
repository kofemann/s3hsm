/* vim: set tabstop=2 softtabstop=2 shiftwidth=2 noexpandtab : */
package main

import (
	"fmt"
	util "github.com/kofemann/s3hsm/util"
	"github.com/minio/minio-go"
	"log"
	"net/url"
	"os"
)

const DATA_MIME_TYPE = "binary/octet-stream"

func usageAndExit(app string, errcode int) {
	fmt.Printf("Usage:\n")
	fmt.Printf("    %s put <pnfsid> <path> [-key[=value]...]\n", app)
	fmt.Printf("    %s get <pnfsid> <path> -uri=<uri>[-key[=value]...]\n", app)
	fmt.Printf("    %s remove -uri=<uri> [-key[=value]...]\n", app)
	os.Exit(errcode)
}

func doPut(ci *util.ConnectionParams, objectName string, path string, opts map[string]string) {
	minioClient, err := minio.New(ci.Endpoint, ci.AccessKey, ci.SecretKey, ci.UseSSL)
	if err != nil {
		log.Fatalln(err)
	}

	bucketName := opts["s3bucket"]
	_, err = minioClient.FPutObject(bucketName, objectName, path, DATA_MIME_TYPE)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("s3://%s/%s", bucketName, objectName)
}

func doGet(ci *util.ConnectionParams, path string, opts map[string]string) {

	minioClient, err := minio.New(ci.Endpoint, ci.AccessKey, ci.SecretKey, ci.UseSSL)
	if err != nil {
		log.Fatalln(err)
	}

	s3uri, ok := opts["uri"]
	if !ok {
		log.Fatalln("uri is missing")
	}

	u, err := url.Parse(s3uri)
	if err != nil {
		log.Fatalf("Failed to parse URI %s:  %v\n", s3uri, err)
	}

	bucketName := u.Host
	// strip leading slash
	objectName := u.Path[1:]

	err = minioClient.FGetObject(bucketName, objectName, path)
	if err != nil {
		log.Fatalf("Failed to get object back: %v\n", err)
	}
}

func doRemove(ci *util.ConnectionParams, opts map[string]string) {

	minioClient, err := minio.New(ci.Endpoint, ci.AccessKey, ci.SecretKey, ci.UseSSL)
	if err != nil {
		log.Fatalln(err)
	}

	s3uri, ok := opts["uri"]
	if !ok {
		log.Fatalln("uri is missing")
	}

	u, err := url.Parse(s3uri)
	if err != nil {
		log.Fatalf("Failed to parse URI %s:  %v\n", s3uri, err)
	}

	bucketName := u.Host
	// strip leading slash
	objectName := u.Path[1:]

	err = minioClient.RemoveObject(bucketName, objectName)
	if err != nil {
		log.Fatalf("Failed to remove object: %v\n", err)
	}

}

func main() {

	if len(os.Args) < 2 {
		usageAndExit(os.Args[0], 1)
	}

	action := os.Args[1]
	opts := util.Options2Map(os.Args)
	connectionInfo := util.GetConnectionParams(opts)

	switch action {
	case "get":
		doGet(connectionInfo, os.Args[3], opts)
	case "put":
		doPut(connectionInfo, os.Args[2], os.Args[3], opts)
	case "remove":
		doRemove(connectionInfo, opts)
	default:
		usageAndExit(os.Args[0], 1)
	}

	os.Exit(0)
}
