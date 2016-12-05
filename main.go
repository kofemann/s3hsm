/* vim: set tabstop=4 softtabstop=4 shiftwidth=4 noexpandtab : */
package main

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	util "github.com/kofemann/s3hsm/util"
	"github.com/minio/minio-go"
	"io"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path"
	"time"
)

const DATA_MIME_TYPE = "binary/octet-stream"

var DEBUG = log.New(os.Stderr, "[DEBUG] ", log.LstdFlags)

func usageAndExit(app string, errcode int) {
	fmt.Println()
	fmt.Printf(" %s - use S3 and HSM for dCache", app)
	fmt.Println()
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Printf("    $ %s put <pnfsid> <path> [-key[=value]...]\n", app)
	fmt.Printf("    $ %s get <pnfsid> <path> -uri=<uri>[-key[=value]...]\n", app)
	fmt.Printf("    $ %s remove -uri=<uri> [-key[=value]...]\n", app)
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("    -debuglog=<filename>    : log debug informaion into specified file.")
	fmt.Println("    -s3config=<filename>    : path to is3 endpoint config file.")
	fmt.Println("    -s3endpoint=<host:port> : S3 endpoint's host and port")
	fmt.Println("    -s3usessl               : use https protocol when talking to S3 endpoint.")
	fmt.Println("    -s3bucket=<bucket>      : name of S3 bucket to use.")
	fmt.Println("    -s3key=<key>            : S3 AccessKey, overwrites the value from config file.")
	fmt.Println("    -s3secret=<secret>      : S3 SecretAccessKey, overwrites the value from config file.")
	fmt.Println("    -enc                    : Encrypt data with a random key before sending to S3 storage.")

	os.Exit(errcode)
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func doPut(ci *util.ConnectionParams, objectName string, filePath string, opts map[string]string) {
	minioClient, err := minio.New(ci.Endpoint, ci.AccessKey, ci.SecretKey, ci.UseSSL)
	if err != nil {
		log.Fatalln(err)
	}

	var reader io.Reader
	key := ""

	inFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open source file: %v\n", err)
	}
	defer inFile.Close()

	if ci.UseEnc {
		rand.Seed(time.Now().UnixNano())
		key = String(24)
		block, err := aes.NewCipher([]byte(key))
		if err != nil {
			log.Fatalf("Failed to initialize encryption key: %v\n", err)
		}

		var iv [aes.BlockSize]byte
		stream := cipher.NewOFB(block, iv[:])
		reader = &cipher.StreamReader{S: stream, R: inFile}
	} else {
		reader = inFile
	}

	bucketName := opts["s3bucket"]
	start := time.Now()
	_, err = minioClient.PutObject(bucketName, objectName, reader, DATA_MIME_TYPE)
	if err != nil {
		log.Fatalln(err)
	}
	DEBUG.Printf("PUT of %s done in %v\n", objectName, time.Since(start))

	u := url.URL{Scheme: "s3", Host: "s3", Path: path.Join(bucketName, objectName)}
	if len(key) > 0 {
		q := u.Query()
		q.Set("enc", key)
		u.RawQuery = q.Encode()
	}
	fmt.Println(u.String())
}

func doGet(ci *util.ConnectionParams, filePath string, opts map[string]string) {

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
	q := u.Query()
	key := q.Get("enc")

	outFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Failed to open destination file: %v\n", err)
	}
	defer outFile.Close()

	var writer io.Writer
	if len(key) > 0 {
		block, err := aes.NewCipher([]byte(key))
		if err != nil {
			log.Fatalf("Failed to initialize encryption key: %v\n", err)
		}

		var iv [aes.BlockSize]byte
		stream := cipher.NewOFB(block, iv[:])
		writer = &cipher.StreamWriter{S: stream, W: outFile}
	} else {
		writer = outFile
	}

	bucketName := path.Dir(u.Path)[1:] // strip leading slash
	objectName := path.Base(u.Path)

	start := time.Now()
	object, err := minioClient.GetObject(bucketName, objectName)
	if err != nil {
		log.Fatalf("Failed to get object back: %v\n", err)
	}

	if _, err = io.Copy(writer, object); err != nil {
		log.Fatalf("Failed to write localy: %v\n", err)
	}

	DEBUG.Printf("GET of %s done in %v\n", objectName, time.Since(start))
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

	bucketName := path.Dir(u.Path)[1:] // strip leading slash
	objectName := path.Base(u.Path)

	start := time.Now()
	err = minioClient.RemoveObject(bucketName, objectName)
	if err != nil {
		log.Fatalf("Failed to remove object: %v\n", err)
	}

	DEBUG.Printf("REMOVE of %s done in %v\n", objectName, time.Since(start))
}

func main() {

	appName := path.Base(os.Args[0])
	if len(os.Args) < 2 {
		usageAndExit(appName, 1)
	}

	action := os.Args[1]
	opts := util.Options2Map(os.Args)

	logFile, ok := opts["debuglog"]
	if ok {
		f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		defer f.Close()
		DEBUG.SetOutput(f)
	}

	connectionInfo := util.GetConnectionParams(opts)

	switch action {
	case "get":
		doGet(connectionInfo, os.Args[3], opts)
	case "put":
		doPut(connectionInfo, os.Args[2], os.Args[3], opts)
	case "remove":
		doRemove(connectionInfo, opts)
	default:
		usageAndExit(appName, 1)
	}

	os.Exit(0)
}
