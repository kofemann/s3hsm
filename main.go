/* vim: set tabstop=4 softtabstop=4 shiftwidth=4 noexpandtab : */
package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
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
const KEY_SIZE = 32 // 256bit

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

func doPut(ci *util.ConnectionParams, objectName string, filePath string, opts map[string]string) {
	minioClient, err := connect(ci)
	if err != nil {
		log.Fatalln(err)
	}

	var reader io.Reader
	var key []byte

	inFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open source file: %v\n", err)
	}
	defer inFile.Close()

	if ci.UseEnc {
		rand.Seed(time.Now().UnixNano())
		key = make([]byte, KEY_SIZE)
		_, err = rand.Read(key)
		if err != nil {
			log.Fatalf("Failed to generate random key: %v\n", err)
		}

		block, err := aes.NewCipher(key)
		if err != nil {
			log.Fatalf("Failed to initialize encryption key: %v\n", err)
		}

		// as we use a key per file, the IV can be zero.
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
		q.Set("etype", "aes")
		q.Set("ekey", hex.EncodeToString(key))
		u.RawQuery = q.Encode()
	}
	fmt.Println(u.String())
}

func doGet(ci *util.ConnectionParams, filePath string, opts map[string]string) {

	minioClient, err := connect(ci)
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
	key := q.Get("ekey")
	etype := q.Get("etype")

	outFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatalf("Failed to open destination file: %v\n", err)
	}
	defer outFile.Close()

	var writer io.Writer
	if len(key) > 0 {
		if etype != "aes" {
			log.Fatalf("Unsupported encryption type: %s\n", etype)
		}

		rawKey, err := hex.DecodeString(key)
		if err != nil {
			log.Fatalf("Failed to decode encryption key: %v\n", err)
		}

		block, err := aes.NewCipher(rawKey)
		if err != nil {
			log.Fatalf("Failed to initialize encryption key: %v\n", err)
		}

		// as we use a key per file, the IV can be zero.
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

	minioClient, err := connect(ci)
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

func connect(ci *util.ConnectionParams) (*minio.Client, error) {

	var client *minio.Client
	var err error

	switch ci.S3Version {
	case 2:
		client, err = minio.NewV2(ci.Endpoint, ci.AccessKey, ci.SecretKey, ci.UseSSL)
	case 4:
		client, err = minio.NewV4(ci.Endpoint, ci.AccessKey, ci.SecretKey, ci.UseSSL)
	default:
		log.Fatal("Unsupported protocol version")
	}

	if err != nil {
		return client, err
	}

	if ci.Trace {
		client.TraceOn(nil)
	} else {
		client.TraceOff()
	}

	return client, nil
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
