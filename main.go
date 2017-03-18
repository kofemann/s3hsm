/* vim: set tabstop=4 softtabstop=4 shiftwidth=4 noexpandtab : */
package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	util "github.com/kofemann/s3hsm/util"
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

func doPut(ci *util.ConnectionParams, hsm *util.HsmInfo, objectName string, filePath string, opts map[string]string) {
	s3client, err := connect(ci)
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

	uploader := s3manager.NewUploaderWithClient(s3client)

	start := time.Now()

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(objectName),
		ContentType: aws.String(DATA_MIME_TYPE),
		Body:        reader,
	})

	if err != nil {
		log.Fatalln(err)
	}
	DEBUG.Printf("PUT of %s done in %v\n", objectName, time.Since(start))

	u := url.URL{Scheme: hsm.Type, Host: hsm.Instance, Path: path.Join(bucketName, objectName)}
	if len(key) > 0 {
		q := u.Query()
		q.Set("etype", "aes")
		q.Set("ekey", hex.EncodeToString(key))
		u.RawQuery = q.Encode()
	}
	fmt.Println(u.String())
}

func doGet(ci *util.ConnectionParams, filePath string, opts map[string]string) {

	s3client, err := connect(ci)
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
	resp, err := s3client.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectName),
		})

	if err != nil {
		log.Fatalf("Failed to get object back: %v\n", err)
	}

	if _, err = io.Copy(writer, resp.Body); err != nil {
		log.Fatalf("Failed to write localy: %v\n", err)
	}

	DEBUG.Printf("GET of %s done in %v\n", objectName, time.Since(start))
}

func doRemove(ci *util.ConnectionParams, opts map[string]string) {

	s3client, err := connect(ci)
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
	_, err = s3client.DeleteObject(
		&s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectName),
		})
	if err != nil {
		log.Fatalf("Failed to remove object: %v\n", err)
	}

	DEBUG.Printf("REMOVE of %s done in %v\n", objectName, time.Since(start))
}

func connect(ci *util.ConnectionParams) (*s3.S3, error) {

	// Get Config
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(ci.AccessKey, ci.SecretKey, ""),
		Endpoint:         aws.String(ci.Endpoint),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(!ci.UseSSL),
		S3ForcePathStyle: aws.Bool(true), // reqired for minio server
	}

	if ci.Trace {
		s3Config.WithLogLevel(aws.LogDebugWithRequestErrors | aws.LogDebugWithHTTPBody | aws.LogDebugWithSigning | aws.LogDebugWithRequestRetries)
	}

	// Create Session
	newSession, err := session.NewSession(s3Config)
	if err != nil {
		return nil, err
	}

	// Create S3 Client
	return s3.New(newSession), nil
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

	config := util.GetConfig(opts)
	connectionInfo := &config.S3
	hsmInfo := &config.Hsm

	switch action {
	case "get":
		doGet(connectionInfo, os.Args[3], opts)
	case "put":
		doPut(connectionInfo, hsmInfo, os.Args[2], os.Args[3], opts)
	case "remove":
		doRemove(connectionInfo, opts)
	default:
		usageAndExit(appName, 1)
	}

	os.Exit(0)
}
