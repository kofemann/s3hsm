/* vim: set tabstop=4 softtabstop=4 shiftwidth=4 noexpandtab : */
package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	util "github.com/kofemann/s3hsm/util"
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
	fmt.Printf("    $ %s put <pnfsid> <path>\n", app)
	fmt.Printf("    $ %s get <pnfsid> <path> -uri=<uri>\n", app)
	fmt.Printf("    $ %s remove -uri=<uri>\n", app)
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("    -debuglog=<filename>    : log debug information into specified file.")
	fmt.Println("    -s3bucket=<bucket>      : name of S3 bucket to use.")
	fmt.Println("    -s3config=<filename>    : path to s3 endpoint config file.")
	fmt.Println()
	fmt.Println("HSM fault injection:")
	fmt.Println("    -sleep=<seconds>        : delay request by specified seconds.")
	fmt.Println("    -fault=<error>          : fail request with specified error code")
	os.Exit(errcode)
}

func doPut(ci *util.ConnectionParams, hsm *util.HsmInfo, objectName string, filePath string, bucketName string) {
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

func doGet(ci *util.ConnectionParams, filePath string, s3uri string) {

	s3client, err := connect(ci)
	if err != nil {
		log.Fatalln(err)
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

func doRemove(ci *util.ConnectionParams, s3uri string) {

	s3client, err := connect(ci)
	if err != nil {
		log.Fatalln(err)
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
		Region:           aws.String(ci.Region),
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
	s3client := s3.New(newSession)
	switch ci.S3Version {
	case 2:
		util.Setv2Handlers(s3client)
	case 4:
		// nop
	default:
		log.Fatalf("Unsupported protocol version [%d]\n", ci.S3Version)
	}

	return s3client, nil
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

	configFile, ok := opts["s3config"]
	if !ok {
		usageAndExit(appName, 2)
	}

	config := util.GetConfig(configFile)
	connectionInfo := &config.S3
	hsmInfo := &config.Hsm

	// fault injections: inject errors and delays

	s, ok := opts["sleep"]
	if ok {
		t, err := strconv.Atoi(s)
		if err != nil {
			log.Fatalf("Bad value for sleep: %v\n", err)
		}
		time.Sleep(time.Duration(t) * time.Second)
	}

	s, ok = opts["fail"]
	if ok {
		rc, err := strconv.Atoi(s)
		if err != nil {
			log.Fatalf("Bad value for sleep: %v\n", err)
		}
		DEBUG.Printf("failure injection: error code %d\n", rc)
		os.Exit(rc)
	}

	switch action {
	case "get":
		s3uri, ok := opts["uri"]
		if !ok {
			log.Fatalln("uri is missing")
		}
		doGet(connectionInfo, os.Args[3], s3uri)
	case "put":
		bucketName, ok := opts["s3bucket"]
		if !ok {
			log.Fatalln("s3bucket is missing")
		}
		doPut(connectionInfo, hsmInfo, os.Args[2], os.Args[3], bucketName)
	case "remove":
		s3uri, ok := opts["uri"]
		if !ok {
			log.Fatalln("uri is missing")
		}
		doRemove(connectionInfo, s3uri)
	default:
		usageAndExit(appName, 1)
	}

	os.Exit(0)
}
