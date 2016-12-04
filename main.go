/* vim: set tabstop=2 softtabstop=2 shiftwidth=2 noexpandtab : */
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
	"time"
)

const DATA_MIME_TYPE = "binary/octet-stream"

func usageAndExit(app string, errcode int) {
	fmt.Printf("Usage:\n")
	fmt.Printf("    %s put <pnfsid> <path> [-key[=value]...]\n", app)
	fmt.Printf("    %s get <pnfsid> <path> -uri=<uri>[-key[=value]...]\n", app)
	fmt.Printf("    %s remove -uri=<uri> [-key[=value]...]\n", app)
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

func doPut(ci *util.ConnectionParams, objectName string, path string, opts map[string]string) {
	minioClient, err := minio.New(ci.Endpoint, ci.AccessKey, ci.SecretKey, ci.UseSSL)
	if err != nil {
		log.Fatalln(err)
	}

	var reader io.Reader
	key := ""

	inFile, err := os.Open(path)
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
	_, err = minioClient.PutObject(bucketName, objectName, reader, DATA_MIME_TYPE)
	if err != nil {
		log.Fatalln(err)
	}

	u := url.URL{Scheme: "s3", Host: bucketName, Path: objectName}
	if len(key) > 0 {
		q := u.Query()
		q.Set("enc", key)
		u.RawQuery = q.Encode()
	}
	fmt.Println(u.String())
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
	q := u.Query()
	key := q.Get("enc")

	outFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
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

	bucketName := u.Host
	// strip leading slash
	objectName := u.Path[1:]

	object, err := minioClient.GetObject(bucketName, objectName)
	if err != nil {
		log.Fatalf("Failed to get object back: %v\n", err)
	}

	if _, err = io.Copy(writer, object); err != nil {
		log.Fatalf("Failed to write localy: %v\n", err)
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
