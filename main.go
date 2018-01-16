package main

import (
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/spf13/pflag"
)

func main() {
	var bucketName string
	var bucketPrefix string
	var bucketRegion string
	var secretKey string
	var outputFile string

	pflag.StringVar(&bucketName, "bucket-name", "", "Name of the bucket")
	pflag.StringVar(&bucketPrefix, "bucket-prefix", "", "Bucket prefix")
	pflag.StringVar(&bucketRegion, "bucket-region", "", "Bucket region")
	pflag.StringVar(&secretKey, "secret-key", "", "Key to secret in bucket")
	pflag.StringVar(&outputFile, "output-file", "", "Path to which to write")

	pflag.Parse()

	if bucketName == "" {
		log.Fatalf("--bucket-name is required")
	}
	if bucketRegion == "" {
		log.Fatalf("--bucket-name is required")
	}
	if secretKey == "" {
		log.Fatalf("--secret-key is required")
	}
	if outputFile == "" {
		log.Fatalf("--output-file is required")
	}

	awsSession, err := session.NewSession()
	if err != nil {
		log.Fatalf("Cannot create session: %s", err)
	}
	awsSession.Config.Region = aws.String(bucketRegion)

	s3client := NewS3(awsSession, bucketName, bucketPrefix, "", 10*1024*1024)

	resp, err := s3client.GetEncryptedObject(secretKey)
	if err != nil {
		log.Fatalf("Cannot get object: %s", err)
	}

	err = ioutil.WriteFile(outputFile, resp, 0600)
	if err != nil {
		log.Fatalf("Cannot write file %q: %s", outputFile, err)
	}
}
