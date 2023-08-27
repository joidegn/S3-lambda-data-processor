package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joidegn/scalable-capital/data-processor/data"
	_ "github.com/lib/pq"
)

func main() {
	s3Client, err := NewS3Client()
	if err != nil {
		log.Fatalf("unable to create S3 client, %v", err)
		panic(err) // TODO: Better error handling and logging e.g. using Zap
	}
	dbClient, err := NewDynamoDbClient()
	if err != nil {
		log.Fatalf("unable to create database connection, %v", err)
		panic(err) // TODO: Better error handling and logging e.g. using Zap
	}

	// tableName := os.Getenv("DYNAMODB_TABLE_NAME")  // TODO: Get from secrets manager
	tableName := "DataProcessorStack-databaseEBDE4557-NO1O8XI3QQDI" // TODO: Get from secrets manager
	d := data.NewDataManager(s3Client, dbClient, tableName)

	h := handler{
		d: d,
	}

	lambda.Start(h.handleEvent)
}

func NewS3Client() (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("eu-central-1"),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
		return nil, err
	}

	return s3.NewFromConfig(cfg), nil
}

func NewDynamoDbClient() (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
		return nil, err
	}
	dbClient := dynamodb.NewFromConfig(cfg)

	return dbClient, nil
}
