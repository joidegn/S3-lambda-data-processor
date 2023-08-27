package main

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

func TestHandler(t *testing.T) {

	var tests = []struct {
		name  string
		input events.S3Event
		want  string
	}{
		{
			name: "test1",
			input: events.S3Event{
				Records: []events.S3EventRecord{
					{
						S3: events.S3Entity{
							Bucket: events.S3Bucket{
								Name: "test-bucket",
								Arn:  "test-arn",
							},
							Object: events.S3Object{
								Key: "test-key",
							},
						},
						EventVersion: "2.1",
						EventSource:  "aws:s3",
						AWSRegion:    "eu-central-1",
						EventTime:    time.Now(),
						EventName:    "ObjectCreated:Put",
						PrincipalID: events.S3UserIdentity{
							PrincipalID: "AWS:<some-principal-id>",
						},
						RequestParameters: events.S3RequestParameters{
							SourceIPAddress: "<some-ip-address>",
						},
						ResponseElements: map[string]string{
							"x-amz-id-2":    "<some-id>",
							"x-amz-request": "<some-request>",
						},
					},
				},
			},
			want: "Processed object uploaded to bucket test-bucket with key test-key",
		}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lambdaHandler(context.Background(), tt.input)
			if err != nil {
				t.Errorf("Handler() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("Handler() = %v, want %v", got, tt.want)
			}
		})
	}

}
