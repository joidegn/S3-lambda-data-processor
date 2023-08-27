package main

import (
	"os"
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsdynamodb"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3notifications"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type DataProcessorStackProps struct {
	awscdk.StackProps
}

func NewDataProcessorStack(scope constructs.Construct, id string, props *DataProcessorStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, &id, &sprops)

	dir, _ := os.Getwd()

	ecr_image := awslambda.EcrImageCode_FromAssetImage(jsii.String(filepath.Join(dir, "data-processor")),
		&awslambda.AssetImageCodeProps{},
	)

	// Create data processing lambda

	dataProcessor := awslambda.NewFunction(stack, jsii.String("lambdaFromContainer"), &awslambda.FunctionProps{
		Code:         ecr_image,
		Handler:      awslambda.Handler_FROM_IMAGE(),
		Runtime:      awslambda.Runtime_FROM_IMAGE(),
		FunctionName: jsii.String("dataProcessor"),
		Timeout:      awscdk.Duration_Seconds(jsii.Number(30)),
	})

	// Create s3 bucket and event notification.

	s3 := awss3.NewBucket(stack, jsii.String("s3bucket"), &awss3.BucketProps{})

	notification := awss3notifications.NewLambdaDestination(dataProcessor)

	s3.AddEventNotification(awss3.EventType_OBJECT_CREATED, notification)

	s3.GrantRead(dataProcessor, nil) // grant the lambda role read access to the bucket

	// Create Dynamodb database

	table := awsdynamodb.NewTable(stack, jsii.String("database"), &awsdynamodb.TableProps{
		PartitionKey: &awsdynamodb.Attribute{
			Name: jsii.String("object_reference"),
			Type: awsdynamodb.AttributeType_STRING,
		},
	})

	// Grant the lambda role access to the database

	statement := awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Effect:    awsiam.Effect_ALLOW,
		Actions:   jsii.Strings("dynamodb:*"),
		Resources: jsii.Strings("*"),
	})
	dataProcessor.AddToRolePolicy(statement)

	// log lambda function ARN
	awscdk.NewCfnOutput(stack, jsii.String("lambdaFunctionArn"), &awscdk.CfnOutputProps{
		Value:       dataProcessor.FunctionArn(),
		Description: jsii.String("Lambda function ARN"),
	})

	// log s3 bucket ARN
	awscdk.NewCfnOutput(stack, jsii.String("s3BucketArn"), &awscdk.CfnOutputProps{
		Value:       s3.BucketArn(),
		Description: jsii.String("s3 bucket ARN"),
	})

	// log dynamodb ARN and table
	awscdk.NewCfnOutput(stack, jsii.String("database-table-arn"),
		&awscdk.CfnOutputProps{
			ExportName: jsii.String("database-arn"),
			Value:      table.TableArn()})
	awscdk.NewCfnOutput(stack, jsii.String("database-table-name"),
		&awscdk.CfnOutputProps{
			ExportName: jsii.String("database-name"),
			Value:      table.TableName()})

	return stack
}

func main() {
	defer jsii.Close()

	app := awscdk.NewApp(nil)

	NewDataProcessorStack(app, "DataProcessorStack", &DataProcessorStackProps{
		awscdk.StackProps{
			Env: env(),
		},
	})

	app.Synth(nil)
}

// env determines the AWS environment (account+region) in which our stack is to
// be deployed. For more information see: https://docs.aws.amazon.com/cdk/latest/guide/environments.html
func env() *awscdk.Environment {
	// If unspecified, this stack will be "environment-agnostic".
	// Account/Region-dependent features and context lookups will not work, but a
	// single synthesized template can be deployed anywhere.
	//---------------------------------------------------------------------------
	// return nil

	// Uncomment if you know exactly what account and region you want to deploy
	// the stack to. This is the recommendation for production stacks.
	//---------------------------------------------------------------------------
	// return &awscdk.Environment{
	//  Account: jsii.String("123456789012"),
	//  Region:  jsii.String("us-east-1"),
	// }

	// Uncomment to specialize this stack for the AWS Account and Region that are
	// implied by the current CLI configuration. This is recommended for dev
	// stacks.
	//---------------------------------------------------------------------------
	return &awscdk.Environment{
		Account: jsii.String(os.Getenv("CDK_DEFAULT_ACCOUNT")),
		Region:  jsii.String(os.Getenv("CDK_DEFAULT_REGION")),
	}
}
