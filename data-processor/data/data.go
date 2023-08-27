package data

import (
	"context"
	"io"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ryanc414/dynamodbav"
)

const taxesPaidTableName = "taxes_paid"

type JoinedData struct {
	ObjectReference string `dynamodbav:"object_reference"`
	*Client
	Portfolios []*Portfolio `dynamodbav:"portfolios"`
	Accounts   []*Account   `dynamodbav:"accounts"`
}

type DataManager struct {
	S3Client  *s3.Client
	db        *dynamodb.Client
	tableName string
}

// DownloadFile gets an object from a bucket and stores it in a local file.
func (d DataManager) DownloadFile(bucketName string, objectKey string) ([]byte, error) {
	result, err := d.S3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Printf("Couldn't get object %v:%v. Error: %v\n", bucketName, objectKey, err)
		return []byte{}, err
	}
	defer result.Body.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Couldn't read object body from %v. Error: %v\n", objectKey, err)
	}
	return body, err
}

func (d DataManager) GetTaxesPaidByClient(clientReference string) (int, error) {
	var taxesPaid int

	return taxesPaid, nil
}

func (d DataManager) InsertClient(client Client) error {
	joined := JoinedData{
		ObjectReference: client.ClientReference,
		Client:          &client,
	}

	// Check if there are already portfolios for this client
	// This might happen because the portfolio file was processed before the client file

	portfolios, err := d.db.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		KeyConditionExpression: aws.String("object_reference = :client_reference"),
		FilterExpression:       aws.String("portfolio_reference <> :null"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":client_reference": &types.AttributeValueMemberS{Value: client.ClientReference},
			":null":             &types.AttributeValueMemberNULL{Value: true},
		},
	})
	if err != nil {
		log.Printf("Couldn't query portfolios for client %v. Error: %v\n", client.ClientReference, err)
		return err
	}
	if len(portfolios.Items) > 0 {
		for _, item := range portfolios.Items {
			var portfolio Portfolio
			err = attributevalue.UnmarshalMap(item, &portfolio)
			if err != nil {
				log.Printf("Couldn't unmarshal portfolio: %v. Error: %v\n", item, err)
				return err
			}
			joined.Portfolios = append(joined.Portfolios, &portfolio)
		}
	}

	marshalled, err := dynamodbav.MarshalItem(joined)
	if err != nil {
		log.Printf("Couldn't marshal joined data: %v. Error: %v\n", joined, err)
		return err
	}

	out, err := d.db.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      marshalled,
	})
	if err != nil {
		log.Printf("Couldn't insert client: %v. Error: %v\n", client, err)
		return err
	}
	log.Printf("Inserted client: %v\n", out)

	return nil
}

func (d DataManager) InsertPortfolio(portfolio Portfolio) error {
	joined := JoinedData{
		ObjectReference: portfolio.ClientReference,
		Portfolios: []*Portfolio{
			&portfolio,
		},
	}

	// Check if there already is a client for this portfolio
	// This would normally be the case unless the portfolio file got processed before the client file

	clients, err := d.db.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		KeyConditionExpression: aws.String("object_reference = :client_reference"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":client_reference": &types.AttributeValueMemberS{Value: portfolio.ClientReference},
		},
	})
	if err != nil {
		log.Printf("Couldn't query clients for portfolio %v. Error: %v\n", portfolio.PortfolioReference, err)
		return err
	}

	// Check if there already is an account for this portfolio. This would be the case if the account file was processed before the portfolio file
	result, err := d.db.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"object_reference": &types.AttributeValueMemberS{Value: strconv.Itoa(portfolio.AccountNumber)},
		},
	})
	if err != nil {
		log.Printf("Couldn't query accounts for portfolio %v. Error: %v\n", portfolio.PortfolioReference, err)
		return err
	}
	if result.Item != nil {
		var account Account
		err = attributevalue.UnmarshalMap(result.Item, &account)
		if err != nil {
			log.Printf("Couldn't unmarshal account: %v. Error: %v\n", result.Item, err)
			return err
		}
		joined.Accounts = append(joined.Accounts, &account)
	}

	joined.Client = &Client{ClientReference: portfolio.ClientReference}
	if len(clients.Items) > 0 {
		joined.Client = &Client{}
		err = attributevalue.UnmarshalMap(clients.Items[0], joined.Client)
		if err != nil {
			log.Printf("Couldn't unmarshal client: %v. Error: %v\n", clients.Items[0], err)
			return err
		}
	}

	marshalled, err := dynamodbav.MarshalItem(joined)
	if err != nil {
		log.Printf("Couldn't marshal joined data: %v. Error: %v\n", joined, err)
		return err
	}

	out, err := d.db.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      marshalled,
	})
	if err != nil {
		log.Printf("Couldn't insert portfolio: %v. Error: %v\n", portfolio, err)
		return err
	}
	log.Printf("Inserted portfolio: %v\n", out)

	return nil
}

func (d DataManager) InsertAccount(account Account) error {
	joined := JoinedData{
		ObjectReference: strconv.Itoa(account.AccountNumber),
		Accounts: []*Account{
			&account,
		},
	}

	// Check if there alread is a portfolio for this account

	portfolios, err := d.db.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		KeyConditionExpression: aws.String("account_number = :account_number"),
		FilterExpression:       aws.String("portfolio_reference <> :null"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":account_number": &types.AttributeValueMemberS{Value: strconv.Itoa(account.AccountNumber)},
			":null":           &types.AttributeValueMemberNULL{Value: true},
		},
	})
	if err != nil {
		log.Printf("Couldn't query portfolios for account %v. Error: %v\n", account.AccountNumber, err)
		return err
	}
	if len(portfolios.Items) > 0 {
		for _, item := range portfolios.Items {
			var portfolio Portfolio
			err = attributevalue.UnmarshalMap(item, &portfolio)
			if err != nil {
				log.Printf("Couldn't unmarshal portfolio: %v. Error: %v\n", item, err)
				return err
			}
			joined.Portfolios = append(joined.Portfolios, &portfolio)
			joined.ObjectReference = portfolio.ClientReference
		}
	}

	// Check if there are already transactions for this account
	// This might happen because the transaction file was processed before the account file

	transactions, err := d.db.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(d.tableName),
		KeyConditionExpression: aws.String("object_reference = :account_number"),
		FilterExpression:       aws.String("transaction_reference <> :null"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":account_number": &types.AttributeValueMemberS{Value: strconv.Itoa(account.AccountNumber)},
			":null":           &types.AttributeValueMemberNULL{Value: true},
		},
	})
	if err != nil {
		log.Printf("Couldn't query transactions for account %v. Error: %v\n", account.AccountNumber, err)
		return err
	}

	for _, item := range transactions.Items {
		var transaction Transaction
		err = attributevalue.UnmarshalMap(item, &transaction)
		if err != nil {
			log.Printf("Couldn't unmarshal transaction: %v. Error: %v\n", item, err)
			return err
		}
		account.Transactions = append(account.Transactions, &transaction)
	}

	marshalled, err := dynamodbav.MarshalItem(joined)
	if err != nil {
		log.Printf("Couldn't marshal joined data: %v. Error: %v\n", joined, err)
		return err
	}

	out, err := d.db.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      marshalled,
	})
	if err != nil {
		log.Printf("Couldn't insert account: %v. Error: %v\n", account, err)
		return err
	}
	log.Printf("Inserted account: %v\n", out)

	return nil
}

func (d DataManager) InsertTransaction(transaction Transaction) error {
	// Check if there already is an account for this transaction
	// This would be the case if the account file was processed before the transaction file and is the normal case

	result, err := d.db.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"account_number": &types.AttributeValueMemberS{Value: strconv.Itoa(transaction.AccountNumber)},
		},
	})
	if err != nil {
		log.Printf("Couldn't query accounts for transaction %v. Error: %v\n", transaction.TransactionReference, err)
		return err
	}
	if result.Item != nil {
		var account Account
		err = attributevalue.UnmarshalMap(result.Item, &account)
		if err != nil {
			log.Printf("Couldn't unmarshal account: %v. Error: %v\n", result.Item, err)
			return err
		}
		account.Transactions = append(account.Transactions, &transaction)
		marshalled, err := dynamodbav.MarshalItem(account)
		if err != nil {
			log.Printf("Couldn't marshal account: %v. Error: %v\n", account, err)
			return err
		}
		out, err := d.db.PutItem(context.TODO(), &dynamodb.PutItemInput{
			TableName: aws.String(d.tableName),
			Item:      marshalled,
		})
		if err != nil {
			log.Printf("Couldn't insert account: %v. Error: %v\n", account, err)
			return err
		}
		log.Printf("Inserted account: %v\n", out)
	} else {

		out, err := d.db.PutItem(context.TODO(), &dynamodb.PutItemInput{
			TableName: aws.String(d.tableName),
			Item: map[string]types.AttributeValue{
				"object_reference":      &types.AttributeValueMemberS{Value: transaction.TransactionReference},
				"account_number":        &types.AttributeValueMemberN{Value: strconv.Itoa(transaction.AccountNumber)},
				"transaction_reference": &types.AttributeValueMemberS{Value: transaction.TransactionReference},
				"amount":                &types.AttributeValueMemberN{Value: strconv.FormatFloat(transaction.Amount, 'f', 2, 64)},
				"keyword":               &types.AttributeValueMemberS{Value: transaction.Keyword},
			},
		})
		if err != nil {
			log.Printf("Couldn't insert transaction: %v. Error: %v\n", transaction, err)
			return err
		}
		log.Printf("Inserted transaction: %v\n", out)
	}

	return nil
}

func (d DataManager) SendErrorEvent(err error) error {
	return nil
}

func NewDataManager(s3Client *s3.Client, dbClient *dynamodb.Client, tableName string) *DataManager {
	return &DataManager{
		S3Client:  s3Client,
		db:        dbClient,
		tableName: tableName,
	}
}
