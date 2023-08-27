package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/joidegn/scalable-capital/data-processor/data"
)

type Event struct {
	Records []Record `json:"records"`
}

type Record struct {
	S3 S3 `json:"s3`
}

type S3 struct {
	Bucket Bucket `json:"bucket"`
	Object Object `json:"object"`
}

type Bucket struct {
	Name string
}

type Object struct {
	Key string
}

type handler struct {
	d *data.DataManager
}

func (h handler) handleEvent(ctx context.Context, event events.S3Event) (string, error) {
	log.Printf("Event: %+v", event)
	eventJson, _ := json.Marshal(event)
	var data Event

	json.Unmarshal(eventJson, &data)
	bucket := data.Records[0].S3.Bucket.Name
	key := data.Records[0].S3.Object.Key

	fileType := strings.Split(key, "_")[0] // The file type is determined by the file name up until the first underscore.
	log.Printf("File type: %s", fileType)

	fileContent, err := h.d.DownloadFile(bucket, key)
	if err != nil {
		log.Printf("Error fetching file: %s", err)
		return "", err
	}

	log.Printf("File content: %s", fileContent)

	err = h.processFile(fileType, fileContent)
	if err != nil {
		log.Printf("Error processing file: %s", err)
		h.d.SendErrorEvent(err)
		return "", err
	}

	msg := fmt.Sprintf("Processed object uploaded to bucket %s with key %s", bucket, key)
	log.Print(msg)

	return msg, nil
}

func (h handler) processFile(fileType string, fileContent []byte) error {
	var err error
	switch fileType {
	case "clients":
		log.Printf("Processing clients file")
		err = h.processClientFile(fileContent)
	case "portfolios":
		log.Printf("Processing portfolios file")
		err = h.processPortfolioFile(fileContent)
	case "accounts":
		log.Printf("Processing accounts file")
		err = h.processAccountsFile(fileContent)
	case "transactions":
		log.Printf("Processing transactions file")
		h.processTransactionsFile(fileContent)
	default:
		log.Printf("Unknown file type")
		err = fmt.Errorf("unknown file type")
	}
	return err
}

func (h handler) processClientFile(fileContent []byte) error {
	clients, err := data.ParseClientCSV(fileContent)
	if err != nil {
		log.Printf("Error parsing clients file: %s", err)
		return err
	}
	log.Printf("Clients: %+v", clients)
	for _, client := range clients {
		err = h.d.InsertClient(*client)
		if err != nil {
			log.Printf("Error inserting client: %s", err)
			return err
		}

		// Collect data for client message
		//taxesPaid, err := h.d.GetTaxesPaidByClient(client.ClientReference)
		//if err != nil {
		//	log.Printf("Error getting taxes paid: %s", err)
		//	return err
		//}

	}

	return nil
}

func (h handler) processPortfolioFile(fileContent []byte) error {
	portfolios, err := data.ParsePortfolioCSV(fileContent)
	if err != nil {
		log.Printf("Error parsing portfolios file: %s", err)
		return err
	}
	log.Printf("Portfolios: %+v", portfolios)
	for _, portfolio := range portfolios {
		err = h.d.InsertPortfolio(*portfolio)
		if err != nil {
			log.Printf("Error inserting portfolio: %s", err)
			return err
		}
	}
	return nil
}

func (h handler) processAccountsFile(fileContent []byte) error {
	accounts, err := data.ParseAccountCSV(fileContent)
	if err != nil {
		log.Printf("Error parsing accounts file: %s", err)
		return err
	}
	log.Printf("Accounts: %+v", accounts)
	for _, account := range accounts {
		err = h.d.InsertAccount(*account)
		if err != nil {
			log.Printf("Error inserting account: %s", err)
			return err
		}
	}
	return nil
}

func (h handler) processTransactionsFile(fileContent []byte) error {
	transactions, err := data.ParseTransactionCSV(fileContent)
	if err != nil {
		log.Printf("Error parsing transactions file: %s", err)
		return err
	}
	log.Printf("Transactions: %+v", transactions)
	for _, transaction := range transactions {
		err = h.d.InsertTransaction(*transaction)
		if err != nil {
			log.Printf("Error inserting transaction: %s", err)
			return err
		}
	}
	return nil
}
