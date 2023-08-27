package data

import (
	"bytes"
	"encoding/csv"

	"github.com/gocarina/gocsv"
)

type Client struct {
	RecordID         int     `dynamodbav:"record_id" csv:"record_id"`
	FirstName        string  `dynamodbav:"first_name" csv:"first_name"`
	LastName         string  `dynamodbav:"last_name" csv:"last_name"`
	ClientReference  string  `dynamodbav:"client_reference" csv:"client_reference"`
	TaxFreeAllowance float64 `dynamodbav:"tax_free_allowance" csv:"tax_free_allowance"`
}

type Portfolio struct {
	RecordID           int    `dynamodbav:"record_id" csv:"record_id"`
	AccountNumber      int    `dynamodbav:"account_number" csv:"account_number"`
	PortfolioReference string `dynamodbav:"portfolio_reference" csv:"portfolio_reference"`
	ClientReference    string `dynamodbav:"client_reference" csv:"client_reference"`
	AgentCode          string `dynamodbav:"agent_code" csv:"agent_code"`
}

type Account struct {
	RecordID      int            `dynamodbav:"record_id" csv:"record_id"`
	AccountNumber int            `dynamodbav:"account_number" csv:"account_number"`
	CashBalance   float64        `dynamodbav:"cash_balance" csv:"cash_balance"`
	Currency      string         `dynamodbav:"currency" csv:"currency"`
	TaxesPaid     float64        `dynamodbav:"taxes_paid" csv:"taxes_paid"`
	Transactions  []*Transaction `dynamodbav:"transactions"`
	Balance       float64        `dynamodbav:"balance" csv:"-"`
}

type Transaction struct {
	RecordID             int     `dynamodbav:"record_id" csv:"record_id"`
	AccountNumber        int     `dynamodbav:"account_number" csv:"account_number"`
	TransactionReference string  `dynamodbav:"transaction_reference" csv:"transaction_reference"`
	Amount               float64 `dynamodbav:"amount" csv:"amount"`
	Keyword              string  `dynamodbav:"keyword" csv:"keyword"`
}

func ParseClientCSV(data []byte) ([]*Client, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	clients := []*Client{}
	err := gocsv.UnmarshalCSV(reader, &clients)
	return clients, err
}

func ParsePortfolioCSV(data []byte) ([]*Portfolio, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	portfolios := []*Portfolio{}
	err := gocsv.UnmarshalCSV(reader, &portfolios)
	return portfolios, err
}

func ParseAccountCSV(data []byte) ([]*Account, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	accounts := []*Account{}
	err := gocsv.UnmarshalCSV(reader, &accounts)
	return accounts, err
}
func ParseTransactionCSV(data []byte) ([]*Transaction, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	transactions := []*Transaction{}
	err := gocsv.UnmarshalCSV(reader, &transactions)
	return transactions, err
}
