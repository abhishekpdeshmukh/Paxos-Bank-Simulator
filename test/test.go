package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

type Transaction struct {
	ID          int
	SetNumber   int
	FromAccount int
	ToAccount   int
	Amount      int
}

func ReadTransactions(nodeID int) ([]Transaction, error) {

	db, err := sql.Open("sqlite3", fmt.Sprintf("../node/node_%d.db", nodeID))
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, set_number, from_account, to_account, amount FROM transactions")
	if err != nil {
		return nil, fmt.Errorf("error querying transactions: %v", err)
	}
	defer rows.Close()

	var transactions []Transaction

	for rows.Next() {
		var tx Transaction
		if err := rows.Scan(&tx.ID, &tx.SetNumber, &tx.FromAccount, &tx.ToAccount, &tx.Amount); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		transactions = append(transactions, tx)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return transactions, nil
}

func main() {

	nodeID := 1

	transactions, err := ReadTransactions(nodeID)
	if err != nil {
		log.Fatalf("Failed to read transactions: %v", err)
	}

	fmt.Printf("Transactions for Node %d:\n", nodeID)
	fmt.Println("ID | SetNumber | FromAccount | ToAccount | Amount")
	fmt.Println("-----------------------------------------------")
	for _, tx := range transactions {
		fmt.Printf("%d | %d | %d | %d | %d\n", tx.ID, tx.SetNumber, tx.FromAccount, tx.ToAccount, tx.Amount)
	}
}
