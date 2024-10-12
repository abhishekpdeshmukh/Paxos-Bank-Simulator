package main

import (
	"database/sql"
	"flag"
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

// ReadTransactions reads transactions from the specified node's database.
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

// DropTables drops all tables for a given node's database.
func DropTables(nodeID int) error {
	db, err := sql.Open("sqlite3", fmt.Sprintf("../node/node_%d.db", nodeID))
	if err != nil {
		return fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("DROP TABLE IF EXISTS transactions")
	if err != nil {
		return fmt.Errorf("error dropping tables: %v", err)
	}

	return nil
}

func main() {
	
	clearFlag := flag.Bool("clear", false, "Clear all transactions from all node databases")
	nodeIDFlag := flag.Int("node", -1, "Specify the node ID to read transactions from")

	flag.Parse()

	if *clearFlag {
		// Loop through all nodes (adjust the range as necessary)
		for i := 1; i <= 5; i++ {
			fmt.Printf("Dropping tables for Node %d...\n", i)
			if err := DropTables(i); err != nil {
				log.Fatalf("Failed to drop tables for Node %d: %v", i, err)
			}
		}
		fmt.Println("All tables dropped successfully.")
		return
	}

	if *nodeIDFlag != -1 {
		transactions, err := ReadTransactions(*nodeIDFlag)
		if err != nil {
			log.Fatalf("Failed to read transactions: %v", err)
		}

		fmt.Printf("Transactions for Node %d:\n", *nodeIDFlag)
		fmt.Println("ID | SetNumber | FromAccount | ToAccount | Amount")
		fmt.Println("-----------------------------------------------")
		for _, tx := range transactions {
			fmt.Printf("%d | %d | %d | %d | %d\n", tx.ID, tx.SetNumber, tx.FromAccount, tx.ToAccount, tx.Amount)
		}
	} else {
		fmt.Println("Please provide a valid node ID using the -node flag.")
		flag.Usage()
	}
}
