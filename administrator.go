package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"

	// "node"
	"os"
	"strconv"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS-BANK-SIMULATOR/proto"
	"google.golang.org/grpc"
)

type Transaction struct {
	SetNumber int
	From      int
	To        int
	Amount    int
}

var transactions []Transaction

func setUPClientRPC(id int) (pb.NodeServiceClient, context.Context, *grpc.ClientConn) {

	conn, err := grpc.Dial("localhost:5005"+strconv.Itoa(id), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	// defer conn.Close()
	c := pb.NewNodeServiceClient(conn)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	// defer cancel()
	return c, ctx, conn
}
func ReadTransactions(filename string) ([]Transaction, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		setNumber, err := strconv.Atoi(record[0])
		if err != nil {
			return nil, err
		}

		amount, err := strconv.Atoi(record[3])
		if err != nil {
			return nil, err
		}

		From, err := strconv.Atoi(record[1])
		if err != nil {
			return nil, err
		}

		To, err := strconv.Atoi(record[2])
		if err != nil {
			return nil, err
		}

		transaction := Transaction{
			SetNumber: setNumber,
			From:      From,
			To:        To,
			Amount:    amount,
		}
		fmt.Println(transaction)
		transactions = append(transactions, transaction)
	}
	return transactions, nil
}

func main() {
	// var nodes []*node.Node
	// Infinite loop for continuously taking user input
	for {
		// Display menu options
		fmt.Println("\nSelect an option:")
		fmt.Println("1. Read Transaction SET on Default Path")
		fmt.Println("2. Send Transactions")
		fmt.Println("3. Kill Nodes")
		fmt.Println("4. Print Balance on Specific Server")
		fmt.Println("5. Spawn All Nodes")
		fmt.Println("6. Exit")

		// Read user's choice
		var option int
		fmt.Print("Enter your option: ")
		_, err := fmt.Scanln(&option)
		if err != nil {
			fmt.Println("Invalid input, please enter a number.")
			continue
		}

		// Use a switch-case to handle different options
		switch option {
		case 1:
			fmt.Println("Executing: Read Transaction SET on Default Path")
			// Call your function for this option here
			ReadTransactions("input.csv")
		case 2:
			fmt.Println("Executing: Send Transaction SET on respective servers")
			// Call your function for this option here
			for _, tran := range transactions {
				c, ctx, conn := setUPClientRPC(tran.From)
				r, err := c.AcceptTransactions(ctx, &pb.TransactionRequest{
					SetNumber: int32(tran.SetNumber),
					From:      int32(tran.From),
					To:        int32(tran.To),
					Amount:    int32(tran.Amount),
				})
				if err != nil {
					log.Fatalf("Could not add: %v", err)
				}
				log.Printf(r.Ack)
				conn.Close()
			}
		case 3:

			c, ctx, conn := setUPClientRPC(1)
			r, err := c.Kill(ctx, &pb.AdminRequest{Command: "Die and Perish"})
			if err != nil {
				log.Fatalf("Could not add: %v", err)
			}
			log.Printf(r.Ack)
			conn.Close()
		case 4:
			// fmt.Println("Executing: Print Balance on Specific Server")
			for i := 1; i < 5; i++ {
				c, ctx, conn := setUPClientRPC(i)
				r, err := c.GetBalance(ctx, &pb.AdminRequest{Command: "Die and Perish"})
				if err != nil {
					log.Fatalf("Could not add: %v", err)
				}
				log.Println(" Server ", r.NodeID, " Has Balance : ", r.Balance)
				conn.Close()
			}
		case 5:
			fmt.Println("Executing: Spawn All Nodes")
			// Call your function for this option here
			// for i := 0; i < 5; i++ {
			// 	nodes = append(nodes, node.Spawn(i))
			// }
		case 6:
			fmt.Println("Exiting program...")
			os.Exit(0)
		default:
			fmt.Println("Invalid option. Please choose a valid number.")
		}
	}
}
