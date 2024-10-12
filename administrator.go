package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS-BANK-SIMULATOR/proto"
	"google.golang.org/grpc"
)

// Global variables to store transactions grouped by set number and alive servers for each set
var transactionSets = make(map[int][]*pb.TransactionRequest)
var liveServersMap = make(map[int][]int)
var previouslyKilledServers = make(map[int]bool) // Track previously killed servers
var golbalTransactionID = 1

// Function to set up client RPC connection
func setUPClientRPC(id int) (pb.NodeServiceClient, context.Context, *grpc.ClientConn) {
	conn, err := grpc.Dial("localhost:5005"+strconv.Itoa(id), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	c := pb.NewNodeServiceClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*60)
	return c, ctx, conn
}

func ReadTransactions(filename string) (map[int][]*pb.TransactionRequest, map[int][]int, error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	var currentSetNumber int
	var currentAliveServers []int

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		// If the first column contains the set number, we are starting a new set
		if record[0] != "" {
			// Parse the new set number
			setNumber, err := strconv.Atoi(record[0])
			if err != nil {
				fmt.Println("Skipping invalid set number:", record[0])
				continue
			}
			currentSetNumber = setNumber

			// Parse the live servers for this set from the last column
			aliveServersStr := strings.Trim(record[2], "[]")
			serverParts := strings.Split(aliveServersStr, ", ")
			currentAliveServers = []int{}
			for _, serverStr := range serverParts {
				server, err := strconv.Atoi(serverStr[1:])
				if err != nil {
					fmt.Println("Skipping invalid server:", serverStr)
					continue
				}
				currentAliveServers = append(currentAliveServers, server)
			}
			// Store the live servers for this set
			liveServersMap[currentSetNumber] = currentAliveServers
			// continue // Go to the next line for transactions
		}

		// If no set number is provided, we are still in the current set
		// Parse the transaction details (from, to, amount)
		transactionDetails := strings.Trim(record[1], "()")
		transactionParts := strings.Split(transactionDetails, ", ")
		if len(transactionParts) != 3 {
			fmt.Println("Skipping invalid transaction details:", record[1])
			continue
		}
		from, err := strconv.Atoi(transactionParts[0][1:])
		if err != nil {
			fmt.Println("Skipping invalid 'from' field:", transactionParts[0])
			continue
		}
		to, err := strconv.Atoi(transactionParts[1][1:])
		if err != nil {
			fmt.Println("Skipping invalid 'to' field:", transactionParts[1])
			continue
		}
		amount, err := strconv.Atoi(transactionParts[2])
		if err != nil {
			fmt.Println("Skipping invalid amount:", transactionParts[2])
			continue
		}

		// Add the transaction to the current set
		trans := pb.TransactionRequest{
			SetNumber: int32(currentSetNumber),
			Id:        int32(golbalTransactionID),
			From:      int32(from),
			To:        int32(to),
			Amount:    int32(amount),
		}
		fmt.Println(&trans)
		transactionSets[currentSetNumber] = append(transactionSets[currentSetNumber], &trans)
		golbalTransactionID++
	}
	return transactionSets, liveServersMap, nil
}

func main() {
	// Initialize variables to track the current set being sent and whether all sets have been read
	var currentSetNumber = 1
	var allSetsRead = false

	// Infinite loop for continuously taking user input
	for {
		// Display menu options
		fmt.Println("\nSelect an option:")
		fmt.Println("1. Read Transaction SET on Default Path")
		fmt.Println("2. Send Transactions (Next Set)")
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

		// Handle user input with a switch-case
		switch option {
		case 1:
			// Read the transactions from the default CSV file path
			fmt.Println("Executing: Read Transaction SET on Default Path")
			transactionSets, liveServersMap, err = ReadTransactions("sample.csv")
			if err != nil {
				fmt.Printf("Error reading transactions: %v\n", err)
				continue
			}
			allSetsRead = true
			currentSetNumber = 1 // Reset to the first set after reading
			fmt.Println("Transactions successfully read.")

		case 2:
			// Send transactions from the current set number
			if !allSetsRead {
				fmt.Println("No transactions have been read. Please choose option 1 first.")
				continue
			}
			if transactions, ok := transactionSets[currentSetNumber]; ok {
				fmt.Printf("Processing Set %d\n", currentSetNumber)

				// Get the alive servers for the current set
				aliveServers := liveServersMap[currentSetNumber]
				aliveServerSet := make(map[int]bool)
				for _, server := range aliveServers {
					aliveServerSet[server] = true
				}

				// Revive servers that were previously killed but are now alive
				for serverID := range previouslyKilledServers {
					if previouslyKilledServers[serverID] && aliveServerSet[serverID] {
						fmt.Printf("Reviving Server %d for this set\n", serverID)
						// // Placeholder for Revive logic, you will implement it later
						c, ctx, conn := setUPClientRPC(serverID)
						ack, err := c.Revive(ctx, &pb.ReviveRequest{NodeID: int32(serverID)})
						if err != nil {
							log.Fatalf("Could not revive: %v", err)
						}
						log.Printf("Revive Command Sent: %s for Server %d", ack, serverID)
						conn.Close()
						// Mark as no longer killed
						previouslyKilledServers[serverID] = false
					}
				}

				// Kill servers that are not alive for the current set
				for i := 1; i <= 5; i++ {
					if _, isAlive := aliveServerSet[i]; !isAlive {
						fmt.Printf("Killing Server %d for this set\n", i)
						c, ctx, conn := setUPClientRPC(i)
						r, err := c.Kill(ctx, &pb.AdminRequest{Command: "Die and Perish"})
						if err != nil {
							log.Fatalf("Could not kill: %v", err)
						}
						log.Printf("Kill Command Sent: %s for Server %d", r.Ack, i)
						conn.Close()
						// Track that this server is now killed
						previouslyKilledServers[i] = true
					}
				}

				// Now send transactions to all servers as usual
				for _, tran := range transactions {
					// Set up RPC connection and send the transaction to all servers (including killed ones)
					c, ctx, conn := setUPClientRPC(int(tran.From))
					r, err := c.AcceptTransactions(ctx, tran)
					if err != nil {
						log.Fatalf("Could not send transaction: %v", err)
					}
					log.Printf("Transaction Sent: %s", r.Ack)
					conn.Close()
				}

				currentSetNumber++ // Move to the next set
			} else {
				fmt.Println("No more sets to send.")
			}

		case 3:
			// Example for killing nodes
			c, ctx, conn := setUPClientRPC(1)
			r, err := c.Kill(ctx, &pb.AdminRequest{Command: "Die and Perish"})
			if err != nil {
				log.Fatalf("Could not kill: %v", err)
			}
			log.Printf("Kill Command Sent: %s", r.Ack)
			conn.Close()

		case 4:
			// Example for printing balances from each node
			for i := 1; i <= 5; i++ {
				c, ctx, conn := setUPClientRPC(i)
				r, err := c.GetBalance(ctx, &pb.AdminRequest{Command: "Requesting Balance"})
				if err != nil {
					log.Fatalf("Could not retrieve balance: %v", err)
				}
				log.Println("Server", r.NodeID, "Balance:", r.Balance)
				conn.Close()
			}

		case 5:
			// Placeholder for spawning all nodes
			fmt.Println("Executing: Spawn All Nodes")
			// Add node spawning logic here if required

		case 6:
			// Exit the program
			fmt.Println("Exiting program...")
			os.Exit(0)

		default:
			// Invalid option handling
			fmt.Println("Invalid option. Please choose a valid number.")
		}
	}
}
