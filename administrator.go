package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS-BANK-SIMULATOR/proto"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
)

type Transaction struct {
	ID          int
	SetNumber   int
	FromAccount int
	ToAccount   int
	Amount      int
}

var transactionSets = make(map[int][]*pb.TransactionRequest)
var liveServersMap = make(map[int][]int)
var previouslyKilledServers = make(map[int]bool) // Track previously killed servers
var golbalTransactionID = 1

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
		// If the first column contains the set number, starting a new set
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

var serverMutexMap = make(map[int]*sync.Mutex)
var serverMutexMapLock sync.Mutex

func getServerMutex(serverID int) *sync.Mutex {
	serverMutexMapLock.Lock()
	defer serverMutexMapLock.Unlock()

	if _, exists := serverMutexMap[serverID]; !exists {
		serverMutexMap[serverID] = &sync.Mutex{}
	}
	return serverMutexMap[serverID]
}
func ReadDBTransactions(nodeID int) error {

	db, err := sql.Open("sqlite3", fmt.Sprintf("node/node_%d.db", nodeID))
	if err != nil {
		return fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, set_number, from_account, to_account, amount FROM transactions")
	if err != nil {
		return fmt.Errorf("error querying transactions: %v", err)
	}
	defer rows.Close()

	var transactions []Transaction

	for rows.Next() {
		var tx Transaction
		if err := rows.Scan(&tx.ID, &tx.SetNumber, &tx.FromAccount, &tx.ToAccount, &tx.Amount); err != nil {
			return fmt.Errorf("error scanning row: %v", err)
		}
		transactions = append(transactions, tx)
	}

	if err := rows.Err(); err != nil {
		return err
	}
	fmt.Printf("Transactions for Node %d:\n", nodeID)
	fmt.Printf("%-10s | %-10s | %-12s | %-12s | %-10s\n", "ID", "SetNumber", "FromAccount", "ToAccount", "Amount")
	fmt.Println("---------------------------------------------------------------")
	for _, tx := range transactions {
		fmt.Printf("%-10d | %-10d | %-12d | %-12d | %-10d\n", tx.ID, tx.SetNumber, tx.FromAccount, tx.ToAccount, tx.Amount)
	}

	// return transactions, nil
	return nil
}
func main() {

	var currentSetNumber = 1
	var allSetsRead = false

	for {

		fmt.Println("\nSelect an option:")
		fmt.Println("1. Read Transaction SET on Default Path")
		fmt.Println("2. Send Transactions (Next Set)")
		fmt.Println("3. PrintLog of a given Server")
		fmt.Println("4. Print Balance on Specific Server")
		fmt.Println("5. Print DB of a Specific Server")
		fmt.Println("6. BONUS Print Balance")
		fmt.Println("7. Print Latency of a server")
		fmt.Println("8. Exit")

		var option int
		fmt.Print("Enter your option: ")
		_, err := fmt.Scanln(&option)
		if err != nil {
			fmt.Println("Invalid input, please enter a number.")
			continue
		}

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
				var wg2 sync.WaitGroup
				// wg2.Add(len(previouslyKilledServers))
				for serverID := range previouslyKilledServers {
					wg2.Add(1)
					if previouslyKilledServers[serverID] && aliveServerSet[serverID] {
						fmt.Printf("Reviving Server %d for this set\n", serverID)

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
					wg2.Done()
				}
				wg2.Wait()
				// Kill servers that are not alive for the current set
				wg2.Add(5)
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
					wg2.Done()
				}
				wg2.Wait()

				// wg.Add(len(transactions))
				sort.Slice(transactions, func(i, j int) bool {
					if transactions[i].From == transactions[j].From {
						return transactions[i].Id < transactions[j].Id
					}
					return false
				})
				fmt.Println("Current transactions Order")
				fmt.Println(transactions)
				for _, tran := range transactions {
					wg2.Add(1)
					serverID := int(tran.From)
					serverMutex := getServerMutex(serverID)
					go func(tran *pb.TransactionRequest, serverMutex *sync.Mutex) {
						serverMutex.Lock()
						defer serverMutex.Unlock()

						c, ctx, conn := setUPClientRPC(serverID)
						defer conn.Close()
						fmt.Println("Sending Transaction ", tran)
						r, err := c.AcceptTransactions(ctx, tran)
						if err != nil {
							log.Printf("Could not send transaction: %v", err)
							return
						}
						log.Printf("Transaction Sent from Server %d: %s", serverID, r.Ack)

					}(tran, serverMutex)
					time.Sleep(2 * time.Millisecond)
					wg2.Done()
				}
				wg2.Wait()
				fmt.Println("Still Waiting")

				// time.Sleep(400 * time.Millisecond)
				fmt.Println("Done Waiting")
				currentSetNumber++
			} else {
				fmt.Println("No more sets to send.")
			}

		case 3:
			var option int
			fmt.Print("Enter the Server for which you want to print local logs ")
			_, err := fmt.Scanln(&option)
			if err != nil {
				fmt.Println("Invalid input, please enter a number.")
				continue
			}
			c, ctx, conn := setUPClientRPC(option)
			r, err := c.PrintLogs(ctx, &pb.PrintLogRequest{})
			if err != nil {
				log.Fatalf("Could not retrieve balance: %v", err)
			}
			fmt.Printf("Below are the logs of Server %d:\n", option)
			fmt.Printf("%-10s | %-10s | %-12s | %-12s | %-10s\n", "ID", "SetNumber", "FromAccount", "ToAccount", "Amount")
			fmt.Println("---------------------------------------------------------------")
			for _, tx := range r.Logs {
				fmt.Printf("%-10d | %-10d | S%-11d | S%-11d | %-10d\n", tx.Id, tx.SetNumber, tx.From, tx.To, tx.Amount)
			}

			conn.Close()

		case 4:

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

			var option int
			fmt.Println("Which server db do you want to read")
			_, err := fmt.Scanln(&option)
			if err != nil {
				fmt.Println("Invalid input, please enter a number.")
				continue
			}
			fmt.Println("Executing: Read Database of a specific node")
			ReadDBTransactions(option)

		case 6:
			var serverID int

			fmt.Print("Enter the Server ID (1-5): ")
			fmt.Scanln(&serverID)

			balance := getBalanceForSpecificServer(serverID, currentSetNumber)
			fmt.Printf("Current balance for server %d: %d\n", serverID, balance)

		case 7:
			var option int
			fmt.Println("Which Server Latency Do you want to see")
			_, err := fmt.Scanln(&option)
			if err != nil {
				fmt.Println("Invalid input, please enter a number.")
				continue
			}
			c, ctx, conn := setUPClientRPC(option)
			latencies, err := c.GetLatencies(ctx, &pb.EmptyRequest{})
			if err != nil {
				fmt.Println("Error fetching latencies:", err)
				continue
			}

			fmt.Println("Transaction Latency Details:")
			for _, record := range latencies.Latencies {
				fmt.Printf("Transaction ID: %d, From: %d, To: %d, Amount: %d, Latency: %s\n",
					record.Id, record.From, record.To, record.Amount, record.Latency)
			}

			if len(latencies.Latencies) > 0 {

				totalLatency := time.Duration(0)
				var totalThroughput float64 = 0
				for _, record := range latencies.Latencies {
					latencyDuration, _ := time.ParseDuration(record.Latency)
					totalLatency += latencyDuration
					latencyInSeconds := latencyDuration.Seconds()
					if latencyInSeconds > 0 {
						throughputPerTransaction := 1 / latencyInSeconds
						fmt.Printf("Throughput : %.2f transactions per second\n", throughputPerTransaction)
						totalThroughput += throughputPerTransaction
					}
				}
				avgLatency := totalLatency / time.Duration(len(latencies.Latencies))
				fmt.Printf("\nAverage Latency: %v\n", avgLatency)
				fmt.Printf("Total Throughput: %.2f transactions per second\n", totalThroughput)
			} else {
				fmt.Println("\nNo transactions to calculate average latency.")
			}

			conn.Close()

		case 8:
			fmt.Println("Exiting program...")
			os.Exit(0)
		default:

			fmt.Println("Invalid option. Please choose a valid number.")
		}
	}
}

func getBalanceForSpecificServer(serverID int, currentSetNumber int) int {
	// A set to track processed transaction IDs (to avoid duplicates)
	processedTxnIds := make(map[int]bool)

	initialBalance := 100
	finalBalance := initialBalance
	var longestCommittedLog []*pb.TransactionRequest

	// Get the list of alive servers for the current set
	aliveServers := liveServersMap[currentSetNumber]

	// Iterate over all alive servers to find the most up-to-date committed log
	for _, aliveServerID := range aliveServers {

		c, ctx, conn := setUPClientRPC(aliveServerID)
		defer conn.Close()

		response, err := c.PrintLogs(ctx, &pb.PrintLogRequest{})
		if err != nil {
			fmt.Printf("Error fetching logs from server %d: %v\n", aliveServerID, err)
			continue
		}

		// Checking if this server has the longest committed log
		if len(response.CommitedLogs) > len(longestCommittedLog) {
			longestCommittedLog = response.CommitedLogs
		}
	}

	// Apply the longest committed log
	for _, txn := range longestCommittedLog {

		if (txn.From == int32(serverID) || txn.To == int32(serverID)) && !processedTxnIds[int(txn.Id)] {
			finalBalance = applyTransaction(finalBalance, txn, serverID)
			processedTxnIds[int(txn.Id)] = true
		}
	}

	for _, aliveServerID := range aliveServers {

		c, ctx, conn := setUPClientRPC(aliveServerID)
		defer conn.Close()

		response, err := c.PrintLogs(ctx, &pb.PrintLogRequest{})
		if err != nil {
			fmt.Printf("Error fetching logs from server %d: %v\n", aliveServerID, err)
			continue
		}

		for _, txn := range response.Logs {
			if (txn.From == int32(serverID) || txn.To == int32(serverID)) && !processedTxnIds[int(txn.Id)] {
				finalBalance = applyTransaction(finalBalance, txn, serverID)
				processedTxnIds[int(txn.Id)] = true
			}
		}
	}

	return finalBalance
}

func applyTransaction(balance int, txn *pb.TransactionRequest, clientId int) int {

	if txn.From == int32(clientId) {
		balance -= int(txn.Amount)
	} else if txn.To == int32(clientId) {
		balance += int(txn.Amount)
	}
	return balance
}
