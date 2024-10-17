package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS-BANK-SIMULATOR/proto"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
)

var Majority int = 3

type Node struct {
	pb.UnimplementedNodeServiceServer
	pb.UnimplementedPaxosServiceServer
	nodeID                int32
	isActive              bool
	myBalance             int32
	currBallotNum         int32
	isPromised            bool
	promisedBallot        *pb.Ballot
	transactionLog        []*pb.TransactionRequest
	megaBlock             []*pb.TransactionRequest
	AcceptedMegaBlock     []*pb.TransactionRequest
	CommittedTransactions []*pb.TransactionRequest
	highestTransIDSent    int32
	lastCommittedIndex    int32
	db                    *sql.DB
	lock                  sync.Mutex
	transactionQueue      chan *pb.TransactionRequest
}

func (s *Node) AcceptTransactions(ctx context.Context, req *pb.TransactionRequest) (*pb.NodeResponse, error) {
	fmt.Println("Received transaction request:", req)
	s.transactionQueue <- req // Push the transaction into the channel
	return &pb.NodeResponse{Ack: "Transaction added to the queue"}, nil
}

func processTransaction(s *Node) {
	for req := range s.transactionQueue {
		// req := s.transactionQueue[0]
		// s.transactionQueue = s.transactionQueue[1:]
		fmt.Println("Attempting To accept ", req)

		if s.myBalance-int32(req.Amount) < 0 && s.isActive {
			var flag bool = true
			for {
				s.currBallotNum += 1
				fmt.Println("Inside Infinite Loop")
				fmt.Println("Initiate consensus")
				var wg sync.WaitGroup
				wg.Add(5)
				tempBalance := 0
				fail := 0
				var successCount, failCount, respondedCount int // Track successful promises and failures
				var highestBallotSeen int32 = s.currBallotNum   // Track highest ballot seen
				var majorityReached bool = false                // Flag for majority reached
				var mu sync.Mutex                               // Mutex for synchronizing access to shared variables

				for i := 1; i <= 5; i++ {
					go func(i int, s *Node) {
						if i != int(s.nodeID) {
							fmt.Println("Inside Prepare")
							c, ctx, conn := SetupNodeRpcReciever(i)
							defer conn.Close()

							// Make Prepare request
							promise, err := c.Prepare(ctx, &pb.PrepareRequest{
								Ballot: &pb.Ballot{
									BallotNum: s.currBallotNum,
									NodeID:    s.nodeID,
								},
							})

							mu.Lock() // Lock before modifying shared variables

							if err != nil {
								if ctx.Err() == context.DeadlineExceeded {
									// Timeout error, consider node inactive for this round
									fmt.Println("Prepare request to node", i, "timed out")
								} else {
									// Handle other RPC errors (node down, etc.)
									fmt.Println("Error in Prepare RPC:", err)
								}
								failCount++
							} else if promise.BallotNumber.BallotNum > s.currBallotNum {
								// Node rejected due to seeing a higher ballot number
								fmt.Println("Rejected due to higher ballot from node", i, "with ballot", promise.BallotNumber.BallotNum)
								if promise.BallotNumber.BallotNum > highestBallotSeen {
									highestBallotSeen = promise.BallotNumber.BallotNum // Track highest ballot
								}
								failCount++
							} else {
								// Successfully got promise, process it
								fmt.Println("Promise From ", i)
								fmt.Println(promise)
								s.lock.Lock()
								for _, transaction := range promise.Accept_Val {
									if transaction.To == s.nodeID {
										tempBalance += int(transaction.Amount)
									}
								}
								for _, transaction := range promise.Local {
									if transaction.To == s.nodeID {
										tempBalance += int(transaction.Amount)
									}
								}
								s.megaBlock = append(s.megaBlock, promise.Accept_Val...)
								s.megaBlock = append(s.megaBlock, promise.Local...)
								s.lock.Unlock()

								successCount++ // Increment successful promise count
							}

							respondedCount++ // Increment total responses received (success or fail)

							if successCount >= 3 && !majorityReached {
								fmt.Println("Majority reached, but waiting for remaining nodes until timeout")
								majorityReached = true // Mark that we have the majority
							}

							mu.Unlock() // Unlock after modifying shared variables

						}
						wg.Done()
					}(i, s)
				}

				wg.Wait() // Wait for all responses

				// Check if we have at least a majority of successful promises
				if successCount >= 2 {
					fmt.Println("Proceeding to Accept phase in PAXOS")
					// Continue to Accept phase with the gathered promises
				} else {
					fmt.Println("Majority not reached, retrying with higher ballot")
					s.currBallotNum = highestBallotSeen + 1 // Update ballot number and retry Paxos
					s.megaBlock = nil
					time.Sleep(3 * time.Second)
					continue // Retry with the new ballot number
				}

				s.lock.Lock()

				// Step 1: Prepare the megaBlock
				wg.Add(5)
				s.megaBlock = append(s.megaBlock, s.transactionLog...) // Add existing transaction log

				// Use tempBalance, which has already accumulated the balance from previous transactions
				if flag && int(s.myBalance)+tempBalance >= int(req.Amount) {
					s.megaBlock = append(s.megaBlock, req)
					flag = false
				}

				fmt.Println("MY MEGA BLOCK IS ", s.megaBlock)
				fail = 0
				successCount = 0

				s.lock.Unlock() // Unlock after preparing the megaBlock

				// Step 2: Send Accept Requests
				for i := 1; i <= 5; i++ {
					go func(i int) {
						if i != int(s.nodeID) {
							fmt.Println("Inside Accept of PAXOS")
							c, ctx, conn := SetupNodeRpcReciever(i)
							defer conn.Close()

							accepted, err := c.Accept(ctx, &pb.AcceptRequest{
								ProposalNumber: s.currBallotNum,
								Value:          s.megaBlock,
							})
							if err != nil {
								if ctx.Err() == context.DeadlineExceeded {
									fmt.Println("Accept request to node", i, "timed out")
								} else {
									fmt.Println("Error in Accept RPC:", err)
								}
								mu.Lock()
								fail++
								mu.Unlock()
							} else {
								mu.Lock()
								successCount++
								mu.Unlock()
								fmt.Println(accepted)
							}

						}
						wg.Done()
					}(i)
				}

				// Wait for responses
				wg.Wait()

				// Step 3: Check if majority (3) has responded positively
				if successCount >= 2 {
					fmt.Println("Majority reached, proceed to Commit phase")
					// s.lock.Lock()
					// Now update the balance since consensus has been reached
					s.myBalance += int32(tempBalance) // Use tempBalance to update the node's actual balance
					tempBalance = 0                   // Reset temporary balance after updating
					// s.lock.Unlock()
				} else {
					fmt.Println("Majority not reached, retrying Paxos")
					s.megaBlock = nil
					fmt.Println("Current leader log after failing paxos ", s.transactionLog)
					time.Sleep(3 * time.Second) // Retry after a delay
					// You can retry Paxos here, or handle it as per your logic

					continue
				}
				s.lock.Lock()
				wg.Add(5)
				for i := 1; i <= 5; i++ {
					go func(i int) {

						if i != int(s.nodeID) {
							fmt.Println("Inside Commit of PAXOS")
							c, ctx, conn := SetupNodeRpcReciever(i)
							ack, err := c.Commit(ctx, &pb.CommitRequest{
								BallotNumber: &pb.Ballot{
									BallotNum: s.currBallotNum,
									NodeID:    s.nodeID,
								},
							})
							if err != nil {
								// log.Fatalf("Could not add: %v", err)

								// return
							}

							fmt.Println(ack)
							conn.Close()
						}
						wg.Done()
					}(i)
				}
				wg.Wait()
				fmt.Println("Hello After wait going to commit now")
				s.CommittedTransactions = append(s.CommittedTransactions, s.megaBlock...)
				if len(s.megaBlock) != 0 {
					err := s.commitTransactions(true) // Commit transactions to the database
					if err != nil {
						fmt.Println("Error while commiting")
						// return nil, err
					}
				}
				fmt.Println("Before setting megaBlock and transaction  to nil")
				s.megaBlock = nil
				s.transactionLog = nil
				fmt.Println(s.megaBlock)
				fmt.Println("After setting megaBlock to nil")

				if s.myBalance-int32(req.Amount) >= 0 {
					s.myBalance -= req.Amount
					// s.transactionLog = append(s.transactionLog, req)
					fmt.Println("This my transaction log ", s.transactionLog)
					fmt.Println("Escaping Paxos")
					s.lock.Unlock()
					fmt.Println("My megablock is ", s.megaBlock)
					break
				}
				s.lock.Unlock()
				time.Sleep(3 * time.Second)
			}
		} else {
			s.lock.Lock()
			fmt.Println("My mega block ", s.megaBlock)
			fmt.Println("My transactional log before ", s.transactionLog)
			if s.myBalance-int32(req.Amount) >= 0 {
				s.myBalance -= req.Amount
				s.transactionLog = append(s.transactionLog, req)
				fmt.Println("This my transaction log ", s.transactionLog)
			}

			s.lock.Unlock()
		}
		// return &pb.NodeResponse{Ack: "Node " + strconv.Itoa(int(s.nodeID)) + " Took All transactions"}, nil

	}
	// Get the next transaction from the queue

}
func (s *Node) Kill(ctx context.Context, req *pb.AdminRequest) (*pb.NodeResponse, error) {
	println(req.Command)
	if s.isActive {
		s.isActive = false
		return &pb.NodeResponse{Ack: "I wont respond consider me dead"}, nil
	} else {
		return &pb.NodeResponse{Ack: "I am already Dead why are you trying to kill  me again"}, nil
	}
}

func (s *Node) Revive(ctx context.Context, req *pb.ReviveRequest) (*pb.ReviveResponse, error) {
	s.lock.Lock()
	s.isActive = true
	lastCommittedIndex := s.lastCommittedIndex // Use the highest transaction ID committed
	s.lock.Unlock()
	fmt.Println("Last Commited Index of ", s.nodeID, " is ", lastCommittedIndex)
	var wg sync.WaitGroup
	var mu sync.Mutex

	highestBlockLength := 0
	var highestBlock []*pb.TransactionRequest

	// Send RPC to all other nodes to request their commit blocks
	for i := 1; i <= 5; i++ { // Assuming 5 nodes in the system
		if i == int(s.nodeID) {
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			c, ctx, conn := SetupNodeRpcReciever(i)
			defer conn.Close()

			// Send the last committed index with the request
			response, err := c.RequestCommitBlock(ctx, &pb.CommitBlockRequest{
				NodeId:             s.nodeID,
				LastCommittedIndex: lastCommittedIndex,
			})
			if err != nil {
				fmt.Printf("Failed to get commit block from node %d: %v\n", i, err)
				return
			}

			mu.Lock()
			if response.CommitBlockLength > int32(highestBlockLength) {
				highestBlockLength = int(response.CommitBlockLength)
				highestBlock = response.CommittedTransactions
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	fmt.Println("Sending Highest Block ", highestBlock)
	// Now commit the highest block to the revived node
	s.lock.Lock()
	s.commitTransactionsFromBlock(highestBlock)
	s.lock.Unlock()

	return &pb.ReviveResponse{
		Success: true,
	}, nil
}
func (s *Node) commitTransactionsFromBlock(block []*pb.TransactionRequest) error {
	// Apply only the transactions that come after the last committed index
	var temp []*pb.TransactionRequest
	for _, transaction := range block {
		if transaction.Id > s.lastCommittedIndex {
			// Apply the transaction logic (update balance, etc.)
			if transaction.To == s.nodeID {
				s.myBalance += transaction.Amount
			}
			temp = append(temp, transaction)
			// Update the highest transaction ID committed
			// s.lastCommittedIndex = transaction.Id
		}
	}
	// fmt.Println("Filter Block is")
	// fmt.Println(temp)
	if len(temp) == 0 {
		return nil
	}
	s.AcceptedMegaBlock = temp
	s.commitTransactions(false) // Commit to the database
	s.AcceptedMegaBlock = nil
	return nil
}
func (s *Node) RequestCommitBlock(ctx context.Context, req *pb.CommitBlockRequest) (*pb.CommitBlockResponse, error) {
	fmt.Println("Got A request from ", req.NodeId)
	fmt.Println("Sending ", s.CommittedTransactions)
	return &pb.CommitBlockResponse{
		CommitBlockLength:     int32(len(s.CommittedTransactions)),
		CommittedTransactions: s.CommittedTransactions,
	}, nil
}
func (s *Node) GetBalance(ctx context.Context, req *pb.AdminRequest) (*pb.BalanceResponse, error) {
	println(req.Command)
	// s.lock.Lock()
	// defer s.lock.Unlock()
	// if s.isActive {

	// } else {
	// 	return
	// }
	return &pb.BalanceResponse{Balance: int32(s.myBalance), NodeID: int32(s.nodeID)}, nil
}

func (s *Node) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	for !s.isActive {
		time.Sleep(100 * time.Millisecond) // Sleep for a short time to avoid CPU hogging
		return &pb.PromiseResponse{}, fmt.Errorf("server dead")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	// Log current state for debugging purposes
	fmt.Println("Inside Folower Prepare")
	fmt.Println("AcceptedTransactions: ", s.AcceptedMegaBlock)
	fmt.Println("TransactionLog: ", s.transactionLog)

	// Check if the request's ballot number is valid to promise
	if compareBallot(s, req) {
		// Only update promised ballot and currBallotNum if we're going to promise
		s.currBallotNum = req.Ballot.BallotNum
		s.promisedBallot = req.Ballot

		if s.isPromised {
			var highestLocalTransID int32 = -1
			for _, transaction := range s.transactionLog {
				if transaction.Id > highestLocalTransID {
					highestLocalTransID = transaction.Id
				}
			}
			s.highestTransIDSent = highestLocalTransID
			fmt.Println("The highest ID sent is ", highestLocalTransID)
			// If already promised, return previously accepted ballot and values
			return &pb.PromiseResponse{
				BallotNumber: req.Ballot,
				AcceptNum:    s.promisedBallot,
				Accept_Val:   s.AcceptedMegaBlock,
				Local:        s.transactionLog,
			}, nil
		} else {
			// Otherwise, send a new promise
			var highestLocalTransID int32 = -1
			for _, transaction := range s.transactionLog {
				if transaction.Id > highestLocalTransID {
					highestLocalTransID = transaction.Id
				}
			}
			s.highestTransIDSent = highestLocalTransID
			fmt.Println("The highest ID sent is ", highestLocalTransID)
			s.isPromised = true
			return &pb.PromiseResponse{
				BallotNumber: req.Ballot, // Promise on the received ballot number
				AcceptNum:    nil,        // No previously accepted ballot
				Accept_Val:   nil,        // No previously accepted values
				Local:        s.transactionLog,
			}, nil
		}
	}

	// If the condition fails (higher or equal ballot number exists), reject the prepare request
	return &pb.PromiseResponse{BallotNumber: &pb.Ballot{BallotNum: s.currBallotNum}}, fmt.Errorf("Prepare request rejected; higher or equal ballot number exists")
}

func (s *Node) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	for !s.isActive {
		// time.Sleep(100 * time.Second)
		return &pb.AcceptedResponse{Success: false}, fmt.Errorf("server dead")
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Println("Inside Accept of follower")
	fmt.Println("TransactionLog: ", s.transactionLog)

	// Only accept if the proposal number is greater than or equal to current ballot number
	if req.ProposalNumber == s.currBallotNum {
		// Accept the proposal and update AcceptedMegaBlock
		s.AcceptedMegaBlock = req.Value
		s.currBallotNum = req.ProposalNumber

		// Return a successful response
		fmt.Println("Accepted Mega Block: ", s.AcceptedMegaBlock)
		return &pb.AcceptedResponse{
			ProposalNumber: req.ProposalNumber,
			Success:        true, // Indicate that the proposal was accepted
		}, nil
	}

	// If the proposal number is less, reject the proposal
	fmt.Println("Rejected proposal with ProposalNumber: ", req.ProposalNumber)
	return &pb.AcceptedResponse{
		ProposalNumber: req.ProposalNumber,
		Success:        false, // Indicate that the proposal was rejected
	}, nil
}

func (s *Node) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitedResponse, error) {
	for !s.isActive {
		// Return an error if the server is not active
		return &pb.CommitedResponse{}, fmt.Errorf("server dead")
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Println("Inside Commit of follower")

	// Sort transactions by ID before committing
	s.sortTransactions()

	// Only proceed if the promised ballot matches the request's ballot number
	if s.promisedBallot.BallotNum == req.BallotNumber.BallotNum {
		// Apply the committed transactions to the node's state (balance updates)
		for _, transaction := range s.AcceptedMegaBlock {
			if transaction.To == s.nodeID {
				s.myBalance += transaction.Amount
			}
		}

		// Remove transactions from transactionLog that have been committed
		var newTransactionLog []*pb.TransactionRequest
		fmt.Println("Higest Transaction ID sent ", s.highestTransIDSent)
		for _, transaction := range s.transactionLog {
			// Keep transactions that have IDs higher than highestTransIDSent
			if transaction.Id > s.highestTransIDSent {
				newTransactionLog = append(newTransactionLog, transaction)
			}
		}
		fmt.Println("Transactional Log before in Commit ", s.transactionLog)
		s.transactionLog = newTransactionLog // Update the transactionLog with remaining transactions
		// s.transactionLog = nil
		// Commit transactions to the database
		fmt.Println("Transactional Log after in Commit ", s.transactionLog)
		s.CommittedTransactions = append(s.CommittedTransactions, s.AcceptedMegaBlock...)
		if len(s.AcceptedMegaBlock) == 0 {
			return &pb.CommitedResponse{
				Success: true,
			}, nil
		}
		err := s.commitTransactions(false)
		if err != nil {
			return nil, err
		}
		s.highestTransIDSent = -1
		// Clear the accepted mega block since it's now committed
		s.AcceptedMegaBlock = nil
		// s.transactionLog = nil
		// Reset the promise flag
		s.isPromised = false

		// Return success
		return &pb.CommitedResponse{
			Success: true,
		}, nil
	}

	// If the ballot numbers don't match, reject the commit
	return &pb.CommitedResponse{
		Success: false,
	}, nil
}

func (s *Node) InitDB() {
	db, err := sql.Open("sqlite3", fmt.Sprintf("node_%d.db", s.nodeID))
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	s.db = db
	// Create a transactions table if it doesn't exist
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY,
        set_number INTEGER,
        from_account INTEGER,
        to_account INTEGER,
        amount INTEGER
    )`)
	if err != nil {
		fmt.Println("Error creating table:", err)
	}
}

func (s *Node) sortTransactions() {
	sort.Slice(s.AcceptedMegaBlock, func(i, j int) bool {
		return s.AcceptedMegaBlock[i].Id < s.AcceptedMegaBlock[j].Id
	})
}
func (s *Node) commitTransactions(leader bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO transactions (id, set_number, from_account, to_account, amount)
                             VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if !leader {

		s.lastCommittedIndex = s.AcceptedMegaBlock[len(s.AcceptedMegaBlock)-1].Id
		fmt.Println("Last commited index id ", s.lastCommittedIndex)
		for _, transaction := range s.AcceptedMegaBlock {
			_, err := stmt.Exec(transaction.Id, transaction.SetNumber, transaction.From, transaction.To, transaction.Amount)
			if err != nil {
				tx.Rollback()
				return err
			}
			fmt.Println(transaction)
		}
	} else {
		fmt.Println("Printing Mega Block commit for leader ", s.megaBlock)
		s.lastCommittedIndex = s.megaBlock[len(s.megaBlock)-1].Id
		for _, transaction := range s.megaBlock {
			fmt.Println(transaction)
			_, err := stmt.Exec(transaction.Id, transaction.SetNumber, transaction.From, transaction.To, transaction.Amount)
			if err != nil {
				tx.Rollback()
				return err
			}
			fmt.Println(transaction)
		}
	}

	return tx.Commit()
}

func main() {
	id, _ := strconv.Atoi(os.Args[1])

	currNode := &Node{
		nodeID:           int32(id),
		isActive:         true,
		myBalance:        100,
		currBallotNum:    0,
		promisedBallot:   &pb.Ballot{},
		lock:             sync.Mutex{},
		transactionQueue: make(chan *pb.TransactionRequest, 100), // Buffered channel
	}
	currNode.InitDB()
	go processTransaction(currNode)
	go SetupRpc(id, currNode)
	go SetupNodeRpcSender(id, currNode)

	println("Node ", id, " Working")
	for {

	}
}

func SetupNodeRpcSender(id int, node *Node) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa((5005+id+5)))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPaxosServiceServer(s, node)
	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func SetupNodeRpcReciever(id int) (pb.PaxosServiceClient, context.Context, *grpc.ClientConn) {
	// fmt.Println("Setting Up RPC Reciever")
	conn, err := grpc.Dial("localhost:"+strconv.Itoa((5005+id+5)), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	// defer conn.Close()
	c := pb.NewPaxosServiceClient(conn)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	// cancel()
	return c, ctx, conn
}

func SetupRpc(id int, node *Node) {
	lis, err := net.Listen("tcp", ":5005"+strconv.Itoa(id))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterNodeServiceServer(s, node)
	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func compareBallot(s *Node, req *pb.PrepareRequest) bool {
	if req.Ballot.BallotNum > int32(s.promisedBallot.BallotNum) {
		return true
	} else if req.Ballot.BallotNum == int32(s.promisedBallot.BallotNum) && req.Ballot.NodeID > int32(s.nodeID) {
		return true
	}
	return false
}
