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

type Node struct {
	pb.UnimplementedNodeServiceServer
	pb.UnimplementedPaxosServiceServer
	nodeID            int32
	isActive          bool
	myBalance         int32
	currBallotNum     int32
	isPromised        bool
	promisedBallot    *pb.Ballot
	transactionLog    []*pb.TransactionRequest
	megaBlock         []*pb.TransactionRequest
	AcceptedMegaBlock []*pb.TransactionRequest
	db                *sql.DB
	lock              sync.Mutex
}

func (s *Node) AcceptTransactions(ctx context.Context, req *pb.TransactionRequest) (*pb.NodeResponse, error) {

	fmt.Println("Trying to Accept Transactions")

	if s.myBalance-int32(req.Amount) < 0 && s.isActive {
		fmt.Println("Initiate consensus")
		var wg sync.WaitGroup
		wg.Add(5)
		s.currBallotNum += 1
		for i := 1; i <= 5; i++ {
			go func(i int, s *Node) {
				if i != int(s.nodeID) {
					fmt.Println("Inside Prepare")
					c, ctx, conn := SetupNodeRpcReciever(i)
					promise, err := c.Prepare(ctx, &pb.PrepareRequest{
						Ballot: &pb.Ballot{
							BallotNum: s.currBallotNum,
							NodeID:    s.nodeID,
						},
					})
					if err != nil {
						log.Fatalf("Could not add: %v", err)
					}
					fmt.Println("Promise From ", i)
					fmt.Println(promise)
					s.lock.Lock()
					for _, transaction := range promise.Accept_Val {
						if transaction.To == s.nodeID {
							s.myBalance += transaction.Amount
						}
					}
					for _, transaction := range promise.Local {
						if transaction.To == s.nodeID {
							s.myBalance += transaction.Amount
						}
					}
					fmt.Println("Printing Promise")
					fmt.Println(promise)
					s.megaBlock = append(s.megaBlock, promise.Accept_Val...)
					s.megaBlock = append(s.megaBlock, promise.Local...)
					s.lock.Unlock()
					conn.Close()
				}
				wg.Done()
			}(i, s)
		}
		fmt.Println("outside Prepare")
		wg.Wait()
		fmt.Println("Really outside Prepare")
		wg.Add(5)
		s.lock.Lock()
		s.megaBlock = append(s.megaBlock, s.transactionLog...)
		s.megaBlock = append(s.megaBlock, req)

		fmt.Println("MY MEGA BLOCK IS ")

		fmt.Println(s.megaBlock)
		for i := 1; i <= 5; i++ {
			go func(i int) {

				if i != int(s.nodeID) {
					fmt.Println("Inside Accept")

					c, ctx, conn := SetupNodeRpcReciever(i)
					accepted, err := c.Accept(ctx, &pb.AcceptRequest{
						ProposalNumber: s.currBallotNum,
						Value:          s.megaBlock,
					})
					if err != nil {
						log.Fatalf("Could not add: %v", err)
					}
					fmt.Println(accepted)
					conn.Close()
				}
				defer wg.Done()
			}(i)
		}
		wg.Wait()
		wg.Add(5)
		for i := 1; i <= 5; i++ {
			go func(i int) {

				if i != int(s.nodeID) {
					fmt.Println("Inside Commit")
					c, ctx, conn := SetupNodeRpcReciever(i)
					ack, err := c.Commit(ctx, &pb.CommitRequest{
						BallotNumber: &pb.Ballot{
							BallotNum: s.currBallotNum,
							NodeID:    s.nodeID,
						},
					})
					if err != nil {
						log.Fatalf("Could not add: %v", err)
					}

					fmt.Println(ack)
					conn.Close()
				}
				defer wg.Done()
			}(i)
		}
		wg.Wait()
		err := s.commitTransactions(true) // Commit transactions to the database
		if err != nil {
			return nil, err
		}
		s.lock.Unlock()
	}
	s.lock.Lock()
	if s.myBalance-int32(req.Amount) >= 0 {
		s.myBalance -= req.Amount
		s.transactionLog = append(s.transactionLog, req)
		// if len(s.megaBlock) != 0 {

		// }
		fmt.Println("This my transaction log ", s.transactionLog)
	}

	s.lock.Unlock()
	return &pb.NodeResponse{Ack: "Node " + strconv.Itoa(int(s.nodeID)) + " Took All transactions"}, nil
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
	defer s.lock.Unlock()
	s.isActive = true
	return &pb.ReviveResponse{
		Success: true,
	}, nil
}

func (s *Node) GetBalance(ctx context.Context, req *pb.AdminRequest) (*pb.BalanceResponse, error) {
	println(req.Command)
	s.lock.Lock()
	defer s.lock.Unlock()
	// if s.isActive {

	// } else {
	// 	return
	// }
	return &pb.BalanceResponse{Balance: int32(s.myBalance), NodeID: int32(s.nodeID)}, nil
}

func (s *Node) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Println("AcceptedTransactions: ", s.AcceptedMegaBlock)
	fmt.Println("TransactionLog: ", s.transactionLog)
	s.promisedBallot = req.Ballot
	s.currBallotNum = req.Ballot.BallotNum
	if s.isPromised && compareBallot(s, req) {
		s.isPromised = true
		return &pb.PromiseResponse{
			BallotNumber: req.Ballot,
			AcceptNum:    s.promisedBallot,
			Accept_Val:   s.AcceptedMegaBlock,
			Local:        s.transactionLog,
		}, nil
	} else if !s.isPromised && (s.currBallotNum < req.Ballot.BallotNum || s.currBallotNum == req.Ballot.BallotNum) {
		// && s.nodeID < req.Ballot.NodeID
		s.isPromised = true
		return &pb.PromiseResponse{
			BallotNumber: req.Ballot, // Promise on the received ballot number
			AcceptNum:    nil,        // No previously accepted ballot
			Accept_Val:   nil,        // No previously accepted values
			Local:        s.transactionLog,
		}, nil
	}
	return &pb.PromiseResponse{}, fmt.Errorf("Prepare request rejected; higher or equal ballot number exists")

}

func (s *Node) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Println("Inside Accept")
	s.AcceptedMegaBlock = req.Value
	fmt.Println("Accepted Mega Block: ", s.AcceptedMegaBlock)
	fmt.Println("TransactionLog: ", s.transactionLog)
	if req.ProposalNumber >= s.currBallotNum {

		return &pb.AcceptedResponse{ProposalNumber: req.ProposalNumber}, nil
	}
	return &pb.AcceptedResponse{}, nil
}

func (s *Node) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitedResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Println("Inside Commit")
	s.sortTransactions()

	if s.promisedBallot.BallotNum == req.BallotNumber.BallotNum {
		for _, transaction := range s.AcceptedMegaBlock {
			if transaction.To == s.nodeID {
				s.myBalance += transaction.Amount
			}
		}

		err := s.commitTransactions(false) // Commit transactions to the database
		if err != nil {
			return nil, err
		}
		s.AcceptedMegaBlock = nil
		s.isPromised = false
		return &pb.CommitedResponse{
			Success: true,
		}, nil
	}
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
		for _, transaction := range s.AcceptedMegaBlock {
			_, err := stmt.Exec(transaction.Id, transaction.SetNumber, transaction.From, transaction.To, transaction.Amount)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	} else {
		for _, transaction := range s.megaBlock {
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
		nodeID:         int32(id),
		isActive:       true,
		myBalance:      100,
		currBallotNum:  0,
		promisedBallot: &pb.Ballot{},
		lock:           sync.Mutex{},
	}
	currNode.InitDB()
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
	} else if req.Ballot.BallotNum == int32(s.promisedBallot.BallotNum) && req.Ballot.NodeID > int32(req.Ballot.NodeID) {
		return true
	}
	return false
}
