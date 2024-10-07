package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS-BANK-SIMULATOR/proto"
	"google.golang.org/grpc"
)

type Node struct {
	pb.UnimplementedNodeServiceServer
	pb.UnimplementedPaxosServiceServer
	nodeID               int32
	isActive             bool
	myBalance            int32
	currBallotNum        int32
	isPromised           bool
	promisedBallot       *pb.Ballot
	AcceptedTransactions []*pb.TransactionRequest
	transactionLog       []*pb.TransactionRequest
	megaBlock            []*pb.TransactionRequest
	lock                 sync.Locker
}

//	type Ballot struct {
//		nodeID    int
//		ballotNum int
//	}
type Transaction struct {
	SetNumber int
	From      int
	To        int
	Amount    int
}

// var transactions []Transaction

func (s *Node) AcceptTransactions(ctx context.Context, req *pb.TransactionRequest) (*pb.NodeResponse, error) {
	// x := pb.TransactionRequest{
	// 	SetNumber: req.SetNumber),
	// 	From:      req.From),
	// 	To:        req.To),
	// 	Amount:    req.Amount),
	// }
	fmt.Println("Trying to Accept Transactions")

	if s.myBalance-int32(req.Amount) < 0 {
		fmt.Println("Initiate consensus")
		var wg sync.WaitGroup
		wg.Add(4)
		for i := 1; i < 5; i++ {
			go func(i int) {
				defer wg.Done()
				if i != int(s.nodeID) {
					c, ctx, conn := SetupNodeRpcReciever(i)
					s.currBallotNum += 1
					promise, err := c.Prepare(ctx, &pb.PrepareRequest{
						Ballot: &pb.Ballot{
							BallotNum: s.currBallotNum,
							NodeID:    s.nodeID,
						},
					})
					if err != nil {
						log.Fatalf("Could not add: %v", err)
					}
					// fmt.Println(promise)
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
					s.megaBlock = append(s.megaBlock, s.transactionLog...)
					s.megaBlock = append(s.megaBlock, req)
					s.lock.Unlock()
					conn.Close()
				}
			}(i)
		}
		wg.Wait()
		wg.Add(4)
		fmt.Println("MY MEGA BLOCK IS ")
		fmt.Println(s.megaBlock)
		for i := 1; i < 5; i++ {
			go func(i int) {
				defer wg.Done()
				if i != int(s.nodeID) {

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
			}(i)
		}
		wg.Wait()
	}
	s.lock.Lock()
	if s.myBalance-int32(req.Amount) >= 0 {
		s.myBalance -= req.Amount
		s.transactionLog = append(s.transactionLog, req)
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

func (s *Node) GetBalance(ctx context.Context, req *pb.AdminRequest) (*pb.BalanceResponse, error) {
	println(req.Command)
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.isActive {
		return &pb.BalanceResponse{Balance: int32(s.myBalance), NodeID: int32(s.nodeID)}, nil
	} else {
		return nil, errors.New(" am not alive")
	}
}

func (s *Node) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// if s.isPromised && compareBallot(s, req) {
	fmt.Println("AcceptedTransactions: ", s.AcceptedTransactions)
	fmt.Println("TransactionLog: ", s.transactionLog)

	s.promisedBallot = req.Ballot
	return &pb.PromiseResponse{
		BallotNumber: req.Ballot,
		AcceptNum:    s.promisedBallot,
		Accept_Val:   s.AcceptedTransactions,
		Local:        s.transactionLog,
	}, nil

	// } else if !s.isPromised && (s.currBallotNum < req.Ballot.BallotNum || s.currBallotNum == req.Ballot.BallotNum && s.nodeID < req.Ballot.NodeID) {
	// 	return &pb.PromiseResponse{
	// 		BallotNumber: req.Ballot, // Promise on the received ballot number
	// 		AcceptNum:    nil,        // No previously accepted ballot
	// 		Accept_Val:   nil,        // No previously accepted values
	// 		Local:        s.transactionLog,
	// 	}, nil
	// }
	// return &pb.PromiseResponse{}, fmt.Errorf("Prepare request rejected; higher or equal ballot number exists")

}

func (s *Node) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fmt.Println("Inside Accept")
	// if req.ProposalNumber >= s.currBallotNum {
	fmt.Println(req.Value)
	for _, transaction := range req.Value {
		if transaction.To == s.nodeID {
			s.myBalance += transaction.Amount
		}
	}
	return &pb.AcceptedResponse{ProposalNumber: req.ProposalNumber}, nil
	// }
	// return &pb.AcceptedResponse{}, nil
}

func main() {
	id, _ := strconv.Atoi(os.Args[1])

	currNode := Node{nodeID: int32(id), isActive: true, myBalance: 200, currBallotNum: 0, promisedBallot: &pb.Ballot{}, lock: new(sync.Mutex)}
	go SetupRpc(id, currNode)
	go SetupNodeRpcSender(id, currNode)
	println("Node ", id, " Working")
	for {

	}
}

func SetupNodeRpcSender(id int, node Node) {
	lis, err := net.Listen("tcp", ":5005"+strconv.Itoa(id+5))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPaxosServiceServer(s, &node)
	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func SetupNodeRpcReciever(id int) (pb.PaxosServiceClient, context.Context, *grpc.ClientConn) {
	fmt.Println("Setting Up RPC Reciever")
	conn, err := grpc.Dial("localhost:5005"+strconv.Itoa(id+5), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	// defer conn.Close()
	c := pb.NewPaxosServiceClient(conn)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	// cancel()
	return c, ctx, conn
}

func SetupRpc(id int, node Node) {
	lis, err := net.Listen("tcp", ":5005"+strconv.Itoa(id))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterNodeServiceServer(s, &node)
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
