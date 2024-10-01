package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	pb "github.com/abhishekpdeshmukh/PAXOS-BANK-SIMULATOR/proto"
	"google.golang.org/grpc"
)

type Node struct {
	pb.UnimplementedNodeServiceServer
	ID        int
	IsActive  bool
	myBalance int
}
type Transaction struct {
	SetNumber int
	From      int
	To        int
	Amount    int
}

var transactions []Transaction

func (s *Node) AcceptTransactions(ctx context.Context, req *pb.TransactionRequest) (*pb.NodeResponse, error) {
	x := Transaction{
		SetNumber: int(req.SetNumber),
		From:      int(req.From),
		To:        int(req.To),
		Amount:    int(req.Amount),
	}
	transactions = append(transactions, x)
	s.myBalance -= int(req.Amount)
	return &pb.NodeResponse{Ack: "Took All transactions"}, nil
}
func (s *Node) Kill(ctx context.Context, req *pb.AdminRequest) (*pb.NodeResponse, error) {
	println(req.Command)
	if s.IsActive {
		s.IsActive = false
		return &pb.NodeResponse{Ack: "Response From Node"}, nil
	} else {
		return &pb.NodeResponse{Ack: "I am already Dead why are you killing me again"}, nil
	}
}


func (s *Node) GetBalance(ctx context.Context, req *pb.AdminRequest) (*pb.BalanceResponse, error) {
	println(req.Command)
	if s.IsActive {
		return &pb.BalanceResponse{Balance: int32(s.myBalance), ServerId: int32(s.ID)}, nil
	} else {
		return nil, errors.New(" am not alive")
	}
}
func main() {
	id, _ := strconv.Atoi(os.Args[1])
	currNode := Node{ID: id, IsActive: true, myBalance: 50}
	go SetupRpc(id, currNode)
	println("Node ", id, " Working")
	for {

	}
}

func SetupNodeRpcSender(id int, node Node) {
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

func SetupNodeRpcReciever(id int) (pb.NodeServiceClient, context.Context, *grpc.ClientConn) {
	fmt.Println("Executing: Kill Nodes")
	conn, err := grpc.Dial("localhost:5005"+strconv.Itoa(id), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	// defer conn.Close()
	c := pb.NewNodeServiceClient(conn)

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
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
