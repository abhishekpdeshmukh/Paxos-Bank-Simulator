Paxos-Bank-Simulator
This repository contains the implementation of a distributed banking application developed as part of the CSE 535: Distributed Systems course. The application leverages a Modified Paxos Consensus Protocol to manage transactions across a network of servers, ensuring consistency and reliability, even in the presence of server failures. The protocol facilitates the replication of the transaction log across multiple nodes, enabling the application to provide a robust and fault-tolerant distributed system.

Features
Distributed Consensus: Utilizes the Paxos algorithm to achieve consensus on transactions.
Fault Tolerance: Ensures that transactions are processed reliably, even in the event of node failures.
Scalability: Capable of running on multiple nodes, supporting distributed banking operations.
Concurrency: Supports handling concurrent transactions across the network of servers.
Requirements



Golang 1.16 or higher
Protocol Buffers Compiler (protoc) for Go (protoc-gen-go and protoc-gen-go-grpc)
Setup Instructions
Follow the steps below to build and run the Paxos-Bank-Simulator.

Step 1: Compile the Protocol Buffers

First, compile the .proto file to generate the required Go files for gRPC communication. Run the following command in the root directory of the project:

protoc --proto_path=. --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. adminrpc.proto
This will generate the necessary Go files for the administrator RPC service.

Step 2: Running the Administrator
To start the Administrator, navigate to the root directory of the project and execute the following command:
go run ./administrator.go

The administrator manages the banking system, providing options to start transactions, spawn nodes, and handle various operations in the system.

Step 3: Running the Nodes
The system operates with five nodes, each representing a server in the distributed system. To run each node, open a terminal window and navigate to the node directory. Then, execute the following command, where 1/2/3/4/5 represents the node number:

go run ./node.go 1

Repeat the above command for node numbers 1 through 5, opening separate terminal windows for each. Each node will act as an individual server participating in the Paxos protocol.

Step 4: Managing Transactions
Once the administrator and nodes are running, the administrator provides an interface to initiate and manage transactions. Use the administrator's menu options to start processing transactions, view balances, and simulate node failures or communication disruptions.

Project Structure
administrator.go: The main file that manages the administrator role, spawning nodes and handling transactions.
node.go: The main file that simulates a Paxos node, handling consensus and transaction processing.
adminrpc.proto: The Protocol Buffers definition for gRPC communication between the administrator and nodes.
Conclusion
The Paxos-Bank-Simulator demonstrates a practical implementation of the Paxos consensus protocol in a distributed banking system. By following the steps above, you can simulate a reliable, fault-tolerant banking system with distributed nodes running in a network.

For any questions or issues, feel free to reach out.


