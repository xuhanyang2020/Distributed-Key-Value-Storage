# Distributed Key-Value Database System


## Overview
This project implements a key-value database system that supports distributed transaction. This system supports transactions that read and write to distributed objects while ensuring full ACID properties.


## Branches, Accounts, Clients and Servers
The system represents a collections of accounts and their states.

### Branch
In the distributed system, we have branches (A, B, C etc.) where accounts are stored in. 

### Account
An account is named by the identifier of the branch following by the account name; e.g., `A.x` is the account `x` stored in branch `A`.

### Server
Each servers represents a branch. The branch will be specified when server is set up. 


### Client
Client is used to interact with database system. Each client could pick up a server randomly as a transaction coordinator and make operations of the accounts.

## How to run

### Libraries

```go
Go 1.15
```

### Building

1. change config.txt file in your working environment. The format of the file is [server_name] [ip_address] [port]. Deploy the project on clusters.

2. Make sure go into the directory /src, run the following commands to produce executable files
```shell
    go build server.go
    go build client.go
```
### Running


For servers, run
```shell
./server [server_name] [config_name]
```

For clients, run
```shell
./client [client_name] [config_name]
```

### Running example
Here is a simple example of how to use the databse system. Two transactions are provided in the repository
[transaction1](transaction1.txt) and [transaction2](transaction2.txt). If running transaction1 followed by transaction2, the returned results for two transactions will be like [result1](result1.txt) and [result2](result2.txt).

The following is transaction1. Left is input command and right is returned results.

```shell
BEGIN 
                OK
DEPOSIT A.x 10
                OK
DEPOSIT B.y 10
                OK
COMMIT
                COMMIT OK
```

The following is transaction2.

```shell
BEGIN
                OK
BALANCE A.x
                10
BALANCE B.y
                10
COMMIT
                COMMIT OK

```
## Commands to run
After setting up, clients starts accepting commands from stdin. 
There are several commands for clients when doing a transaction, 
### **BEGIN**
Operation format:
```shell
BEGIN
```
Open a new transaction, the client randomly picks a server as the transaction coordinator.
### **DEPOSIT**
Operation format for depositing 100 into account x of branch A:
```shell
DEPOSIT A.x 100
```
Deposit some amount into an account. The balance will increase by the given amount. If the account does not exists then the deposit operation will creates a new account.
### **WITHDRAW**
Operation format for withdrawing 80 into account x of branch A:
```shell
WITHDRAW A.x 80
```
Withdraw some amount from an account. If the account does not exists, the transaction will abort in advance and return `NOT FOUND, ABORTED`. Notice that any negative balance during the transaction is allowed during the transaction as the legality check is done until `COMMIT` command. 
### **BALANCE**
Operation format for checking the balance of account x of branch A:
```shell
BALANCE A.x
```
This query is used to read the balance of an account. If the account does not exists, the transaction will abort in advance and return `NOT FOUND, ABORT`.
### **COMMIT**
Operation format:
```shell
COMMIT
```
Commit the transaction, making its results visible to other transactions. The client should reply either with `COMMIT OK` or `ABORTED`, in the case that the transaction had to be aborted during the commit process.
### **ABORT**
Operation format:
Operation format:
```shell
ABORT
```
Abort the transaction. All updates made during the transaction must be rolled back. The client should reply with `ABORTED` to confirm that the transaction was aborted.

## Function implementation

### **BEGIN**
Client sends a “BEGIN” command to the coordinator (randomly chosen). The coordinator forwards the request to the cluster. Each server gives this transaction a timestamp and returns “OK”.

### **DEPOSIT**
Client sends a “DEPOSIT A.foo 10” command to the coordinator. The coordinator forwards the request to server A. Here we check the write rule of timestamped ordering. If it’s ok, add(update) the value in the TW list, and return “OK”. Else, return “ABORT” to the coordinator and abort the 

### **WITHDRAW**
Client sends a “DEPOSIT A.foo 10” command to the coordinator. The coordinator forwards the request to server A. Here we check the write rule of timestamped ordering. If it’s ok, add(update) the value in the TW list, and return “OK”. Else, return “ABORT” to the coordinator and abort the transaction in clusters.

### **BALANCE**
Client sends a “BALANCE A.foo” command to the coordinator. The coordinator forwards the request to server A. Here we check the reading rule of timestamped ordering:
* Committed timestamp is greater than current timestamp, abort the transaction in clusters
* Committed timestamp is less than current timestamp:
  * No other smaller timestamp exists in TW (no pending actions). Reading account and add timestamp into RTS.
  * Some smaller timestamp exists in TW. Wait until no pending writing actions with a smaller timestamp in TW. Then Read.

### **COMMIT** 
We use the **Two-Phase Commit** mechanism to commit the transaction. Client sends a “COMMIT” command to the coordinator. The coordinator checks TW and waits for other transactions with lower timestamps and uncommitted writes. 
* **Prepare Phase** : If it’s ok, the coordinator sends “PREPARE” message to other servers, waiting for the votes. Other servers check if the transaction is valid (by checking the result of the balance combined with the TW). If valid, return “VOTE YES”, otherwise return “VOTE NO”. 
* **Commit Phase** : After the coordinator receives the vote results from all servers and every node votes YES, it would send a “REALCOMMIT” message. The other servers would commit the transaction and update the account information based on “REALCOMMIT” message.

### **ABORT**
At the voting phase, if there are one or more servers vote “NO”, the coordinator would send a “ABORT” message to other servers. Other servers would abort the transactions by updating the last_transaction_timestamp information and emptying the TW.


## Functionality Testing:
### **Atomicity**:
Each transaction is either wholly committed or aborted. 
*	When there exist actions violating the writing rule or reading rule. The server will send “ABORT” message to the coordinator and the coordinator sends “ABORT” to the cluster. All servers delete the changes in TW.
*	When committing, we use “2 Phase Commit” to implement. Before the transaction is committed, the account does not change. After commit, changes of accounts in TW will be deleted and the commit_map will be updated to the newest timestamp. Then other pending transaction continue.
### **Consistency**
We must make sure the balance of accounts is greater than 0. Therefore, before each commit we use function “checkTWValidity” to identify whether the transaction is qualified for commit. We calculate a “fake balance” for every account in “write_map” of current transaction. If “fake balance” is less than 0, the transaction will be aborted in clusters.
### **Isolation**
We use optimistic method by applying timestamp ordering. Each transaction has a unique timestamp in the cluster. Based on each transaction’s timestamp and write && read rule of timestamp ordering, we can control the state of each transaction (OK, Commit or ABort). 
### **Deadlock**
Here we use optimistic method with timestamp. This method naturally avoids the occurrence of deadlock.
