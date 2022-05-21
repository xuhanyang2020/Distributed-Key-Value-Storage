package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type server struct {
	port               string
	branch             string
	timestamp          int
	configuration      []string
	num_nodes          int
	num_votes          int
	in_prepare         bool
	cur_prepare_commit int

	network_mutex  sync.Mutex
	msg_map_mutex  sync.Mutex
	accounts_mutex sync.Mutex
	cond_mutex     sync.Mutex
	commit_monitor *sync.Cond
	commit_chan    chan bool

	accounts      map[string]int
	server_conn   map[string]net.Conn
	client_conn   map[string]net.Conn
	timestamp_map map[string]int
	commit_map    map[string]int
	write_map     map[int][]string // Key : transaction timestamp, value : which objects will be modified by this transaction
	RTS           map[string][]int
	TW            map[string]map[int]int
}

var self server

func main() {
	// fmt.Print(os.Args[1])
	// fmt.Print(os.Args[2])
	initServer(os.Args[1:])
	// after reading the config.txt setting up tcp listening socket
	fmt.Printf("self branch is %s,listening port is %s \n", self.branch, self.port)
	listen, err := net.Listen("tcp", ":"+self.port)
	if err != nil {
		fmt.Println("Error setting up listener")
		return
	}
	// multiple gorountines to connect with others
	serverConnect()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("Fail to set up connection with servers or clients")
			continue
		}
		go requestProcesser(conn)
	}

}

func initServer(args []string) {
	self.timestamp = 0
	self.num_nodes = 1
	self.num_votes = 0
	self.in_prepare = false

	self.server_conn = make(map[string]net.Conn)
	self.client_conn = make(map[string]net.Conn)
	self.timestamp_map = make(map[string]int)
	self.accounts = make(map[string]int)
	self.RTS = make(map[string][]int)
	self.TW = make(map[string]map[int]int)

	self.commit_map = make(map[string]int)
	self.write_map = make(map[int][]string)

	self.commit_monitor = sync.NewCond(&self.cond_mutex)
	self.commit_chan = make(chan bool)

	if len(args) != 2 {
		fmt.Println("Usage: ./server <port> <branch>")
		os.Exit(1)
	}

	self.branch = args[0]
	config_string := args[1]

	// read configuration from config.txt
	configFile, err := os.Open(config_string)
	if err != nil {
		fmt.Println("cannot open the configuration file")
		return
	}
	// scanner to read line by line
	scanner := bufio.NewScanner(configFile)
	for scanner.Scan() {
		// add each line to string[] for further connection
		line_content := scanner.Text()
		self.configuration = append(self.configuration, line_content)

		// update local server's port config
		if strings.Split(line_content, " ")[0] == self.branch {
			self.port = strings.Split(line_content, " ")[2]
		}
	}

}

// get connected with other servers
func serverConnect() {
	config := self.configuration
	for i := 0; i < len(config); i++ {
		// config[i] records the ip and port of a server
		line_content := config[i]
		splited_line := strings.Split(line_content, " ")
		// multiple goroutines to connect with servers
		go func(splited_line []string) {
			// if the server is local server, no need to connect with
			if strings.Compare(self.branch, splited_line[0]) == 0 {
				return
			}

			IP_addr, targetPort, targetBranch := splited_line[1], splited_line[2], splited_line[0]
			// each server already sets up listening socket, just connect
			for {
				conn, err := net.Dial("tcp", IP_addr+":"+targetPort)
				if err == nil {
					fmt.Printf("Setting up TCP connection with %s %s \n", IP_addr, targetPort)
					// after setting connections, just convey info
					self.network_mutex.Lock()
					conn.Write([]byte(wrapMsg("INIT")))
					self.network_mutex.Unlock()
					// Set connection in map
					self.network_mutex.Lock()
					self.server_conn[targetBranch] = conn
					self.num_nodes++
					self.network_mutex.Unlock()
					break
				}
			}

		}(splited_line)
	}
}

func requestProcesser(conn net.Conn) {
	defer conn.Close()
	bufReader := bufio.NewReader(conn)
	// Get name and print time when connection made.
	bytes, err := bufReader.ReadBytes('\n')
	if err != nil {
		conn.Close()
		return
	}
	command := string(bytes)
	command = strings.TrimSuffix(command, "\n")
	splits := strings.Fields(command)
	is_client := splits[0] == "1"
	node_name := splits[1]
	if is_client {
		self.client_conn[node_name] = conn
	}

	for {
		msg, err1 := bufReader.ReadString('\n')
		if err1 != nil {
			self.network_mutex.Lock()
			conn.Write([]byte(wrapMsg("COULD NOT READ MESSAGES")))
			self.network_mutex.Unlock()
			return
		}
		msg = strings.TrimSuffix(msg, "\n")
		go processMsg(msg, is_client, node_name)
	}
}

func processMsg(msg string, is_client bool, node_name string) {
	DPrintf("Server %s received message: %s\n", self.branch, msg)
	if is_client {
		processMsgFromClient(msg)
	} else {
		processMsgFromServer(msg, node_name)
	}
}

// msg format : 1 client0 DEPOSIT A.x 10
func processMsgFromClient(msg string) {
	splits := strings.Fields(msg)
	client_name := splits[1]
	command := splits[2]
	client_conn := self.client_conn[client_name]
	if command == "BEGIN" {
		// forward begin to other peers
		self.network_mutex.Lock()
		for key, value := range self.server_conn {
			if key != self.branch {
				_, err := value.Write([]byte(msg + "\n"))
				if err != nil {
					DPrintf("Fail to send message to %s\n", key)
				}
			}
		}
		self.network_mutex.Unlock()
		execute_command(splits)
		self.network_mutex.Lock()
		client_conn.Write([]byte("OK\n"))
		self.network_mutex.Unlock()
		return
	} else if command == "COMMIT" {
		reply := execute_command(splits)
		if reply == "COMMIT OK" {
			realCommit(client_name)
		} else {
			abort_transaction(client_name)
		}
		self.network_mutex.Lock()
		client_conn.Write([]byte(reply + "\n"))
		self.network_mutex.Unlock()

		self.commit_monitor.L.Lock()
		self.commit_monitor.Broadcast()
		self.commit_monitor.L.Unlock()
	} else if command == "ABORT" {
		abort_transaction(client_name)
		self.network_mutex.Lock()
		client_conn.Write([]byte("ABORTED\n"))
		self.network_mutex.Unlock()
	} else {
		branch := strings.Split(splits[3], ".")[0]
		if branch != self.branch {
			DPrintf("Server %s forward message to %s\n", self.branch, branch)

			conn := self.server_conn[branch]

			self.network_mutex.Lock()
			conn.Write([]byte(msg + "\n"))
			self.network_mutex.Unlock()
		} else {

			reply := execute_command(splits)

			self.network_mutex.Lock()
			client_conn.Write([]byte(reply + "\n"))
			self.network_mutex.Unlock()
		}
	}
}

func processMsgFromServer(msg string, peer_name string) {
	splits := strings.Fields(msg)
	command := splits[2]
	switch command {
	case "REPLY":
		// reply from other servers, format: 1 sender_name REPLY msg client_name
		client_name := splits[3]
		reply_msg := ""
		for i := 4; i < len(splits); i++ {
			reply_msg = reply_msg + splits[i] + " "
		}
		reply_msg = strings.TrimSuffix(reply_msg, " ")
		client_conn := self.client_conn[client_name]
		self.network_mutex.Lock()
		client_conn.Write([]byte(reply_msg + "\n"))
		self.network_mutex.Unlock()

	case "PREPARE":
		// prepare from other servers, formart: 1 sender_name PREPARE client_name
		client_name := splits[3]
		last_uncommitted_timestamp, _ := checkTimestamp(client_name)
		vote := last_uncommitted_timestamp == 0
		vote = vote && checkTWValidity(client_name)
		if vote {
			// if no uncommitted transaction, vote yes
			self.network_mutex.Lock()
			self.server_conn[peer_name].Write([]byte(wrapMsg("VOTE YES" + " " + client_name)))
			self.network_mutex.Unlock()
		} else {
			// if uncommitted transaction, vote no
			self.network_mutex.Lock()
			self.server_conn[peer_name].Write([]byte(wrapMsg("VOTE NO" + " " + client_name)))
			self.network_mutex.Unlock()
		}

	case "VOTE":
		// vote from other servers, format: 1 sender_name VOTE YES/NO client_name
		vote := splits[3]
		if vote == "YES" {
			// if vote yes, add vote value
			client_name := splits[4]
			self.msg_map_mutex.Lock()
			if !self.in_prepare || self.timestamp_map[client_name] != self.cur_prepare_commit {
				self.msg_map_mutex.Unlock()
				return
			}
			self.num_votes += 1
			if self.num_votes == self.num_nodes {
				self.commit_chan <- true
			}
			self.msg_map_mutex.Unlock()
		} else {
			client_name := splits[4]
			self.msg_map_mutex.Lock()
			if !self.in_prepare || self.timestamp_map[client_name] != self.cur_prepare_commit {
				self.msg_map_mutex.Unlock()
				return
			}
			self.commit_chan <- true
			self.msg_map_mutex.Unlock()
		}

	default:
		return_msg := execute_command(splits)
		if len(return_msg) > 0 {
			client_name := splits[1]
			self.network_mutex.Lock()
			conn := self.server_conn[peer_name]
			conn.Write([]byte(wrapMsg("REPLY " + client_name + " " + return_msg)))
			self.network_mutex.Unlock()
		}
	}
}

// msg format : 1 client0 DEPOSIT A.x 10
func execute_command(msg []string) string {
	command := msg[2]
	client_name := msg[1]
	DPrintf("server %s execute %s: %s\n", self.branch, client_name, command)
	switch command {
	case "BEGIN":
		self.msg_map_mutex.Lock()
		self.timestamp += 1
		self.timestamp_map[client_name] = self.timestamp
		self.msg_map_mutex.Unlock()
		return ""
	case "COMMIT":
		last_uncommitted_timestamp, last_uncommitted_account := checkTimestamp(client_name)
		if last_uncommitted_timestamp > 0 {
			remain := true
			for remain {
				self.commit_monitor.L.Lock()
				self.commit_monitor.Wait()
				self.msg_map_mutex.Lock()
				last_uncommitted_timestamp, last_uncommitted_account = checkTimestamp(client_name)
				_, remain = self.TW[last_uncommitted_account][last_uncommitted_timestamp]
				self.msg_map_mutex.Unlock()
			}
			self.commit_monitor.L.Unlock()
		}

		return start_2PC(client_name)
	case "REALCOMMIT":
		tran_client_name := msg[3]
		realCommit(tran_client_name)
		self.commit_monitor.L.Lock()
		self.commit_monitor.Broadcast()
		self.commit_monitor.L.Unlock()
	case "ABORT":
		tran_client_name := msg[3]
		self.msg_map_mutex.Lock()
		t := self.timestamp_map[tran_client_name]
		write_objects, exists := self.write_map[t]
		if exists {
			for _, write_account := range write_objects {
				delete(self.TW[write_account], t)
			}
		}
		self.msg_map_mutex.Unlock()
		self.commit_monitor.L.Lock()
		self.commit_monitor.Broadcast()
		self.commit_monitor.L.Unlock()
	case "BALANCE":
		account := strings.Split(msg[3], ".")[1]
		// timestamp
		self.msg_map_mutex.Lock()
		t := self.timestamp_map[client_name]

		latest_commit, exists := self.commit_map[account]

		_, account_exists_inTW := self.TW[account]
		// account does not exists in committed and incoming map
		if !exists && !exists_less_uncommitted(t, account) && !account_exists_inTW {
			self.msg_map_mutex.Unlock()
			abort_transaction(client_name)
			return "NOT FOUND, ABORTED"
		}

		// according to read rule, check if Tc greater than than committed version of object
		if t > latest_commit {
			self.commit_monitor.L.Lock()
			// In the for loop, check whether the version of maximum write of object is committed
			// If not, block here to wait for that to be committed
			// Use monitor mode here (commit_monitor)
			for exists_less_uncommitted(t, account) {
				// Current goroutine waiting here, therefore it should release the msg_lock
				self.msg_map_mutex.Unlock()
				// Here current goroutine should wait until no timestamp less than current timestamp
				// that exists in TW (maybe committed or aborted by other goroutine),
				// Wait until other goroutine update the TW
				self.commit_monitor.Wait()
				self.msg_map_mutex.Lock()
			}
			self.commit_monitor.L.Unlock()

			value, written_by_same := self.TW[account][t]

			if written_by_same {
				// add current timestamp into RTS
				_, already_read := self.RTS[account]
				if !already_read {
					self.RTS[account] = make([]int, 0)
				}
				self.RTS[account] = append(self.RTS[account], t)

				self.accounts_mutex.Lock()
				balance, ok := self.accounts[account]
				self.accounts_mutex.Unlock()
				if ok {
					value += balance
				}
				self.msg_map_mutex.Unlock()
				return fmt.Sprintf("%s = %d", msg[3], value)

			} else {
				_, already_read := self.RTS[account]
				if !already_read {
					self.RTS[account] = make([]int, 0)
				}
				self.msg_map_mutex.Unlock()

				self.accounts_mutex.Lock()
				value, account_exists := self.accounts[account]
				self.accounts_mutex.Unlock()
				if account_exists {
					return fmt.Sprintf("%s = %d", msg[3], value)
				} else {
					abort_transaction(client_name)
					return "NOT FOUND, ABORTED"
				}
			}
		}
	case "DEPOSIT":
		account := strings.Split(msg[3], ".")[1]
		// timestamp
		self.msg_map_mutex.Lock()
		t := self.timestamp_map[client_name]
		self.msg_map_mutex.Unlock()
		money, _ := strconv.Atoi(msg[4])
		// check for the read ts and write ts lists
		can_write := writeRuleCheck(client_name, account)

		if !can_write {
			abort_transaction(client_name)
			return "ABORTED"
		}
		self.msg_map_mutex.Lock()

		// if there is incoming update in TW, change the content of TW,
		// otherwise, create a new tuple in TW
		_, exists1 := self.TW[account]
		if !exists1 {
			self.TW[account] = make(map[int]int, 0)
		}

		account_timestamps := self.TW[account]
		_, exists := account_timestamps[t]
		if !exists {
			account_timestamps[t] = 0
		}
		account_timestamps[t] += money

		_, exists = self.write_map[t]
		if !exists {
			self.write_map[t] = make([]string, 0)
		}
		// if already in write_map
		already_in_writemap := false
		for _, element := range self.write_map[t] {
			if strings.Compare(account, element) == 0 {
				already_in_writemap = true
				break
			}
		}
		// if not exists in write_map, create the account
		if !already_in_writemap {
			self.write_map[t] = append(self.write_map[t], account)
		}
		self.msg_map_mutex.Unlock()
		return "OK"
	case "WITHDRAW":
		account := strings.Split(msg[3], ".")[1]

		self.msg_map_mutex.Lock()
		t := self.timestamp_map[client_name]
		self.msg_map_mutex.Unlock()
		// check for the read ts and write ts lists
		can_write := writeRuleCheck(client_name, account)
		money, _ := strconv.Atoi(msg[4])
		if !can_write {
			abort_transaction(client_name)
			return "ABORTED"
		}

		self.msg_map_mutex.Lock()
		_, exists := self.write_map[t]

		// still need this code ?
		//
		//

		// make sure write_map already have this timestamp and this account
		if !exists {
			self.write_map[t] = make([]string, 0)
		}

		already_in_writemap := false
		for _, element := range self.write_map[t] {
			if strings.Compare(account, element) == 0 {
				already_in_writemap = true
				break
			}
		}
		// if not exists in write_map, create the account
		if !already_in_writemap {
			self.write_map[t] = append(self.write_map[t], account)
		}
		// whether modifies this account before
		_, exists1 := self.TW[account]
		if !exists1 {
			self.TW[account] = make(map[int]int, 0)
		}

		account_timestamps := self.TW[account]
		_, exists = account_timestamps[t]
		if !exists {
			account_timestamps[t] = 0
		}
		account_timestamps[t] -= money
		self.msg_map_mutex.Unlock()

		// before withdraw, we should judge if the account exists in account(committed) or in TW (waiting to be committed)
		self.accounts_mutex.Lock()
		_, account_exists := self.accounts[account]
		self.accounts_mutex.Unlock()
		self.msg_map_mutex.Lock()
		if !account_exists {
			self.commit_monitor.L.Lock()
			// In the for loop, check whether the version of maximum write of object is committed
			// If not, block here to wait for that to be committed
			// Use monitor mode here (commit_monitor)
			for exists_less_uncommitted(t, account) {
				// Current goroutine waiting here, therefore it should release the msg_lock
				self.msg_map_mutex.Unlock()
				// Here current goroutine should wait until no timestamp less than current timestamp
				// that exists in TW (maybe committed or aborted by other goroutine),
				// Wait until other goroutine update the TW
				self.commit_monitor.Wait()
				self.msg_map_mutex.Lock()
			}
			_, account_exists = self.accounts[account]
			self.commit_monitor.L.Unlock()

		}
		self.msg_map_mutex.Unlock()

		if !account_exists {
			abort_transaction(client_name)
			return "NOT FOUND, ABORTED"
		}
		return "OK"
	default:
		return "UNKNOWN COMMAND"
	}
	return ""
}

func start_2PC(client_name string) string {
	self.msg_map_mutex.Lock()
	self.in_prepare = true
	self.num_votes = 0
	self.cur_prepare_commit = self.timestamp_map[client_name]
	self.msg_map_mutex.Unlock()

	// send prepare message to all servers
	self.network_mutex.Lock()
	for _, conn := range self.server_conn {
		conn.Write([]byte(wrapMsg("PREPARE " + client_name)))
	}
	self.network_mutex.Unlock()

	go voteForSelf(client_name)

	// wait for all servers to reply
	committed := false
	select {
	case <-time.After(time.Duration(1200) * time.Millisecond):
		committed = sendRealcommitMsg(client_name)
	case <-self.commit_chan:
		committed = sendRealcommitMsg(client_name)
	}
	self.msg_map_mutex.Lock()
	self.in_prepare = false
	self.msg_map_mutex.Unlock()
	if committed {
		return "COMMIT OK"
	} else {
		return "ABORTED"
	}
}

func realCommit(client_name string) {
	self.msg_map_mutex.Lock()
	defer self.msg_map_mutex.Unlock()
	timestamp := self.timestamp_map[client_name]
	writes, exists := self.write_map[timestamp]
	if !exists {
		return
	}
	self.accounts_mutex.Lock()
	defer self.accounts_mutex.Unlock()
	for _, account := range writes {
		_, exist := self.accounts[account]
		if exist {
			self.accounts[account] += self.TW[account][timestamp]
		} else {
			self.accounts[account] = self.TW[account][timestamp]
		}
		self.commit_map[account] = timestamp
		delete(self.TW[account], timestamp)
	}
}

func sendRealcommitMsg(client_name string) bool {
	self.msg_map_mutex.Lock()
	votes := self.num_votes
	self.msg_map_mutex.Unlock()
	committed := (votes == self.num_nodes)

	if committed {
		for _, conn := range self.server_conn {

			self.network_mutex.Lock()
			conn.Write([]byte(wrapMsg("REALCOMMIT " + client_name)))
			self.network_mutex.Unlock()
		}
	} else {
		abort_transaction(client_name)
	}
	return committed
}

func voteForSelf(client_name string) {
	if !checkTWValidity(client_name) {
		self.commit_chan <- true
		return
	}
	self.msg_map_mutex.Lock()
	defer self.msg_map_mutex.Unlock()
	self.num_votes++
	if self.num_votes == self.num_nodes {
		self.commit_chan <- true
	}
}

func checkTimestamp(client_name string) (int, string) {
	self.msg_map_mutex.Lock()
	defer self.msg_map_mutex.Unlock()
	t_timestamp := self.timestamp_map[client_name]
	accounts, exist := self.write_map[t_timestamp]
	if !exist {
		return 0, ""
	}
	last_uncommitted_timestamp := 0
	last_uncommitted_account := ""
	for _, account := range accounts {
		tw_account := self.TW[account]
		for timestamp := range tw_account {
			if timestamp > last_uncommitted_timestamp && timestamp < t_timestamp {
				last_uncommitted_timestamp = timestamp
				last_uncommitted_account = account
			}
		}
	}
	return last_uncommitted_timestamp, last_uncommitted_account
}

func checkTWValidity(client_name string) bool {
	self.msg_map_mutex.Lock()
	defer self.msg_map_mutex.Unlock()
	timestamp := self.timestamp_map[client_name]
	accounts, exist := self.write_map[timestamp]
	if !exist {
		return true
	}
	for _, account := range accounts {
		tw_account := self.TW[account]
		self.accounts_mutex.Lock()
		cur_remains, exist := self.accounts[account]
		if !exist {
			cur_remains = 0
		}
		self.accounts_mutex.Unlock()
		for tx_timestamp := range tw_account {
			if tx_timestamp > timestamp {
				continue
			}
			cur_remains += tw_account[tx_timestamp]
		}
		if cur_remains < 0 {
			return false
		}
	}
	return true
}

// Check a writing action can be added into TW
func writeRuleCheck(account string, client_name string) bool {
	self.msg_map_mutex.Lock()
	defer self.msg_map_mutex.Unlock()
	t := self.timestamp_map[client_name]

	// timestamp >= RTS
	read_max_timestamp := findMax(self.RTS[account])
	if t < read_max_timestamp {
		return false
	}

	// timestamp >= max ommitted version timestamp
	last_commit_write, exists := self.commit_map[account]
	if exists && last_commit_write >= t {
		return false
	}
	return true
}

func findMax(array []int) int {
	max := 0
	for i := 0; i < len(array); i++ {
		if max > array[i] {
			max = array[i]
		}
	}
	return max
}

// If exists uncommitted version in TW which timestamp is smaller
func exists_less_uncommitted(timestamp int, account string) bool {
	uncommitted_ts, exists := self.TW[account]
	// does not include
	if !exists {
		return false
	}
	for t := range uncommitted_ts {
		if t < timestamp {
			return true
		}
	}
	return false
}
func wrapMsg(msg string) string {
	return "0 " + self.branch + " " + msg + "\n"
}

func abort_transaction(client_name string) {
	self.msg_map_mutex.Lock()
	defer self.msg_map_mutex.Unlock()
	t := self.timestamp_map[client_name]
	write_objects, exists := self.write_map[t]
	// clear out all modification on write_map
	if exists {
		for _, write_account := range write_objects {
			delete(self.TW[write_account], t)
		}
	}
	// send abort messages
	for _, conn := range self.server_conn {
		self.network_mutex.Lock()
		conn.Write([]byte(wrapMsg("ABORT " + client_name)))
		self.network_mutex.Unlock()
	}
}

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
