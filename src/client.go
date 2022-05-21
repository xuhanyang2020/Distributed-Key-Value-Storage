package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
)

type Client struct {
	host_name        string
	server_names     []string
	server_addresses map[string]string
	server_ports     map[string]string
	connections      []net.Conn
	buf_readers      []*bufio.Reader
}

var client Client

func init_client() {
	client.server_addresses = make(map[string]string)
	client.server_ports = make(map[string]string)
}

func readConfig(config string) {
	file, err := os.Open(config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		line_split := strings.Split(line, " ")
		client.server_names = append(client.server_names, line_split[0])
		client.server_addresses[line_split[0]] = line_split[1]
		client.server_ports[line_split[0]] = line_split[2]
	}
}

func connectServers() {
	for _, server_name := range client.server_names {
		server_address := client.server_addresses[server_name]
		server_port := client.server_ports[server_name]
		server_address_port := server_address + ":" + server_port
		tcp_address, _ := net.ResolveTCPAddr("tcp", server_address_port)
		conn, err := net.DialTCP("tcp", nil, tcp_address)
		for err != nil {
			conn, err = net.DialTCP("tcp", nil, tcp_address)
		}
		conn.Write([]byte(wrapMsgClient("INIT")))
		client.connections = append(client.connections, conn)
		client.buf_readers = append(client.buf_readers, bufio.NewReader(conn))
	}
}

func wrapMsgClient(msg string) string {
	return "1 " + client.host_name + " " + msg + "\n"
}

func main() {
	// parse the command line
	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Println("Usage: client <host> <port>")
		os.Exit(1)
	}
	init_client()
	client.host_name = args[0]
	config := args[1]
	readConfig(config)
	connectServers()

	// transaction flag
	in_transaction := false

	// randomly generated server number
	server := -1

	// read from stdin
	reader := bufio.NewReader(os.Stdin)
	for {
		line, _ := reader.ReadString('\n')
		line = strings.TrimSuffix(line, "\n")
		if len(line) == 0 {
			continue
		}
		command := strings.Split(line, " ")[0]
		switch command {
		case "BEGIN":
			if in_transaction {
				fmt.Println("Already in transaction, cannot start a new one")
				continue
			}
			in_transaction = true
			server = rand.Intn(len(client.server_names))
			client.connections[server].Write([]byte(wrapMsgClient(line)))

			resp, err := client.buf_readers[server].ReadString('\n')
			if err != nil {
				fmt.Println(err)
				continue
			}
			resp = strings.TrimSuffix(resp, "\n")
			fmt.Println(resp)
			if strings.Compare(resp, "OK") != 0 {
				in_transaction = false
			}
		case "DEPOSIT":
			fallthrough
		case "WITHDRAW":
			if !in_transaction {
				fmt.Println("Not in transaction, cannot " + command)
				continue
			}
			client.connections[server].Write([]byte(wrapMsgClient(line)))

			resp, err := client.buf_readers[server].ReadString('\n')
			if err != nil {
				fmt.Println(err)
				continue
			}
			resp = strings.TrimSuffix(resp, "\n")
			fmt.Println(resp)
			if strings.Compare(resp, "OK") != 0 {
				in_transaction = false
			}
		case "BALANCE":
			if !in_transaction {
				fmt.Println("Not in transaction, cannot " + command)
				continue
			}
			client.connections[server].Write([]byte(wrapMsgClient(line)))

			resp, err := client.buf_readers[server].ReadString('\n')
			if err != nil {
				fmt.Println(err)
				continue
			}
			resp = strings.TrimSuffix(resp, "\n")
			fmt.Println(resp)
			resp = strings.TrimSuffix(resp, "\n")
			if strings.Compare(resp, "NOT FOUND, ABORTED") == 0 {
				in_transaction = false
			}
		case "COMMIT":
			if !in_transaction {
				fmt.Println("Not in transaction, cannot " + command)
				continue
			}
			in_transaction = false
			client.connections[server].Write([]byte(wrapMsgClient(line)))

			resp, err := client.buf_readers[server].ReadString('\n')
			if err != nil {
				fmt.Println(err)
				continue
			}
			resp = strings.TrimSuffix(resp, "\n")
			fmt.Println(resp)
		case "ABORT":
			if !in_transaction {
				fmt.Println("Not in transaction, cannot " + command)
				continue
			}
			in_transaction = false
			client.connections[server].Write([]byte(wrapMsgClient(line)))

			resp, err := client.buf_readers[server].ReadString('\n')
			if err != nil {
				fmt.Println(err)
				continue
			}
			resp = strings.TrimSuffix(resp, "\n")
			fmt.Println(resp)
		default:
		}
	}
}
