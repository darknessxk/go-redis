package main

import (
	"fmt"
	"net"
	"os"
)

func handleRequest(conn net.Conn) {
	buf := make([]byte, 1024)
	reqLen, err := conn.Read(buf)

	if err != nil {
		fmt.Println("[!] Error reading:", err.Error())
	}

	fmt.Println("[+] Received: ", string(buf[:reqLen]))

	if string(buf[:reqLen]) == "PING" {
		conn.Write([]byte("+PONG\r\n"))
	} else {
		conn.Write([]byte("+OK\r\n"))
	}
}

func main() {
	fmt.Println("[+] Starting Kat Redis")

	l, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("[!] Failed to bind to port 6379")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("[!] Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	fmt.Println("[+] Connection accepted")

	for {
		handleRequest(conn)
	}
}
