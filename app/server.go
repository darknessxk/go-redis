package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

func closeClient(c io.Closer, errPtr *error) {
	err := c.Close()
	if *errPtr == nil {
		*errPtr = err
	}
}

func handleRequest(conn net.Conn) error {
	maxBufLen := 2048
	buf := make([]byte, maxBufLen)

	reqLen, err := conn.Read(buf)

	if err != nil {
		fmt.Println("[!] Error reading:", err.Error())
		return err
	}

	fmt.Printf("[+] Received %d bytes\n", reqLen)

	parser := GetDataHandler(buf[:reqLen]).(*Elements)
	command := GetCommand(parser)

	if command == nil {
		fmt.Println("[!] Invalid command")
		return nil
	}

	response := command.Execute()
	serialized, err := response.serialize()

	if err != nil {
		fmt.Println("[!] Error serializing:", err.Error())
		return err
	}

	fmt.Printf("[+] Sending %d bytes\n", len(serialized))
	_, err = conn.Write([]byte(serialized))

	return nil
}

func runServer(listen *string, port *int) error {
	fmt.Println("[+] Starting Kat Redis")

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *listen, *port))

	if err != nil {
		return err
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("[!] Error accepting connection: ", err.Error())
		return err
	}

	defer closeClient(conn, &err)
	fmt.Println("[+] Connection accepted")

	err = handleRequest(conn)
	if err != nil {
		fmt.Println("[!] Error handling request: ", err.Error())
	}

	return err
}

func main() {

	listen := flag.String("listen", "0.0.0.0", "Address to listen on")
	if len(*listen) < 1 {
		fmt.Println("[!] Invalid listen address")
		os.Exit(1)
	} else if net.ParseIP(*listen) == nil {
		fmt.Println("[!] Invalid listen address")
		os.Exit(1)
	}

	port := flag.Int("port", 6379, "Port to listen on")
	if int(*port) > 65535 {
		fmt.Println("[!] Invalid port number")
		os.Exit(1)
	} else if int(*port) < 1 {
		fmt.Println("[!] Invalid port number")
		os.Exit(1)
	}

	err := runServer(listen, port)
	if err != nil {
		fmt.Println("[!] Error running server: ", err.Error())
		os.Exit(1)
	}
}
