package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func closeCloser(c io.Closer, errPtr *error) {
	fmt.Println("[+] Closing IO Handle")
	err := c.Close()
	if *errPtr == nil {
		*errPtr = err
	}
}

func sendError(conn net.Conn, err error) {
	fmt.Println("[!] Error: ", err.Error())
	errParsed := &Error{value: err.Error()}
	serialized, err := errParsed.serialize()

	_, err = conn.Write([]byte(serialized))

	if err != nil {
		fmt.Println("[!] Error sending error: ", err.Error())
	}
}

func handleRequest(conn net.Conn) (bool, error) {
	maxBufLen := 2048
	buf := make([]byte, maxBufLen)

	reqLen, err := conn.Read(buf)

	if err != nil {
		sendError(conn, err)
		return true, err
	}

	fmt.Printf("[+] Received %d bytes\n", reqLen)

	parser := GetDataHandler(buf[:reqLen]).(*Elements)
	command := GetCommand(parser)

	if command == nil {
		sendError(conn, err)
		return true, nil
	}

	response := command.Execute()

	if response == nil {
		sendError(conn, errors.New("UNK - IMPLEMENTED COMMAND"))
		return true, nil
	}

	serialized, err := response.serialize()

	if err != nil {
		sendError(conn, err)
		return true, err
	}

	fmt.Printf("[+] Sending %d bytes\n", len(serialized))
	_, err = conn.Write([]byte(serialized))

	return false, nil
}

func newConnection(conn net.Conn) error {
	var err error
	defer closeCloser(conn, &err)
	fmt.Println("[+] Connection accepted")

	for {
		shouldStop, err := handleRequest(conn)
		if err != nil {
			fmt.Println("[!] Error handling request: ", err.Error())
			return err
		}

		if shouldStop {
			break
		}
	}

	return err
}

func runServer(listen *string, port *int) error {
	fmt.Println("[+] Starting Kat Redis")

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *listen, *port))

	if err != nil {
		return err
	}

	defer closeCloser(l, &err)

	fmt.Printf("[+] Listening on %s:%d\n", *listen, *port)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("[+] Received SIGTERM, shutting down...")
		err := l.Close()
		if err != nil {
			fmt.Println("[!] Error closing listener: ", err.Error())
			os.Exit(1)
		}

		os.Exit(0)
	}()

	for {
		conn, err := l.Accept()

		if err != nil {
			fmt.Println("[!] Error accepting connection: ", err.Error())
			return err
		}

		go func() {
			err := newConnection(conn)
			if err != nil {
				fmt.Println("[!] Error handling connection: ", err.Error())
			}
		}()
	}
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
