package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

// Data Handlers

type DataHandler interface {
	serialize() (string, error)
	deserialize(data *[]byte) error
}

// GetDataHandler
//
// Returns the proper data handler based on the first byte of the data
func GetDataHandler(data []byte, args ...any) DataHandler {
	var execute bool
	if len(args) > 0 {
		execute = args[0].(bool)
	} else {
		execute = true
	}

	parser := func() DataHandler {
		switch data[0] {
		case '*':
			return &Elements{}
		case '+':
			return &SimpleString{}
		case '$':
			return &BulkString{}
		case ':':
			return &Integer{}
		case '-':
			return &Error{}
		case '#':
			return &Boolean{}
		case ',':
			return &Double{}
		case '(':
			return &BigNumber{}
		case '!':
			return &BulkError{}
		case '=':
			return &VerbatimString{}
		case '%':
			return &Map{}
		case '~':
			return &Set{}
		case '>':
			return &Pushes{}
		default:
			return nil
		}
	}()

	if execute {
		if parser.deserialize(&data) != nil {
			return nil
		}
	}

	return parser
}

// Elements
//
// Implementing proper data handling for handling bulk requests from CLIENTS
type Elements struct {
	Count    int
	Elements []DataHandler
}

func (e *Elements) serialize() (string, error) {
	str := ""
	str += "*" + strconv.Itoa(e.Count) + "\r\n"

	for _, element := range e.Elements {
		serialized, err := element.serialize()

		if err != nil {
			return "", err
		}

		str += serialized
	}

	return str, nil
}

func (e *Elements) deserialize(data *[]byte) error {
	if (*data)[0] != '*' {
		return errors.New("invalid elements data")
	}

	countOffset := 1
	countEnd := strings.Index(string(*data), "\r\n")

	count, err := strconv.Atoi(string((*data)[countOffset:countEnd]))

	if err != nil {
		return err
	}

	e.Count = count

	*data = (*data)[countEnd+2:]
	dataHandler := GetDataHandler(*data)

	for i := 0; i < count; i++ {
		if dataHandler == nil {
			return errors.New("invalid data type")
		}

		e.Elements = append(e.Elements, dataHandler)
	}

	return nil
}

// SimpleString
//
// Implementing proper data handling
type SimpleString struct {
	value string
}

func (s *SimpleString) serialize() (string, error) {
	return fmt.Sprintf("+%s\r\n", s.value), nil
}

func (s *SimpleString) deserialize(data *[]byte) error {
	if (*data)[0] != '+' {
		return errors.New("invalid simple string data")
	}

	dataEnd := strings.Index(string(*data), "\r\n")
	s.value = string((*data)[1:dataEnd])
	*data = (*data)[dataEnd+2:]
	return nil
}

// BulkString
//
// Implementing proper data handling
type BulkString struct {
	value string
}

func (b *BulkString) serialize() (string, error) {
	if b.value == "" {
		return "$-1\r\n", nil
	} else if len(b.value) == 0 {
		return "$0\r\n\r\n", nil
	} else {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(b.value), b.value), nil
	}
}

func (b *BulkString) deserialize(data *[]byte) error {
	if (*data)[0] != '$' {
		return errors.New("invalid bulk string data")
	}

	fragmentOffset := 1
	fragmentEnd := strings.Index(string(*data), "\r\n")

	bufLen, err := strconv.Atoi(string((*data)[fragmentOffset:fragmentEnd]))

	*data = (*data)[fragmentEnd+2:]

	if err != nil {
		return err
	} else if -1 == bufLen {
		return nil
	}

	buf := make([]byte, bufLen)

	if copy(buf, *data) != bufLen {
		return errors.New("invalid bulk string data")
	}

	*data = (*data)[bufLen+2:]
	b.value = string(buf)

	return nil
}

// Integer
//
// Implementing proper data handling
type Integer struct {
	value int
}

func (i *Integer) serialize() (string, error) {
	return fmt.Sprintf(":%d\r\n", i.value), nil
}

func (i *Integer) deserialize(data *[]byte) error {
	if (*data)[0] != ':' {
		return errors.New("invalid integer data")
	}

	dataEnd := strings.Index(string(*data), "\r\n")
	value, err := strconv.Atoi(string((*data)[1:dataEnd]))

	if err != nil {
		return err
	}

	i.value = value
	*data = (*data)[dataEnd+2:]

	return nil
}

// Error
//
// Implementing proper data handling
type Error struct {
	value string
}

func (e *Error) serialize() (string, error) {
	return "-" + e.value + "\r\n", nil
}

func (e *Error) deserialize(data *[]byte) error {
	if (*data)[0] != '-' {
		return errors.New("invalid error data")
	}

	dataEnd := strings.Index(string(*data), "\r\n")
	e.value = string((*data)[1:dataEnd])
	*data = (*data)[dataEnd+2:]
	return nil
}

// Boolean
//
// Implementing proper data handling
type Boolean struct {
	value bool
}

func (b *Boolean) serialize() (string, error) {
	if b.value {
		return "#t\r\n", nil
	} else {
		return "#f\r\n", nil
	}
}

func (b *Boolean) deserialize(data *[]byte) error {
	if (*data)[0] != '#' {
		return errors.New("invalid boolean data")
	}

	dataEnd := strings.Index(string(*data), "\r\n")
	value := string((*data)[1:dataEnd])

	b.value = value == "t" || value == "T"

	*data = (*data)[dataEnd+2:]

	return nil
}

// Double
//
// Implementing proper data handling
type Double struct {
	value float64
}

func (d *Double) serialize() (string, error) {
	return "," + strconv.FormatFloat(d.value, 'f', -1, 64) + "\r\n", nil
}

func (d *Double) deserialize(data *[]byte) error {
	if (*data)[0] != ',' {
		return errors.New("invalid double data")
	}

	dataEnd := strings.Index(string(*data), "\r\n")
	value, err := strconv.ParseFloat(string((*data)[1:dataEnd]), 64)

	if err != nil {
		return err
	}

	d.value = value
	*data = (*data)[dataEnd+2:]

	return nil
}

// BigNumber
//
// Implementing proper data handling
// Unimplemented yet
type BigNumber struct{}

func (b *BigNumber) serialize() (string, error) {
	return "", errors.New("not implemented")
}

func (b *BigNumber) deserialize(data *[]byte) error {
	return errors.New("not implemented")
}

// BulkError
//
// Implementing proper data handling
// Unimplemented yet
type BulkError struct {
	value []string
}

func (b *BulkError) serialize() (string, error) {
	return "", nil
}

func (b *BulkError) deserialize(data *[]byte) error {
	return nil
}

// VerbatimString
//
// Implementing proper data handling
// Unimplemented yet
type VerbatimString struct {
	value string
}

func (v *VerbatimString) serialize() (string, error) {
	return "", nil
}

func (v *VerbatimString) deserialize(data *[]byte) error {
	return nil
}

// Map
//
// Implementing proper data handling
// Unimplemented yet
type Map struct {
	value map[string]string
}

func (m *Map) serialize() (string, error) {
	return "", nil
}

func (m *Map) deserialize(data *[]byte) error {
	return nil
}

// Set
//
// Implementing proper data handling
// Unimplemented yet
type Set struct {
	value []string
}

func (s *Set) serialize() (string, error) {
	return "", nil
}

func (s *Set) deserialize(data *[]byte) error {
	return nil
}

// Pushes
//
// Implementing proper data handling
// Unimplemented yet
type Pushes struct {
	value []string
}

func (p *Pushes) serialize() (string, error) {
	return "", nil
}

func (p *Pushes) deserialize(data *[]byte) error {
	return nil
}

// Null
//
// Implementing proper data handling
type Null struct{}

func (n *Null) serialize() (string, error) {
	return "_\r\n", nil
}

func (n *Null) deserialize(data *[]byte) error {
	if (*data)[0] != '_' {
		return errors.New("invalid null data")
	}

	*data = (*data)[3:]
	return nil
}

// Commands

type Command interface {
	Execute(args ...any) DataHandler
}

func GetCommand(base *Elements) Command {
	command := strings.ToUpper(base.Elements[0].(*BulkString).value)
	switch command {
	case "PING":
		return &CommandPing{}
	case "ECHO":
		return &CommandEcho{}
	default:
		return nil
	}
}

type CommandPing struct{}

func (c *CommandPing) Execute(args ...any) DataHandler {
	return &SimpleString{value: "PONG"}
}

type CommandEcho struct {
	args []string
}

func (c *CommandEcho) Execute(args ...any) DataHandler {
	return &BulkString{value: c.args[0]}
}

// Server

func closeCloser(c io.Closer, errPtr *error) {
	err := c.Close()
	if *errPtr == nil {
		*errPtr = err
	}
}

func handleRequest(conn net.Conn) (bool, error) {
	maxBufLen := 2048
	buf := make([]byte, maxBufLen)

	reqLen, err := conn.Read(buf)

	if err != nil {
		fmt.Println("[!] Error reading:", err.Error())
		return true, err
	}

	fmt.Printf("[+] Received %d bytes\n", reqLen)

	parser := GetDataHandler(buf[:reqLen]).(*Elements)
	command := GetCommand(parser)

	if command == nil {
		fmt.Println("[!] Invalid command")
		return true, nil
	}

	response := command.Execute()
	serialized, err := response.serialize()

	if err != nil {
		fmt.Println("[!] Error serializing:", err.Error())
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
