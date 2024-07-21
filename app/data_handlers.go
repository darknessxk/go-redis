package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

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
