package main

import "strings"

type Command interface {
	Execute() DataHandler
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

func (c *CommandPing) Execute() DataHandler {
	return &SimpleString{value: "PONG"}
}

type CommandEcho struct {
	args []string
}

func (c *CommandEcho) Execute() DataHandler {
	return &BulkString{value: c.args[0]}
}
