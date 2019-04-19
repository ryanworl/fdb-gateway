package respmux

import (
	"strings"

	"github.com/tidwall/redcon"
)

// A Handler responds to an RESP request.
type Handler interface {
	ServeRESP(conn redcon.Conn, cmd redcon.Command)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as RESP handlers. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(conn redcon.Conn, cmd redcon.Command)

// ServeRESP calls f(w, r)
func (f HandlerFunc) ServeRESP(conn redcon.Conn, cmd redcon.Command) {
	f(conn, cmd)
}

// RESPMux is an RESP command multiplexer.
type RESPMux struct {
	handlers map[string]Handler
}

// NewRESPMux allocates and returns a new RESPMux.
func NewRESPMux() *RESPMux {
	return &RESPMux{
		handlers: make(map[string]Handler),
	}
}

// HandleFunc registers the handler function for the given command.
func (m *RESPMux) HandleFunc(command string, handler func(conn redcon.Conn, cmd redcon.Command)) {
	if handler == nil {
		panic("redcon: nil handler")
	}
	m.Handle(command, HandlerFunc(handler))
}

// Handle registers the handler for the given command.
// If a handler already exists for command, Handle panics.
func (m *RESPMux) Handle(command string, handler Handler) {
	if command == "" {
		panic("redcon: invalid command")
	}
	if handler == nil {
		panic("redcon: nil handler")
	}
	if _, exist := m.handlers[command]; exist {
		panic("redcon: multiple registrations for " + command)
	}

	m.handlers[command] = handler
}

// ServeRESP dispatches the command to the handler.
func (m *RESPMux) ServeRESP(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	if handler, ok := m.handlers[command]; ok {
		handler.ServeRESP(conn, cmd)
	} else {
		conn.WriteError("ERR unknown command '" + command + "'")
	}
}
