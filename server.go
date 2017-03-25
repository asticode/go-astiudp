package astiudp

import (
	"encoding/json"
	"net"
	"time"

	"github.com/rs/xlog"
)

// Constants
const (
	maxMessageSize = 4096
	sleepError     = 5 * time.Second
)

// Event names
const (
	EventNameStart      = "astiudp.start"
	EventNameDisconnect = "astiudp.disconnect"
)

// ListenerFunc represents the listener func
type ListenerFunc func(s *Server, eventName string, payload json.RawMessage, addr *net.UDPAddr) error

// Server represents an UDP server
type Server struct {
	addr      *net.UDPAddr
	conn      *net.UDPConn
	listeners map[string][]ListenerFunc
	Logger    xlog.Logger
}

// NewServer returns a new UDP server
func NewServer() *Server {
	return &Server{
		listeners: make(map[string][]ListenerFunc),
		Logger:    xlog.NopLogger,
	}
}

// Init initializes the server
func (s *Server) Init(addr string) (err error) {
	// Resolve addr
	if s.addr, err = net.ResolveUDPAddr("udp4", addr); err != nil {
		return
	}
	return
}

// Close closes the server
func (s *Server) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

// ListenAndRead listens and reads from the server
func (s *Server) ListenAndRead() {
	// For loop to handle reconnecting
	var err error
	for {
		// Listen
		if err = s.Listen(); err != nil {
			s.Logger.Errorf("%s while listen to %s, sleeping %s before retrying", err, s.addr, sleepError)
			time.Sleep(sleepError)
			continue
		}
		s.Logger.Debugf("Listening on %s", s.addr)

		// Execute start listeners
		if err = s.executeListeners(EventNameStart, json.RawMessage{}, nil); err != nil {
			s.Logger.Errorf("%s while executing start listeners, sleeping %s before retrying", err, s.addr, sleepError)
			time.Sleep(sleepError)
			continue
		}

		// Read
		if err = s.Read(); err != nil {
			s.Logger.Errorf("%s while reading from %s, sleeping %s before retrying", err, s.addr, sleepError)
			time.Sleep(sleepError)
			continue
		}
	}
}

// Listen listens to the addr
func (s *Server) Listen() (err error) {
	// Make sure the previous conn is closed
	if s.conn != nil {
		s.conn.Close()
	}

	// Listen
	s.conn, err = net.ListenUDP("udp", s.addr)
	return
}

// Event represents an event sent between a client and a server
type Event struct {
	Name    string      `json:"name"`
	Payload interface{} `json:"payload"`
}

// Write writes an event to the specific addr
func (s *Server) Write(eventName string, payload interface{}, addr *net.UDPAddr) (err error) {
	// Marshal
	var b []byte
	if b, err = json.Marshal(Event{Payload: payload, Name: eventName}); err != nil {
		return
	}

	// Write
	if _, err = s.conn.WriteToUDP(b, addr); err != nil {
		return
	}
	return
}

// EventRead overrides Event to allows proper unmarshaling of the payload
type EventRead struct {
	Event
	Payload json.RawMessage `json:"payload"`
}

// Read reads from the server
func (s *Server) Read() error {
	defer s.executeListeners(EventNameDisconnect, json.RawMessage{}, nil)

	var buf = make([]byte, maxMessageSize)
	var err error
	for {
		// Read from UDP
		var n int
		var addr *net.UDPAddr
		if n, addr, err = s.conn.ReadFromUDP(buf[0:]); err != nil {
			return err
		}

		// Execute the rest in a goroutine
		go func(n int, addr *net.UDPAddr, buf []byte) {
			// Unmarshal event
			var e EventRead
			if err = json.Unmarshal(buf[:n], &e); err != nil {
				s.Logger.Errorf("%s while unmarshaling %s", buf[:n])
				return
			}

			// Execute listeners
			if err = s.executeListeners(e.Name, e.Payload, addr); err != nil {
				s.Logger.Errorf("%s while executing listeners of event %+v for addr %s", err, e, addr)
				return
			}
		}(n, addr, buf)
	}
}

// executeListeners executes listeners for a specific event
func (s *Server) executeListeners(eventName string, payload json.RawMessage, addr *net.UDPAddr) (err error) {
	for _, l := range s.listeners[eventName] {
		if err = l(s, eventName, payload, addr); err != nil {
			return
		}
	}
	return
}

// AddListener adds a listener
func (s *Server) AddListener(eventType string, l ListenerFunc) {
	s.listeners[eventType] = append(s.listeners[eventType], l)
}

// SetListener sets a listener
func (s *Server) SetListener(eventType string, l ListenerFunc) {
	s.listeners[eventType] = []ListenerFunc{l}
}
