package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type MessageHandler func(msg *Message, conn *websocket.Conn)

var actionHandlers = map[byte]MessageHandler{
	Subscribe: subscribeHandler,
	Publish:   publishHandler,
	Pop:       popHandler,
	// Unsubscribe: unsubscribeHandler,
	// Ping:        pingHandler,
	// Pong:        pongHandler,
	// Ack:         ackHandler,
	// Nack:        nackHandler,
	// Merror:      errorHandler,
	AddTopic:  addTopicHandler,
	GlobalSub: globalSubHandler,
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Server struct {
	Mb          MessageBroker
	Connections Connections
	Addr        string
}

type Connections struct {
	WsConnections map[*websocket.Conn]bool
	mu            sync.RWMutex
}

func (s *Server) AddConnection(conn *websocket.Conn) {
	s.Connections.mu.Lock()
	defer s.Connections.mu.Unlock()
	s.Connections.WsConnections[conn] = true
}

func (s *Server) RemoveConnection(conn *websocket.Conn) {
	s.Connections.mu.Lock()
	defer s.Connections.mu.Unlock()
	delete(s.Connections.WsConnections, conn)
}

// handles connection
func handleConnection(w http.ResponseWriter, r *http.Request) {
	connection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer connection.Close()

	for {
		mt, message, err := connection.ReadMessage()
		if err != nil || mt == websocket.CloseMessage {
			break
		}
		msg, err := ParseMessage(message)
		if err != nil {
			connection.WriteMessage(websocket.BinaryMessage, []byte{Merror})
			continue
		}
		handleMessage(msg, connection)
	}
}

func handleMessage(msg *Message, conn *websocket.Conn) {
	if handler, exists := actionHandlers[msg.Action]; exists {
		handler(msg, conn)
		return
	}
	log.Printf("unkown action reveivedd: %d", msg.Action)
	conn.WriteMessage(websocket.BinaryMessage, []byte{Merror})
}

func main() {
	http.HandleFunc("/ws", handleConnection)
	serverAddr := ":31999"
	log.Printf("Server started at %s", serverAddr)
	log.Panicln(http.ListenAndServe(serverAddr, nil))

}
