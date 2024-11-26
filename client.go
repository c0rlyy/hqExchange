package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

// FLX protocal actions
var Subscribe = byte(1)
var Publish = byte(2)
var Pop = byte(3)
var Unsubscribe = byte(4)
var Ping = byte(5)
var Pong = byte(6)
var Ack = byte(7)
var Nack = byte(8)
var Merror = byte(9)
var AddTopic = byte(10)
var GlobalSub = byte(11)
var DeleteTopic = byte(12)

// FLEX protocol accepted message structure
type Message struct {
	Action  byte              // 2nd part of message 1 byte
	Length  uint16            // 2 bytes, expected first value in the protocl
	Payload string            // last part of the expected message, actual data to procces
	Headers map[string]string // headers 3rd part of expected message seperated by \r\n, ends with \r\n\r\n
}

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

var Mb = NewMessageBroker()

func NewMessage(length uint16, action byte, headers map[string]string, payload string) *Message {
	return &Message{
		Length:  length,
		Action:  action,
		Headers: headers,
		Payload: payload,
	}
}

// serializes message back to binary
func (msg *Message) SerializeMessage() []byte {
	var buff bytes.Buffer

	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, uint16(msg.Length))
	buff.Write(lengthBytes)
	buff.Write([]byte{msg.Action})

	for key, val := range msg.Headers {
		var headerBuff bytes.Buffer
		// Write Header: key:value\r\n
		headerBuff.Write([]byte(key))
		headerBuff.Write([]byte(":"))
		headerBuff.Write([]byte(val))
		headerBuff.Write([]byte("\r\n"))

		buff.Write(headerBuff.Bytes())
	}
	buff.Write([]byte("\r\n\r\n"))
	buff.Write([]byte(msg.Payload))
	return buff.Bytes()
}

// parsing messsage to FLEX protocl
func ParseMessage(payload []byte) (*Message, error) {
	if len(payload) < 4 {
		return nil, errors.New("message too short")
	}
	//length to int
	// length := int(payload[0])<<8 | int(payload[1])
	lenU16 := binary.BigEndian.Uint16(payload[:2])
	length := uint16(lenU16)

	action := payload[2]
	headersAndPayload := payload[3:]

	headerEnd := bytes.Index(headersAndPayload, []byte("\r\n\r\n"))
	if headerEnd == -1 {
		log.Println("Malformed message: Missing header terminator")
		return nil, errors.New("malformed message missing header terminator")
	}

	headers := parseHeaders(headersAndPayload[:headerEnd])
	payloadData := string(headersAndPayload[headerEnd+4:]) // skiping the \r\n\r\n

	return NewMessage(length, action, headers, payloadData), nil
}

// parseHeaders makes a map of string string values
func parseHeaders(data []byte) map[string]string {
	headers := make(map[string]string)
	// spliting headers by "\r\n"
	lines := bytes.Split(data, []byte("\r\n"))
	for _, line := range lines {
		// spliting headers to key value pairs
		parts := bytes.SplitN(line, []byte(":"), 2)
		if len(parts) == 2 {
			headers[string(parts[0])] = string(parts[1])
		}
	}
	return headers
}

// starts go rutine that will lisen to incoming messages and send them back to connected clietn
// will error out if topic does not exits
func subscribeHandler(message *Message, connection *websocket.Conn) {
	log.Println("subscribe action received")
	topic, ok := message.Headers["topic"]
	if !ok {
		log.Println("subscribe action missing topic")
		connection.WriteMessage(websocket.BinaryMessage, []byte{Merror})
		return
	}
	// err := Mb.AddTopic(topic)
	// if err != nil {
	// 	log.Printf("Error subscribing to topic %s: %v", topic, err)
	// 	connection.WriteMessage(websocket.BinaryMessage, []byte{Merror})
	// 	return
	// }

	sub, err := Mb.SubscribeTopic(topic)
	if err != nil {
		log.Printf("Error subscribing to topic %s: %v", topic, err)
		connection.WriteMessage(websocket.BinaryMessage, []byte{Merror})
		return
	}

	// lisineng for incoming messages and sendim them back to client
	go func(sub *Subscriber) {
		for {
			if sub.Closed {
				log.Println("Channel Was closed")
				return
			}
			message := <-sub.Ch
			if err := connection.WriteMessage(websocket.BinaryMessage, message); err != nil {
				log.Println("Conneciton errro closing the channel")
				sub.Close()
				return
			}
		}
	}(sub)
	log.Printf("Subscribed to topic: %s", topic)
}

// publishes to a specific topic if missing topic or topic not exits it errors out back to cliebnt
func publishHandler(msg *Message, conn *websocket.Conn) {
	log.Println("recived publish action")
	topic, ok := msg.Headers["topic"]
	if !ok {
		log.Println("no topic in headers")
		conn.WriteMessage(websocket.BinaryMessage, []byte{Merror})
		return
	}
	if err := Mb.Publish(topic, msg.SerializeMessage()); err != nil {
		log.Printf("error %v", err)
		conn.WriteMessage(websocket.BinaryMessage, []byte{Merror})
		return
	}
}

func popHandler(msg *Message, conn *websocket.Conn) {
	log.Println("recived pop action")
	topic, ok := msg.Headers["topic"]
	if !ok {
		log.Println("no topic in headers")
		conn.WriteMessage(websocket.BinaryMessage, []byte{Merror})
		return
	}
	if err := Mb.PopMessage(topic); err != nil {
		log.Printf("error %v", err)
		conn.WriteMessage(websocket.BinaryMessage, []byte{Merror})
		return
	}
}

func globalSubHandler(msg *Message, conn *websocket.Conn) {
	log.Println("recived global subsciber action")
	sub, err := Mb.SubscribeGlobal()
	if err != nil {
		conn.WriteMessage(websocket.BinaryMessage, []byte{Merror})
	}
	go func(sub *Subscriber) {
		for {
			if sub.Closed {
				log.Println("Channel Was closed")
				return
			}
			message := <-sub.Ch
			if err := conn.WriteMessage(websocket.BinaryMessage, message); err != nil {
				log.Println("Conneciton errro closing the channel")
				sub.Close()
				return
			}
		}
	}(sub)

}

func addTopicHandler(msg *Message, conn *websocket.Conn) {
	log.Println("recived add topic action")
	topic, ok := msg.Headers["topic"]
	if !ok {
		log.Println("no topic in headers")
		conn.WriteMessage(websocket.BinaryMessage, []byte{Merror})
		return
	}
	if err := Mb.AddTopic(topic); err != nil {
		log.Printf("error %v", err)
		conn.WriteMessage(websocket.BinaryMessage, []byte{Merror})
		return
	}
}