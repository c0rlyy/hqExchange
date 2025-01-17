package main

import (
	"fmt"
	"log"

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

// TODO put this in server strcut and make handlers methods
var Mb = NewMessageBroker()

// starts go rutine that will lisen to incoming messages and send them back to connected clietn
// will error out if topic does not exits
func subscribeHandler(message *Message, connection *websocket.Conn) {
	log.Println("subscribe action received")
	topic, ok := message.Headers["topic"]
	if !ok {
		log.Println("subscribe action missing topic")
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), "no topic was provided while publishing")
		errorBackClient(errMsg, connection)
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
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), fmt.Sprintf("error while sub topic %v:%v", topic, err))
		errorBackClient(errMsg, connection)
		return
	}

	// lisineng for incoming messages and sendim them back to client
	go func(sub *Subscriber) {
		defer sub.Close()
		for {
			if sub.Closed {
				log.Println("Channel Was closed")
				return
			}
			message := <-sub.Chan
			if err := connection.WriteMessage(websocket.BinaryMessage, message); err != nil {
				log.Println("Conneciton errro closing the channel")
				// sub.Close()
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
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), "no topic was provided while publishing")
		errorBackClient(errMsg, conn)
		return
	}
	if err := Mb.Publish(topic, msg.SerializeMessage()); err != nil {
		log.Printf("error %v", err)
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), fmt.Sprintf("error in topic %v:%v", topic, err))
		errorBackClient(errMsg, conn)
		return
	}
}

func popHandler(msg *Message, conn *websocket.Conn) {
	log.Println("recived pop action")
	topic, ok := msg.Headers["topic"]
	if !ok {
		log.Println("no topic in headers")
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), "no topic was provided while popping")
		errorBackClient(errMsg, conn)
		return
	}
	if err := Mb.PopMessage(topic); err != nil {
		log.Printf("error %v", err)
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), fmt.Sprintf("error in topic %v:%v", topic, err))
		errorBackClient(errMsg, conn)
		return
	}
}

func globalSubHandler(msg *Message, conn *websocket.Conn) {
	log.Println("recived global subsciber action")
	sub, err := Mb.SubscribeGlobal()
	if err != nil {
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), fmt.Sprintf("error while global sub is: %v", err))
		errorBackClient(errMsg, conn)
		return
	}
	go func(sub *Subscriber) {
		for {
			if sub.Closed {
				log.Println("Channel Was closed")
				return
			}
			message := <-sub.Chan
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
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), "topic wasnt provided in header")
		errorBackClient(errMsg, conn)
		return
	}
	if err := Mb.AddTopic(topic); err != nil {
		log.Printf("error %v", err)
		errMsg := NewAutoLengthMessage(Merror, make(map[string]string), fmt.Sprintf("error is %v", err))
		errorBackClient(errMsg, conn)
		return
	}
}

// Sends error message back to client, can error but dunno what to do with that
func errorBackClient(msg *Message, conn *websocket.Conn) error {
	messageBytes := msg.SerializeMessage()
	if err := conn.WriteMessage(websocket.BinaryMessage, messageBytes); err != nil {
		log.Printf("Error sending message to client: %v", err)
		return err
	}
	log.Println("Error message sent to client successfully")
	return nil
}
