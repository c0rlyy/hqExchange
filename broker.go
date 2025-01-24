package main

import (
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
)

type Topic = string

type Data = []byte

type Que = []Data

type EventNotification = Data

type Subscribers = []*Subscriber

// notifications about events might not go trough in odrder they were added
// when there is a lot of them and they are not actibvly consumed
// TODO maybe introduce observer pattern in go intead of eventChannels
// TODO maybe return pointer to que so you dont have to specify which que by topic every time
type MessageBroker struct {
	GlobalSubscribers []*Subscriber
	mu                sync.RWMutex
	Topics            map[Topic]*TopicContext
	EventStream       chan EventNotification
}

// error type for message broker
// TODO create specifing viarables of message broker err that are eqivaluent to errors eq, errEmptyQue, errPopping
type MessageBrokerErr struct {
	// broker may feel overly filled with errors
	// and while its true they they are not necessary, they do limit unpredicted behaviour
	// forcing to be more explicit in your actions
	Msg      string // the actual message
	Op       string // in what opeartion it failed, maybe usless
	Severity string // how serious the erros is eq. "warnign", "critical"
	Context  string // aditional context, like topic name or event name etc
}

func (e *MessageBrokerErr) Error() string {
	return fmt.Sprintf("[%s] %s: %s, Context: %s", e.Severity, e.Op, e.Msg, e.Context)
}

type TopicContext struct {
	mu          sync.Mutex
	Queue       Que
	Subscribers []*Subscriber
}

type Subscriber struct {
	Ch     chan Data
	Closed bool
}

func NewMessageBroker() *MessageBroker {
	mb := &MessageBroker{
		Topics:            make(map[Topic]*TopicContext),
		GlobalSubscribers: make([]*Subscriber, 0),
		mu:                sync.RWMutex{},
		EventStream:       make(chan EventNotification, 10000),
	}
	go mb.lisenToGlobalEvents()
	return mb
}

func NewMessageBrokerErr(msg, op, severity, context string) *MessageBrokerErr {
	return &MessageBrokerErr{
		Msg:      msg,
		Op:       op,
		Severity: severity,
		Context:  context,
	}
}

func NewTopicContext() *TopicContext {
	return &TopicContext{
		mu:          sync.Mutex{},
		Queue:       make([][]byte, 0),
		Subscribers: make([]*Subscriber, 0),
	}
}

func NewSubscriber() *Subscriber {
	return &Subscriber{
		Closed: false,
		Ch:     make(chan []byte, 100),
	}
}

func (s *Subscriber) close() {
	if !s.Closed {
		close(s.Ch)
		s.Closed = true
	}
}

func (s *Subscriber) sendEvent(e EventNotification) error {
	if s.Closed {
		return NewMessageBrokerErr("channel is closed", "", "", "")
	}
	select {
	case s.Ch <- e:
		return nil
	default:
		return NewMessageBrokerErr("channel is full", "", "", "")
	}

}

func (tc *TopicContext) notifyAllSubs(e EventNotification) {
	for _, sub := range tc.Subscribers {
		if err := sub.sendEvent(e); err != nil {
			log.Println(err)
		}
	}
}
func (mb *MessageBroker) sendGlobalEvent(e EventNotification) error {
	select {
	case mb.EventStream <- e:
		return nil
	default:
		log.Printf("Event stream is full. Failed to send event: %+v", e)
		return NewMessageBrokerErr("error while sending on global channel", "", "", "")
	}
}

func (mb *MessageBroker) lisenToGlobalEvents() error {
	for {
		event := <-mb.EventStream
		for _, sub := range mb.GlobalSubscribers {
			if err := sub.sendEvent(event); err != nil {
				log.Println(err)
			}
		}
	}
}

func (tc *TopicContext) dispatchEventConcurrently(e EventNotification) error {
	if len(tc.Subscribers) == 0 {
		return errors.New("no subscribers in the slice")
	}
	var wg sync.WaitGroup
	numOfWorksers := len(tc.Subscribers)
	wg.Add(numOfWorksers)
	for i := 0; i < numOfWorksers; i++ {
		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			sub := tc.Subscribers[i]
			if err := sub.sendEvent(e); err != nil {
				log.Println(err)
			}
		}(&wg, i)
	}
	wg.Wait()
	return nil
}

// func dispatchFnConcurnetlySlice(slice []any,fn func()){

// }

func (mb *MessageBroker) AddTopic(topic Topic) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if _, exists := mb.Topics[topic]; exists {
		return NewMessageBrokerErr("topic already exits", "", "", "")
	}
	mb.Topics[topic] = NewTopicContext()
	headers := map[string]string{
		"topic": topic,
	}

	msg := NewAutoLengthMessage(AddTopic, headers, "added new topic")
	if err := mb.sendGlobalEvent(msg.SerializeMessage()); err != nil {
		return err
	}
	return nil
}

func (mb *MessageBroker) Publish(topic Topic, payload Data) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	tcContext, exists := mb.Topics[topic]
	if !exists {
		return NewMessageBrokerErr("topic doesnt exits", "", "", "")
	}
	tcContext.Queue = append(tcContext.Queue, payload)

	tcContext.notifyAllSubs(payload)
	if err := mb.sendGlobalEvent(payload); err != nil {
		return err
	}
	return nil
}

func (mb *MessageBroker) Pop(topic Topic) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	tcContext, exists := mb.Topics[topic]
	if !exists {
		return NewMessageBrokerErr("topic  doesnt exixts", "", "", "")
	}
	if len(tcContext.Queue) == 0 {
		return NewMessageBrokerErr("nothing in the que", "", "", "")
	}

	msg := tcContext.Queue[0]
	tcContext.Queue = slices.Clone(tcContext.Queue[:1])
	prsedMsg, _ := ParseMessage(msg)
	prsedMsg.Action = Pop
	serializedMsg := prsedMsg.SerializeMessage()

	tcContext.dispatchEventConcurrently(serializedMsg)
	mb.sendGlobalEvent(serializedMsg)
	return nil
}

func (mb *MessageBroker) SubscribeTopic(topic Topic) (*Subscriber, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	tcContext, exists := mb.Topics[topic]
	if !exists {
		return nil, NewMessageBrokerErr("topic  doesnt exixts", "", "", "")
	}
	sub := NewSubscriber()
	tcContext.Subscribers = append(tcContext.Subscribers, sub)
	headers := map[string]string{
		"topic": topic,
	}
	msg := NewAutoLengthMessage(Subscribe, headers, " new topic subsucber topic")
	mb.sendGlobalEvent(msg.SerializeMessage())
	return sub, nil
}

func (mb *MessageBroker) GlobalSubscribe() (*Subscriber, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	sub := NewSubscriber()
	mb.GlobalSubscribers = append(mb.GlobalSubscribers, sub)
	return sub, nil
}

func (mb *MessageBroker) QueGetAll(topic Topic) ([]Data, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	tcContext, exists := mb.Topics[topic]
	if !exists {
		return nil, NewMessageBrokerErr("topic  doesnt exixts", "", "", "")
	}
	return tcContext.Queue, nil

}
