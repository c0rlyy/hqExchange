package main

import (
	"bytes"
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
	Subscribers       map[Topic]Subscribers
	mu                sync.RWMutex
	Topics            map[Topic]Que
}

// subscriber for topics
type Subscriber struct {
	Id     string                 // maybe will use this sometime
	Closed bool                   // need this for reall importnat reason im a moron
	Ch     chan EventNotification // channel to send to notification
	mu     *sync.RWMutex          // might be usless dont know yet, but could make mb lsightl faster if used correctly
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

func NewMessageBroker() *MessageBroker {
	mb := &MessageBroker{
		Subscribers:       make(map[Topic]Subscribers),
		Topics:            make(map[Topic]Que),
		GlobalSubscribers: make([]*Subscriber, 0),
	}
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
func (e *MessageBrokerErr) Error() string {
	return fmt.Sprintf("[%s] %s: %s, Context: %s", e.Severity, e.Op, e.Msg, e.Context)
}

// channel is bufferd, default size is 100
func NewSubscriber() *Subscriber {
	return &Subscriber{
		Id:     "",
		Closed: false,
		Ch:     make(chan EventNotification, 100),
		mu:     &sync.RWMutex{},
	}
}

// TODO finish this
func (s *Subscriber) SendToChan(e EventNotification) error {
	if s.Closed {
		return nil
	}
	return nil
}

func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.Closed {
		close(s.Ch)
		s.Closed = true
	}
}

// deiced to return error since it might be usfull if i want to check wheater the topic was created,
// or it already exitsted, might be usless in most scenarios but this adds more control to the caller
func (mb *MessageBroker) AddTopic(topic Topic) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if _, exists := mb.Topics[topic]; exists {
		return NewMessageBrokerErr("topic already exits", "adding topic", "warrning", topic)
	}

	mb.Topics[topic] = make(Que, 10)
	mb.Subscribers[topic] = make(Subscribers, 0)
	return nil
}

func (mb *MessageBroker) DeleteTopic(topic Topic) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if _, ok := mb.Topics[topic]; !ok {
		return &MessageBrokerErr{
			Msg:      "topic was not found",
			Op:       "deleting topic",
			Severity: "critical",
			Context:  topic,
		}
	}

	delete(mb.Topics, topic)
	for _, sub := range mb.Subscribers[topic] {
		sub.Close()
	}
	delete(mb.Subscribers, topic)
	return nil
}

// sending event to the every subsciuber in specified topic, safe to use
func (mb *MessageBroker) Propagate(e EventNotification, topic Topic) error {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	subs, ok := mb.Subscribers[topic]
	if !ok {
		return NewMessageBrokerErr("topic does not exist", "sending event", "critical", topic)
	}

	for _, sub := range subs {
		closed := sub.Closed
		if closed {
			log.Printf("skipping closed subscriber for topic: %s", topic)
			continue
		}

		go func(s *Subscriber) {
			s.Ch <- e
		}(sub)
	}
	return nil
}

// adds event to the queue for the specified topic.
// if topics doest exits it errors out, made this way for more less unpredicted behavoir
func (mb *MessageBroker) Publish(topic Topic, payload Data) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	_, exists := mb.Topics[topic]
	if !exists {
		return NewMessageBrokerErr("Topic does not exits", "pushing event", "critical", topic)
	}

	mb.Topics[topic] = append(mb.Topics[topic], payload)
	subs, exists := mb.Subscribers[topic]
	if !exists {
		return NewMessageBrokerErr("channel for Topic does not exits", "pushing event", "critical", topic)
	}

	// making this non blocking in case chan buff is full
	for _, sub := range subs {
		go func(sub *Subscriber) {
			if sub.Closed {
				log.Println("channel is closed")
				return
			}
			message := payload
			sub.Ch <- message
		}(sub)
	}
	return nil
}

// TODO check if the current solution does not increase memopryu footprint
// and decide whatehr coping it is worth it since it will be O(n) operaton
func (mb *MessageBroker) PopMessage(topic Topic) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	events, exists := mb.Topics[topic]
	if !exists {
		return NewMessageBrokerErr("Topic does not exits", "popping event", "critical", topic)
	}
	if len(events) == 0 {
		return NewMessageBrokerErr("event que is empty", "popping event", "critical", topic)
	}
	subs, exists := mb.Subscribers[topic]
	if !exists {
		return NewMessageBrokerErr("Subscibers for Topic do not exits", "poping event", "critical", topic)
	}

	// non blocking will make it better in futre
	for _, sub := range subs {
		go func(sub *Subscriber) {
			if sub.Closed {
				log.Println("channel is closed")
				return
			}
			message := events[0]
			sub.Ch <- message
		}(sub)
	}

	mb.Topics[topic] = slices.Clone(events[:1])
	return nil
}

// TODO ADD AN ERROR IF NOT FOUND
// pops specific events from the queue for the specified topic, using comparison so event must == e from inside of events.
func (mb *MessageBroker) DeleteEvents(topic Topic, data Data) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	deleteFn := func(e Data) bool {
		return bytes.Equal(e, data)
	}
	mb.Topics[topic] = slices.DeleteFunc(mb.Topics[topic], deleteFn)
}

// returns all events from que
func (mb *MessageBroker) GetEvents(topic Topic) (Que, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	que, ok := mb.Topics[topic]
	if !ok {
		return nil, NewMessageBrokerErr("topic does not exits", "getting events", "warrning", "")
	}
	return que, nil
}

// gets first event found in the que mathcing the event passed in
func (mb *MessageBroker) GetEvent(topic Topic, data Data) (Data, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	for _, val := range mb.Topics[topic] {
		if slices.Equal(data, val) {
			return val, nil
		}
	}
	return nil, NewMessageBrokerErr("no event was found in que", "get event", "warning", topic)
}

// creating new subscriber everytime this fn is executed that allows for lisinging to events coming in
func (mb *MessageBroker) SubscribeTopic(topic Topic) (*Subscriber, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if _, ok := mb.Topics[topic]; !ok {
		return nil, NewMessageBrokerErr("topic was not found", "subscribing to topic", "critical", topic)
	}

	newSub := NewSubscriber()
	if _, ok := mb.Subscribers[topic]; !ok {
		mb.Subscribers[topic] = make(Subscribers, 0)
	}

	mb.Subscribers[topic] = append(mb.Subscribers[topic], newSub)
	return newSub, nil
}

// TODO maybe add error later on
func (mb *MessageBroker) SubscribeGlobal() (*Subscriber, error) {
	sub := NewSubscriber()
	mb.GlobalSubscribers = append(mb.GlobalSubscribers, sub)
	return sub, nil
}