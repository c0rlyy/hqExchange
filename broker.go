package main

import (
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
}

type TopicContext struct {
	mu          *sync.RWMutex
	Subscribers []*Subscriber
	Writer      *TopicWriter
	Queue       Que
	EventStream chan EventNotification
}

type TopicWriter struct {
	IsClosed bool
}

// subscriber for topics
type Subscriber struct {
	Id     string                 // maybe will use this sometime
	Closed bool                   // need this for reall importnat reason im a moron
	Chan   chan EventNotification // channel to send to notification
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
		Topics:            make(map[Topic]*TopicContext),
		GlobalSubscribers: make([]*Subscriber, 0),
		mu:                sync.RWMutex{},
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

func NewTopicContext() *TopicContext {
	return &TopicContext{
		Subscribers: make([]*Subscriber, 10),
		Writer:      NewTopicWriter(),
		EventStream: make(chan EventNotification, 100),
		Queue:       make(Que, 10),
	}
}

func NewTopicWriter() *TopicWriter {
	return &TopicWriter{
		// EventStream: make(chan []byte, 100),
		IsClosed: false,
	}
}

func (tc *TopicContext) writeEvent(event EventNotification) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.Queue = append(tc.Queue, event)
}

func (tc *TopicContext) NotifyAllSubs(event EventNotification) error {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	for _, sub := range tc.Subscribers {
		if err := sub.SendToChan(event); err != nil {
			return err
		}
	}
	return nil
}

// starts go rutine that notifies all subscibers and writes events to Que
func (tc *TopicContext) ProccessEvents() {
	go func() {
		event := <-tc.EventStream
		tc.writeEvent(event)
		tc.NotifyAllSubs(event)
	}()
}

func (tc *TopicContext) SendEvent(event EventNotification) error {
	select {
	case tc.EventStream <- event:
		return nil
	default:
		return NewMessageBrokerErr("closed", "", "", "")
	}
}

// TODO finish this
func (tc *TopicContext) CloseTopic() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, sub := range tc.Subscribers {
		sub.Close()
	}
	tc.Queue = nil
}

func (tc *TopicContext) subscribe() *Subscriber {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	newSub := NewSubscriber()
	tc.Subscribers = append(tc.Subscribers, newSub)
	return newSub
}

func (e *MessageBrokerErr) Error() string {
	return fmt.Sprintf("[%s] %s: %s, Context: %s", e.Severity, e.Op, e.Msg, e.Context)
}

// channel is bufferd, default size is 100
func NewSubscriber() *Subscriber {
	return &Subscriber{
		Id:     "",
		Closed: false,
		Chan:   make(chan EventNotification, 100),
		mu:     &sync.RWMutex{},
	}
}

// TODO finish this
func (s *Subscriber) SendToChan(e EventNotification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Closed {
		return NewMessageBrokerErr("sending on closed channel", "", "critical", s.Id)
	}

	select {
	case s.Chan <- e:
		return nil
	default:
		return NewMessageBrokerErr("buffer full, message dropped", "", "warning", s.Id)
	}
}

func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.Closed {
		close(s.Chan)
		s.Closed = true
	}
}

func (mb *MessageBroker) ProccessGlobalEvents() error {
	return nil
}

// deiced to return error since it might be usfull if i want to check wheater the topic was created,
// or it already exitsted, might be usless in most scenarios but this adds more control to the caller
func (mb *MessageBroker) AddTopic(topic Topic) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if _, exists := mb.Topics[topic]; exists {
		return NewMessageBrokerErr("topic already exits", "adding topic", "warrning", topic)
	}
	tContext := NewTopicContext()
	//go rutine start
	tContext.ProccessEvents()

	mb.Topics[topic] = tContext
	return nil
}

func (mb *MessageBroker) DeleteTopic(topic Topic) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	tContext, ok := mb.Topics[topic]
	if !ok {
		return &MessageBrokerErr{
			Msg:      "topic was not found",
			Op:       "deleting topic",
			Severity: "critical",
			Context:  topic,
		}
	}

	tContext.CloseTopic()
	delete(mb.Topics, topic)
	return nil
}

// adds event to the queue for the specified topic.
// if topics doest exits it errors out, made this way for more less unpredicted behavoir
func (mb *MessageBroker) Publish(topic Topic, payload Data) error {
	tContext, exists := mb.Topics[topic]
	if !exists {
		return NewMessageBrokerErr("Topic does not exits", "pushing event", "critical", topic)
	}

	if err := tContext.SendEvent(payload); err != nil {
		return err
	}

	// glSub := mb.GlobalSubscribers
	// for _, glSub := range glSub {
	// 	glSub.SendToChan(payload)
	// }
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
// func (mb *MessageBroker) DeleteEvents(topic Topic, data Data) {
// 	mb.mu.Lock()
// 	defer mb.mu.Unlock()
// 	deleteFn := func(e Data) bool {
// 		return bytes.Equal(e, data)
// 	}
// 	mb.Topics[topic] = slices.DeleteFunc(mb.Topics[topic], deleteFn)
// }

// returns all events from que

func (mb *MessageBroker) GetEvents(topic Topic) (Que, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	tContext, ok := mb.Topics[topic]
	if !ok {
		return nil, NewMessageBrokerErr("topic does not exits", "getting events", "warrning", "")
	}
	return tContext.Queue, nil
}

// gets first event found in the que mathcing the event passed in
// func (mb *MessageBroker) GetEvent(topic Topic, data Data) (Data, error) {
// 	mb.mu.RLock()
// 	defer mb.mu.RUnlock()

// 	for _, val := range mb.Topics[topic] {
// 		if slices.Equal(data, val) {
// 			return val, nil
// 		}
// 	}
// 	return nil, NewMessageBrokerErr("no event was found in que", "get event", "warning", topic)
// }

// creating new subscriber everytime this fn is executed that allows for lisinging to events coming in
func (mb *MessageBroker) SubscribeTopic(topic Topic) (*Subscriber, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	tContext, ok := mb.Topics[topic]
	if !ok {
		return nil, NewMessageBrokerErr("topic was not found", "subscribing to topic", "critical", topic)
	}

	newSub := tContext.subscribe()
	return newSub, nil
}

// TODO maybe add error later on
func (mb *MessageBroker) SubscribeGlobal() (*Subscriber, error) {
	sub := NewSubscriber()
	mb.GlobalSubscribers = append(mb.GlobalSubscribers, sub)
	return sub, nil
}
