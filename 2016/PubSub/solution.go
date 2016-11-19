package main

import "fmt"

type PubSub struct {
	publisher   chan string
	subscribers []chan string
}

func NewPubSub() *PubSub {
	topic := &PubSub{publisher: make(chan string), subscribers: make([]chan string, 0)}

	go func() {
		for {
			msg := <-topic.publisher
			for _, subCh := range topic.subscribers {
				subCh <- msg
			}
		}
	}()

	return topic
}

func (ps *PubSub) Subscribe() <-chan string {
	subCh := make(chan string)
	ps.subscribers = append(ps.subscribers, subCh)
	return subCh
}

func (ps *PubSub) Publish() chan<- string {
	return ps.publisher
}

func main() {
	ps := NewPubSub()
	a := ps.Subscribe()
	b := ps.Subscribe()
	c := ps.Subscribe()
	go func() {
		ps.Publish() <- "wat"
		ps.Publish() <- ("wat" + <-c)
	}()
	fmt.Printf("A recieved %s, B recieved %s and we ignore C!\n", <-a, <-b)
	fmt.Printf("A recieved %s, B recieved %s and C received %s\n", <-a, <-b, <-c)
}
