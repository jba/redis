package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/gomodule/redigo/redis"
)

var addr = flag.String("addr", "", "host:port of redis instance")

func main() {
	fmt.Println("starting")
	ctx := context.Background()
	flag.Parse()
	if *addr == "" {
		log.Fatal("need -addr")
	}
	pub, err := NewPublisher(ctx, *addr, "topic1")
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	sub, err := NewSubscriber(ctx, *addr, "topic1")
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	if err := pub.SendBatch(ctx, []string{"now", "is", "the", "time"}); err != nil {
		log.Fatalf("send: %v", err)
	}

	for i := 0; i < 4; i++ {
		msgs, err := sub.ReceiveBatch(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(msgs)
	}
	fmt.Println("done")
}

type publisher struct {
	conn  redis.Conn
	topic string
}

func NewPublisher(ctx context.Context, redisAddr, topic string) (*publisher, error) {
	// TODO: use ctx
	conn, err := redis.Dial("tcp", redisAddr)
	if err != nil {
		return nil, err
	}
	return &publisher{conn, topic}, nil
}

func (p *publisher) SendBatch(ctx context.Context, msgs []string) error {
	// TODO: use ctx
	for _, m := range msgs {
		if err := p.conn.Send("publish", p.topic, m); err != nil {
			return err
		}
	}
	if err := p.conn.Flush(); err != nil {
		return err
	}
	for range msgs {
		if _, err := p.conn.Receive(); err != nil {
			return err
		}
	}
	return nil
}

func (p *publisher) Close() error {
	return p.conn.Close()
}

type subscriber struct {
	psconn redis.PubSubConn
	topic  string
}

func NewSubscriber(ctx context.Context, addr, topic string) (_ *subscriber, err error) {
	// TODO: use ctx
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	psc := redis.PubSubConn{Conn: conn}
	defer func() {
		if err != nil {
			psc.Close()
		}
	}()
	if err := psc.Subscribe(topic); err != nil {
		return nil, err
	}
	switch res := psc.Receive().(type) {
	case redis.Subscription:
		return &subscriber{psc, topic}, nil
	case error:
		return nil, res
	default:
		return nil, fmt.Errorf("XXX: got %v, expected Subscription", res)
	}
}

// TODO(jba): do we need to unsubscribe?

func (s *subscriber) ReceiveBatch(ctx context.Context) ([]string, error) {
	switch res := s.psconn.Receive().(type) {
	case redis.Message:
		return []string{string(res.Data)}, nil
	case error:
		return nil, res
	default:
		return nil, fmt.Errorf("XXX: got %v, expected Message", res)
	}
}

func (s *subscriber) Close() error {
	return s.psconn.Close()
}
