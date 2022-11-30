package main

import (
	"context"
	"fmt"
	"time"

	color "github.com/TwiN/go-color"
	kafka "github.com/segmentio/kafka-go"
)

const (
	ASYNC         = false
	WRITE_TIMEOUT = 10 // seconds
)

type Producer struct {
	client *kafka.Writer
	ctx    context.Context
}

func NewProducer(brokers []string, topic string) *Producer {
	defer func() {
		if p := recover(); p != nil {
			fmt.Println(color.Ize(color.Red, fmt.Sprintf("Panic: %s", p)))
		}
	}()
	return &Producer{
		client: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Topic:        topic,
			Async:        ASYNC,
			WriteTimeout: WRITE_TIMEOUT * time.Second,
		}),
		ctx: context.Background(),
	}
}

func (p *Producer) Push(key, value []byte, ctxTimeout time.Duration) error {
	var ctx context.Context
	var cancel context.CancelFunc
	if ctxTimeout != 0 {
		ctx, cancel = context.WithTimeout(p.ctx, ctxTimeout)
		defer cancel()
	} else {
		ctx = p.ctx
	}
	return p.client.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}

func (p *Producer) Close() error {
	return p.client.Close()
}
