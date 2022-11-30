package main

import (
	"context"
	"fmt"
	"time"

	color "github.com/TwiN/go-color"
	kafka "github.com/segmentio/kafka-go"
)

const (
	CONSUMER_MIN_BYTES      = 1
	CONSUMER_MAX_BYTES      = 1 << 21 // 10M
	WATCH_PARTITION_CHANGES = true
	REBALANCE_TIMEOUT       = 30 // seconds
)

type Consumer struct {
	client *kafka.Reader
	ctx    context.Context
}

func NewConsumer(brokers []string, topic, groupId string) *Consumer {
	defer func() {
		if p := recover(); p != nil {
			fmt.Println(color.Ize(color.Red, fmt.Sprintf("Panic: %s", p)))
		}
	}()
	return &Consumer{
		client: kafka.NewReader(kafka.ReaderConfig{
			Brokers:               brokers,
			Topic:                 topic,
			GroupID:               groupId,
			MinBytes:              CONSUMER_MIN_BYTES,
			MaxBytes:              CONSUMER_MAX_BYTES,
			WatchPartitionChanges: WATCH_PARTITION_CHANGES,
			RebalanceTimeout:      REBALANCE_TIMEOUT * time.Second,
		}),
		ctx: context.Background(),
	}
}

func (c *Consumer) Pull() (key, value []byte, time time.Time, err error) {
	var msg kafka.Message
	if msg, err = c.client.ReadMessage(c.ctx); err != nil {
		return
	}
	key = msg.Key
	value = msg.Value
	time = msg.Time
	return
}

func (c *Consumer) Close() error {
	return c.client.Close()
}
