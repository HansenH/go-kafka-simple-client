package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	color "github.com/TwiN/go-color"
)

const (
	READING_RETRY_INTERVAL = 10 // seconds
)

var quitSignal = make(chan os.Signal, 1)

func readFlags(c, p *bool) {
	flag.BoolVar(c, "c", false, "as a consumer")
	flag.BoolVar(c, "consumer", false, "as a consumer")
	flag.BoolVar(p, "p", false, "as a producer")
	flag.BoolVar(p, "producer", false, "as a producer")
	flag.Parse()
}

func main() {
	var c, p bool
	readFlags(&c, &p)
	fmt.Println(color.Ize(color.Blue, "Go-kafka-simple-client by HansenH"))
	go guide(c, p)
	signal.Notify(quitSignal, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGQUIT)
	<-quitSignal
	fmt.Println(color.Ize(color.Red, "\nProgram exited."))
}

func guide(c, p bool) {
	defer close(quitSignal)
	if c == p {
		fmt.Println(color.Ize(color.Red,
			"Please specify the flag: \n  either -c (consumer) or -p (producer)"))
		for {
			fmt.Println(color.Ize(color.Cyan,
				"You can also select the mode by entering the number here:"))
			fmt.Println(color.Ize(color.Cyan, "  [1] Consumer\t[2] Producer"))
			var mode string
			fmt.Scanln(&mode)
			switch mode {
			case "1":
				guide(true, false)
				return
			case "2":
				guide(false, true)
				return
			default:
				fmt.Println(color.Ize(color.Red, "Invalid input."))
			}
		}
	}
	if c {
		fmt.Println(color.Ize(color.Yellow, "Launching kafka consumer..."))
		fmt.Println(color.Ize(color.Cyan,
			"=================================================="))
		fmt.Println(color.Ize(color.Cyan,
			"(Press RETURN if there is no more to add)"))
		var brokers []string
		var topic, groupID string
		for i := 1; ; i++ {
			fmt.Print(color.Ize(color.Cyan, fmt.Sprintf("Broker [%d]: ", i)))
			var input string
			fmt.Scanln(&input)
			if input == "" {
				break
			}
			brokers = append(brokers, input)
		}
		fmt.Println(color.Ize(color.Cyan,
			"--------------------------------------------------"))
		fmt.Print(color.Ize(color.Cyan, "Topic: "))
		fmt.Scanln(&topic)
		fmt.Println(color.Ize(color.Cyan,
			"--------------------------------------------------"))
		fmt.Print(color.Ize(color.Cyan, "Group ID: "))
		fmt.Scanln(&groupID)
		fmt.Println(color.Ize(color.Cyan,
			"=================================================="))
		runConsumer(brokers, topic, groupID)
		return
	}
	if p {
		fmt.Println(color.Ize(color.Yellow, "Launching kafka producer..."))
		fmt.Println(color.Ize(color.Cyan,
			"=================================================="))
		fmt.Println(color.Ize(color.Cyan,
			"(Press RETURN if there is no more to add)"))
		var brokers []string
		var topic string
		for i := 1; ; i++ {
			fmt.Print(color.Ize(color.Cyan, fmt.Sprintf("Broker [%d]: ", i)))
			var input string
			fmt.Scanln(&input)
			if input == "" {
				break
			}
			brokers = append(brokers, input)
		}
		fmt.Println(color.Ize(color.Cyan,
			"--------------------------------------------------"))
		fmt.Print(color.Ize(color.Cyan, "Topic: "))
		fmt.Scanln(&topic)
		fmt.Println(color.Ize(color.Cyan,
			"=================================================="))
		runProcuder(brokers, topic)
		return
	}
}

func runConsumer(brokers []string, topic string, groupID string) {
	c := NewConsumer(brokers, topic, groupID)
	if c == nil {
		fmt.Println(color.Ize(color.Red, "Failed to create consumer."))
		return
	}
	defer c.Close()
	fmt.Println(color.Ize(color.Yellow, "Kafka consumer is running..."))
	for {
		fmt.Println(color.Ize(color.Blue,
			"--------------------------------------------------"))
		_, v, _, err := c.Pull()
		if err != nil {
			log.Println(color.Ize(color.Red, fmt.Sprintf(
				"Reading failed! Retry in %ds (Error: %s)",
				READING_RETRY_INTERVAL, err)))
			time.Sleep(READING_RETRY_INTERVAL * time.Second)
			continue
		}
		fmt.Println(string(v))
	}
}

func runProcuder(brokers []string, topic string) {
	p := NewProducer(brokers, topic)
	if p == nil {
		fmt.Println(color.Ize(color.Red, "Failed to create producer."))
		return
	}
	defer p.Close()
	fmt.Println(color.Ize(color.Yellow, "Kafka producer is running... Input your messages."))
	fmt.Println(color.Ize(color.Blue,
		"--------------------------------------------------"))
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		msg := scanner.Text()
		err := p.Push(nil, []byte(msg), 0)
		if err != nil {
			log.Println(color.Ize(color.Red, fmt.Sprintf(
				"Writing failed! (Error: %s) (Msg: %s)", err, msg)))
			continue
		}
		if ASYNC {
			fmt.Println(color.Ize(color.Blue,
				"--------------------------------------------------"))
		} else {
			fmt.Println(color.Ize(color.Blue,
				"-----------------------Sent-----------------------"))
		}
	}
}
