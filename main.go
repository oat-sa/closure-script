package main

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"sync"
	"time"

	pubsub "cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ClosureMessageBody structure
type ClosureMessageBody struct {
	DeliveryExecutionID string `json:"deliveryExecutionId"`
	CloseAt             int32  `json:"closeAt"`
}

// ResultsMessageBody structure
type ResultsMessageBody struct {
	ID                  string `json:"id"`
	DeliveryExecutionID string `json:"deliveryExecutionId"`
	ForceClosure        bool   `json:"forceClosure"`
}

// Message structure
type Message struct {
	Body       string      `json:"body"`
	Properties interface{} `json:"properties"`
	Headers    interface{} `json:"headers"`
}

var ctx = context.Background()

func sendClosureMessage(client *pubsub.Client, msg []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	topic := client.Topic(os.Getenv("CLOSURE_TOPIC_ID"))

	res := topic.Publish(ctx, &pubsub.Message{
		Data: msg,
	})

	_, err := res.Get(ctx)

	if err != nil {
		log.Error().Msgf("%s", err)
		log.Error().Msgf("Failed to publish a message for Closure topic")
		return
	}
}

func sendResultsMessage(client *pubsub.Client, deliveryExecutionID string, wg *sync.WaitGroup) {
	defer wg.Done()

	topic := client.Topic(os.Getenv("RESULTS_TOPIC_ID"))

	body, err := json.Marshal(ResultsMessageBody{
		ID:                  deliveryExecutionID, // ?
		DeliveryExecutionID: deliveryExecutionID,
		ForceClosure:        true,
	})

	if err != nil {
		log.Error().Msgf("%s", err)
		log.Error().Msgf("Failed to create a message body for Results topic")
		return
	}

	msg, err := json.Marshal(&Message{
		Body:       string(body),
		Properties: []int{},
		Headers: map[string]string{
			"Content-Type": "application/json",
			"type":         `App\Messenger\Message\ResultExtractionMessage`,
		},
	})

	if err != nil {
		log.Error().Msgf("%s", err)
		log.Error().Msgf("Failed to create a message for Results topic")
		return
	}

	res := topic.Publish(ctx, &pubsub.Message{
		Data: msg,
	})

	_, err = res.Get(ctx)

	if err != nil {
		log.Error().Msgf("%s", err)
		log.Error().Msgf("Failed to publish a message for Results topic")
		return
	}
}

func main() {
	zerolog.LevelFieldName = "severity"

	log.Info().Msg("Closure script starts")

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	subscriptionID := os.Getenv("CLOSURE_SUB_ID")

	timeout := time.Second * 10
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	var cancel context.CancelFunc
	var wg sync.WaitGroup
	var count int
	var resendClosure [][]byte
	var countToClose int

	if subscriptionID == "" || projectID == "" {
		log.Panic().Msg("GOOGLE_CLOUD_PROJECT or/and CLOSURE_SUB_ID not found")
	}

	client, err := pubsub.NewClient(ctx, projectID)

	if err != nil {
		log.Error().Msgf("%s", err)
		log.Panic().Msg("Failed to connect to PubSub")
	}

	defer client.Close()

	log.Info().Msg("Pubsub Client Created")

	log.Info().Msg("Subscribing to " + subscriptionID)
	sub := client.Subscription(subscriptionID)

	sub.ReceiveSettings.NumGoroutines = 10 * runtime.NumCPU()
	sub.ReceiveSettings.MaxOutstandingMessages = -1
	sub.ReceiveSettings.MaxOutstandingBytes = -1
	sub.ReceiveSettings.Synchronous = false

	wg.Add(1)

	go func() {
		var cctx context.Context
		cctx, cancel = context.WithCancel(ctx)

		now := time.Now().Unix()

		err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			log.Debug().Msgf("Received: %s", m.ID)
			count++
			ticker.Reset(timeout)

			m.Ack()
			log.Debug().Msgf("Acknowledgement : %s", m.ID)

			var msg Message

			err := json.Unmarshal(m.Data, &msg)

			if err != nil {
				log.Error().Msgf("%s", err)
				log.Error().Msg("Failed to unmarshal a message")
			}

			var msgBody ClosureMessageBody

			err = json.Unmarshal([]byte(msg.Body), &msgBody)

			if err != nil {
				log.Error().Msgf("%s", err)
				log.Error().Msg("Failed to unmarshal a message body")
			}

			if now >= int64(msgBody.CloseAt) {
				wg.Add(1)
				countToClose++
				go sendResultsMessage(client, msgBody.DeliveryExecutionID, &wg)
			} else {
				// send to topic ONLY after receiving stops
				resendClosure = append(resendClosure, m.Data)
			}
		})

		if err != nil {
			log.Error().Msgf("%s", err)
			log.Panic().Msg("Failed to receive a message")
		}
	}()

	for {
		select {
		case <-ticker.C:
			log.Info().Msgf("Idle timeout: %s", timeout)
			ticker.Stop()
			cancel()

			log.Info().Msgf("Received: %d", count)
			log.Info().Msgf("Need to close: %d", countToClose)

			log.Info().Msgf("Need to resend: %d", len(resendClosure))
			for _, msg := range resendClosure {
				wg.Add(1)
				go sendClosureMessage(client, msg, &wg)
			}

			wg.Done()
			break
		}
		break
	}

	wg.Wait()

	log.Info().Msg("Closure script finishes")
}
