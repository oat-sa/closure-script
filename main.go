package main

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	pubsub "cloud.google.com/go/pubsub"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ClosureMessageBody structure
type ClosureMessageBody struct {
	DeliveryExecutionID string `json:"deliveryExecutionId"`
}

// ClosureMessage structure
type ClosureMessage struct {
	Body       string      `json:"body"`
	Properties interface{} `json:"properties"`
	Headers    interface{} `json:"headers"`
}

var client *redis.Client
var ctx = context.Background()
var psclient *pubsub.Client

var headers = map[string]string{
	"Content-Type": "application/json",
	"type":         `App\Messenger\Message\DeliveryExecutionClosureMessage`,
}

func getDataFromRedis() []string {

	u, err := url.Parse(os.Getenv("REDIS_CLOSURE_DSN"))
	if err != nil {
		log.Panic().Msg("Failed to parse Redis DSN")
	}

	client := redis.NewClient(&redis.Options{
		Addr: u.Host,
	})

	defer client.Close()

	pong, err := client.Ping(ctx).Result()
	log.Debug().Msg(pong)

	if err != nil {
		log.Panic().Msg("Redis is not ready")
	}

	ns := os.Getenv("REDIS_CLOSURE_NAMESPACE")

	keys, err := client.Keys(ctx, ns+":*").Result()
	if err != nil {
		log.Panic().Msg("Failed to fetch data from Redis")
	}

	log.Info().Msgf("Keys found: %d \n", len(keys))

	now := time.Now().UnixNano()

	var expiredKeys []string
	var deliveryExecutionIds []string

	for _, key := range keys {
		parts := strings.Split(key, ":")

		if timestamp, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
			if now >= timestamp {
				expiredKeys = append(expiredKeys, key)
				deliveryExecutionIds = append(deliveryExecutionIds, parts[1])
			}
		}
	}

	client.Del(ctx, expiredKeys...)

	return deliveryExecutionIds
}

func publishClosureMessages(ids []string) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	topicID := os.Getenv("CLOSURE_TOPIC_ID")

	if topicID == "" || projectID == "" {
		log.Panic().Msg("GOOGLE_CLOUD_PROJECT or/and CLOSURE_TOPIC_ID not found")
	}

	psclient, err := pubsub.NewClient(ctx, projectID)

	if err != nil {
		log.Panic().Msg("Failed to connect to PubSub")
		log.Error().Msgf("%s", err)
	}

	defer psclient.Close()

	log.Info().Msg("Pubsub Client Created")

	topic := psclient.Topic(topicID)

	for _, xID := range ids {

		go func(id string) {

			body, err := json.Marshal(ClosureMessageBody{DeliveryExecutionID: id})

			if err != nil {
				log.Error().Msgf("Failed to create a message for %s", id)
				log.Error().Msgf("%s", err)
				return
			}

			data := &ClosureMessage{
				Body:       string(body),
				Properties: []int{},
				Headers:    headers,
			}

			msg, err := json.Marshal(data)

			if err != nil {
				log.Error().Msgf("Failed to create a message for %s", id)
				log.Error().Msgf("%s", err)
				return
			}

			res := topic.Publish(ctx, &pubsub.Message{
				Data: msg,
			})

			msgID, err := res.Get(ctx)

			if err != nil {
				log.Error().Msgf("Failed to send a message for %s", id)
				log.Error().Msgf("%s", err)
			}

			log.Info().Msgf("Message sent for %s", id)
			log.Info().Msgf("Message ID is %s", msgID)
		}(xID)

	}
}

func main() {
	zerolog.LevelFieldName = "severity"

	log.Info().Msg("Closure script starts")

	ids := getDataFromRedis()

	log.Info().Msgf("Expired delivery executions found: %d \n", len(ids))

	if len(ids) > 0 {
		publishClosureMessages(ids)
	}

	log.Info().Msg("Closure script finishes")

}
