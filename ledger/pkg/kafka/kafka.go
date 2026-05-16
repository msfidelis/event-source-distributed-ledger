package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"ledger/pkg/logger"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

// errorClass categorizes handler errors to decide retry vs DLQ routing.
type errorClass int

const (
	errClassNil       errorClass = iota
	errClassRetryable            // transient: network, DB, timeout — will retry up to maxRetries
	errClassPermanent            // deterministic: parse, validation — sent to DLQ immediately
)

// classifyHandlerError determines whether a handler error should be retried.
// Unknown errors default to retryable: they will exhaust maxRetries then go to DLQ.
func classifyHandlerError(err error) errorClass {
	if err == nil {
		return errClassNil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return errClassRetryable
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return errClassRetryable
	}
	var jsonType *json.UnmarshalTypeError
	if errors.As(err, &jsonType) {
		return errClassPermanent
	}
	var jsonSyntax *json.SyntaxError
	if errors.As(err, &jsonSyntax) {
		return errClassPermanent
	}
	return errClassRetryable
}

// Consumer represents a Kafka consumer using Sarama.
type Consumer struct {
	brokers       []string
	topic         string
	groupID       string
	consumerGroup sarama.ConsumerGroup
	dlqProducer   *Producer
	dlqTopic      string
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler.
type ConsumerGroupHandler struct {
	handler      MessageHandler
	topic        string
	dlqProducer  *Producer
	dlqTopic     string
	retryBackoff func(attempt int) time.Duration // nil = exponential default
}

// MessageHandler is the function that processes each Kafka message.
type MessageHandler func(key, value []byte, correlationID string) error

// NewConsumer creates a new Kafka consumer.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
	}
}

// WithDLQ configures a Dead Letter Queue producer and topic for the consumer.
// Messages that exhaust all retries are published to dlqTopic before being committed.
func (c *Consumer) WithDLQ(producer *Producer, dlqTopic string) *Consumer {
	c.dlqProducer = producer
	c.dlqTopic = dlqTopic
	return c
}

// ParseBrokers converts a comma-separated broker string to a slice.
func ParseBrokers(brokers string) []string {
	return strings.Split(brokers, ",")
}

// Setup is called at the beginning of a new consumer group session.
func (h ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log := logger.Instance()
	log.Info().Str("topic", h.topic).Msg("Consumer group session iniciada")
	return nil
}

// Cleanup is called at the end of a consumer group session.
func (h ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log := logger.Instance()
	log.Info().Str("topic", h.topic).Msg("Consumer group session finalizada")
	return nil
}

// publishToDLQ publishes a failed message to the DLQ topic with diagnostic headers.
// Returns nil (no-op) if no DLQ producer is configured.
func (h ConsumerGroupHandler) publishToDLQ(
	msg *sarama.ConsumerMessage,
	handlerErr error,
	correlationID string,
	attempts int,
) error {
	if h.dlqProducer == nil {
		return nil
	}
	return h.dlqProducer.PublishWithHeaders(h.dlqTopic, msg.Key, msg.Value, map[string]string{
		"original_topic": h.topic,
		"error_message":  handlerErr.Error(),
		"failed_at":      time.Now().UTC().Format(time.RFC3339),
		"retry_attempts": strconv.Itoa(attempts),
		"correlation_id": correlationID,
	})
}

func defaultRetryBackoff(attempt int) time.Duration {
	return time.Duration(1<<uint(attempt)) * time.Second
}

// ConsumeClaim processes messages from a specific partition.
//
// Retry policy:
//   - retryable errors (network, timeout, DB): up to maxRetries with exponential backoff
//   - permanent errors (parse, validation): sent to DLQ immediately, no retry
//   - if DLQ publish fails: message is NOT marked — forces rebalance for redelivery
func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const maxRetries = 3

	log := logger.Instance()
	msgCount := 0

	backoff := h.retryBackoff
	if backoff == nil {
		backoff = defaultRetryBackoff
	}

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			msgCount++
			correlationID := extractOrGenerateCorrelationID(message.Headers)

			if msgCount <= 10 || msgCount%1000 == 0 {
				log.Info().
					Str("topic", h.topic).
					Int("message_count", msgCount).
					Int32("partition", message.Partition).
					Int64("offset", message.Offset).
					Str("correlation_id", correlationID).
					Msg("Mensagem recebida")
			}

			var lastErr error
			for attempt := 0; attempt <= maxRetries; attempt++ {
				lastErr = h.handler(message.Key, message.Value, correlationID)
				if lastErr == nil {
					break
				}

				log.Warn().
					Err(lastErr).
					Int("attempt", attempt+1).
					Int("max_retries", maxRetries).
					Int64("offset", message.Offset).
					Str("correlation_id", correlationID).
					Msg("Erro ao processar mensagem")

				if classifyHandlerError(lastErr) == errClassPermanent {
					break
				}

				if attempt < maxRetries {
					select {
					case <-time.After(backoff(attempt)):
					case <-session.Context().Done():
						return nil
					}
				}
			}

			if lastErr != nil {
				log.Error().
					Err(lastErr).
					Int64("offset", message.Offset).
					Str("correlation_id", correlationID).
					Msg("Falha definitiva ao processar mensagem — enviando para DLQ")

				if dlqErr := h.publishToDLQ(message, lastErr, correlationID, maxRetries); dlqErr != nil {
					log.Error().
						Err(dlqErr).
						Int64("offset", message.Offset).
						Msg("Falha ao publicar no DLQ — não marcando mensagem para forçar rebalance")
					continue
				}
			}

			// Only reached when handler succeeded or message was safely sent to DLQ.
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// Consume starts consuming messages from the topic.
func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	log := logger.Instance()
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

	config.Consumer.Fetch.Min = 1
	config.Consumer.Fetch.Default = 1024 * 1024

	config.Consumer.MaxProcessingTime = 30 * time.Second
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Consumer.Group.Rebalance.Timeout = 60 * time.Second

	config.Version = sarama.V2_6_0_0
	config.ClientID = c.groupID

	log.Info().
		Strs("brokers", c.brokers).
		Str("topic", c.topic).
		Str("group_id", c.groupID).
		Msg("Conectando aos brokers")

	consumerGroup, err := sarama.NewConsumerGroup(c.brokers, c.groupID, config)
	if err != nil {
		return err
	}
	c.consumerGroup = consumerGroup

	groupHandler := ConsumerGroupHandler{
		handler:     handler,
		topic:       c.topic,
		dlqProducer: c.dlqProducer,
		dlqTopic:    c.dlqTopic,
	}

	go func() {
		for err := range consumerGroup.Errors() {
			if err != nil {
				log.Error().Err(err).Msg("Erro no consumer group")
			}
		}
	}()

	log.Info().Str("topic", c.topic).Msg("Iniciando consumo de mensagens do tópico")

	for {
		topics := []string{c.topic}
		if err := consumerGroup.Consume(ctx, topics, groupHandler); err != nil {
			log.Error().Err(err).Msg("Erro ao consumir mensagens")
			return err
		}
		if ctx.Err() != nil {
			log.Info().Str("topic", c.topic).Msg("Consumer cancelado")
			return nil
		}
	}
}

// Close gracefully shuts down the consumer.
func (c *Consumer) Close() error {
	if c.consumerGroup != nil {
		log := logger.Instance()
		log.Info().Str("topic", c.topic).Msg("Fechando consumer")
		return c.consumerGroup.Close()
	}
	return nil
}

// Producer represents a Kafka producer using Sarama.
type Producer struct {
	producer sarama.SyncProducer
}

// NewProducer creates a new Kafka producer.
func NewProducer(brokers []string) (*Producer, error) {
	log := logger.Instance()

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_6_0_0

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	log.Info().Strs("brokers", brokers).Msg("Produtor criado")

	return &Producer{producer: producer}, nil
}

// Publish publishes a message to a specific topic.
func (p *Producer) Publish(topic string, key, value []byte) error {
	return p.PublishWithHeaders(topic, key, value, nil)
}

// PublishWithHeaders publishes a message with custom headers.
func (p *Producer) PublishWithHeaders(topic string, key, value []byte, headers map[string]string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	if headers != nil {
		for k, v := range headers {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}

	_, _, err := p.producer.SendMessage(msg)
	return err
}

// Close gracefully shuts down the producer.
func (p *Producer) Close() error {
	if p.producer != nil {
		log := logger.Instance()
		log.Info().Msg("Fechando produtor")
		return p.producer.Close()
	}
	return nil
}

// extractOrGenerateCorrelationID extracts the correlationID from headers or generates a new UUID.
func extractOrGenerateCorrelationID(headers []*sarama.RecordHeader) string {
	for _, header := range headers {
		if string(header.Key) == "correlationID" || string(header.Key) == "correlation_id" {
			return string(header.Value)
		}
	}
	return uuid.New().String()
}
