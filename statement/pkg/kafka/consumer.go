package kafka

import (
	"context"
	"statement/pkg/logger"
	"strings"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type Consumer struct {
	brokers       []string
	topic         string
	groupID       string
	consumerGroup sarama.ConsumerGroup
}

type ConsumerGroupHandler struct {
	handler MessageHandler
	topic   string
}

type MessageHandler func(key, value []byte, correlationID string) error

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
	}
}

func ParseBrokers(brokers string) []string {
	return strings.Split(brokers, ",")
}

func (h ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log := logger.Instance()
	log.Info().Str("topic", h.topic).Msg("Consumer group session iniciada")
	return nil
}

func (h ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log := logger.Instance()
	log.Info().Str("topic", h.topic).Msg("Consumer group session finalizada")
	return nil
}

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgCount := 0
	log := logger.Instance()
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			msgCount++

			// Extrai ou gera correlationID dos headers
			correlationID := extractOrGenerateCorrelationID(message.Headers)

			if msgCount <= 10 || msgCount%1000 == 0 {
				log.Info().
					Str("topic", h.topic).
					Int("msgCount", msgCount).
					Int32("partition", message.Partition).
					Int64("offset", message.Offset).
					Str("correlationID", correlationID).
					Msg("Mensagem recebida")
			}

			if err := h.handler(message.Key, message.Value, correlationID); err != nil {
				log.Error().
					Int64("offset", message.Offset).
					Str("correlationID", correlationID).
					Err(err).
					Msg("Erro ao processar mensagem")
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	config := sarama.NewConfig()

	log := logger.Instance()

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Fetch.Min = 1
	config.Consumer.Fetch.Default = 1024 * 1024
	config.Consumer.MaxProcessingTime = 1_000
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1_000
	config.Version = sarama.V2_6_0_0
	config.ClientID = c.groupID

	log.Info().Strs("brokers", c.brokers).Msg("[Kafka] Conectando aos brokers")
	log.Info().Str("topic", c.topic).Str("groupID", c.groupID).Msg("Configuração do consumidor")

	consumerGroup, err := sarama.NewConsumerGroup(c.brokers, c.groupID, config)
	if err != nil {
		return err
	}
	c.consumerGroup = consumerGroup

	groupHandler := ConsumerGroupHandler{
		handler: handler,
		topic:   c.topic,
	}

	go func() {
		for err := range consumerGroup.Errors() {
			if err != nil {
				log.Error().Err(err).Msg("Erro no consumer group")
			}
		}
	}()

	log.Info().Str("topic", c.topic).Msg("Iniciando consumo de mensagens")

	for {
		topics := []string{c.topic}
		if err := consumerGroup.Consume(ctx, topics, groupHandler); err != nil {
			log.Error().Err(err).Msg("Erro ao consumir")
			return err
		}

		if ctx.Err() != nil {
			log.Info().Str("topic", c.topic).Msg("Consumer cancelado")
			return nil
		}
	}
}

func (c *Consumer) Close() error {
	if c.consumerGroup != nil {
		log := logger.Instance()
		log.Info().Str("topic", c.topic).Msg("Fechando consumer")
		return c.consumerGroup.Close()
	}
	return nil
}

// extractOrGenerateCorrelationID extrai o correlationID dos headers ou gera um novo UUID
func extractOrGenerateCorrelationID(headers []*sarama.RecordHeader) string {
	for _, header := range headers {
		if string(header.Key) == "correlationID" || string(header.Key) == "correlation_id" {
			return string(header.Value)
		}
	}
	// Se não encontrou, gera um novo UUID
	return uuid.New().String()
}
