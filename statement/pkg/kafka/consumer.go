package kafka

import (
	"context"
	"log"
	"strings"

	"github.com/IBM/sarama"
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

type MessageHandler func(key, value []byte) error

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
	log.Printf("[Kafka] Consumer group session iniciada para tópico: %s", h.topic)
	return nil
}

func (h ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Printf("[Kafka] Consumer group session finalizada para tópico: %s", h.topic)
	return nil
}

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgCount := 0

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			msgCount++

			if msgCount <= 10 || msgCount%1000 == 0 {
				log.Printf("[%s] Mensagem #%d recebida (partition=%d, offset=%d)",
					h.topic, msgCount, message.Partition, message.Offset)
			}

			if err := h.handler(message.Key, message.Value); err != nil {
				log.Printf("[Kafka] Erro ao processar mensagem offset %d: %v", message.Offset, err)
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	config := sarama.NewConfig()

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

	log.Printf("[Kafka] Conectando aos brokers: %v", c.brokers)
	log.Printf("[Kafka] Topic: %s, GroupID: %s", c.topic, c.groupID)

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
				log.Printf("[Kafka] Erro no consumer group: %v", err)
			}
		}
	}()

	log.Printf("[Kafka] Iniciando consumo de mensagens do tópico: %s", c.topic)

	for {
		topics := []string{c.topic}
		if err := consumerGroup.Consume(ctx, topics, groupHandler); err != nil {
			log.Printf("[Kafka] Erro ao consumir: %v", err)
			return err
		}

		if ctx.Err() != nil {
			log.Printf("[Kafka] Consumer cancelado para tópico: %s", c.topic)
			return nil
		}
	}
}

func (c *Consumer) Close() error {
	if c.consumerGroup != nil {
		log.Printf("[Kafka] Fechando consumer para tópico: %s", c.topic)
		return c.consumerGroup.Close()
	}
	return nil
}
