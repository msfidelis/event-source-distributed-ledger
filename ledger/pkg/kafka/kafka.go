package kafka

import (
	"context"
	"log"
	"strings"

	"github.com/IBM/sarama"
)

// Consumer representa um consumidor Kafka usando Sarama
type Consumer struct {
	brokers       []string
	topic         string
	groupID       string
	consumerGroup sarama.ConsumerGroup
}

// ConsumerGroupHandler implementa sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	handler MessageHandler
	topic   string
}

// MessageHandler é a função que processa cada mensagem
type MessageHandler func(key, value []byte) error

// NewConsumer cria um novo consumer Kafka usando Sarama
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
	}
}

// ParseBrokers converte string de brokers separados por vírgula em slice
func ParseBrokers(brokers string) []string {
	return strings.Split(brokers, ",")
}

// Setup é executado no início de uma nova sessão, antes de ConsumeClaim
func (h ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Printf("[Kafka] Consumer group session iniciada para tópico: %s", h.topic)
	return nil
}

// Cleanup é executado no final da sessão, depois de todos os ConsumeClaim
func (h ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Printf("[Kafka] Consumer group session finalizada para tópico: %s", h.topic)
	return nil
}

// ConsumeClaim processa mensagens de uma partição específica
func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgCount := 0

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			msgCount++

			// Log das primeiras 10 mensagens e depois a cada 1000
			if msgCount <= 10 || msgCount%1000 == 0 {
				log.Printf("[%s] Mensagem #%d recebida (partition=%d, offset=%d)",
					h.topic, msgCount, message.Partition, message.Offset)
			}

			// Processa a mensagem
			if err := h.handler(message.Key, message.Value); err != nil {
				log.Printf("[Kafka] Erro ao processar mensagem offset %d: %v", message.Offset, err)
				// Mesmo com erro, marca a mensagem como processada para não travar o consumer
			}

			// Marca a mensagem como processada (commit)
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// Consume inicia o consumo de mensagens do tópico
func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	config := sarama.NewConfig()

	// Configurações de consumer
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Começa do mais antigo
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

	// Configurações de fetch - otimizadas para baixa latência
	config.Consumer.Fetch.Min = 1               // 1 byte - responde imediatamente
	config.Consumer.Fetch.Default = 1024 * 1024 // 1MB
	config.Consumer.MaxProcessingTime = 1_000   // 1 segundo (em ms)

	// Auto-commit a cada 1 segundo
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1_000 // 1 segundo (em ms)

	// Metadata e versão
	config.Version = sarama.V2_6_0_0
	config.ClientID = c.groupID

	log.Printf("[Kafka] Conectando aos brokers: %v", c.brokers)
	log.Printf("[Kafka] Topic: %s, GroupID: %s", c.topic, c.groupID)

	// Cria o consumer group
	consumerGroup, err := sarama.NewConsumerGroup(c.brokers, c.groupID, config)
	if err != nil {
		return err
	}
	c.consumerGroup = consumerGroup

	// Handler de mensagens
	groupHandler := ConsumerGroupHandler{
		handler: handler,
		topic:   c.topic,
	}

	// Goroutine para logar erros do consumer group
	go func() {
		for err := range consumerGroup.Errors() {
			if err != nil {
				log.Printf("[Kafka] Erro no consumer group: %v", err)
			}
		}
	}()

	// Loop principal do consumer
	log.Printf("[Kafka] Iniciando consumo de mensagens do tópico: %s", c.topic)

	for {
		// Consome do tópico
		topics := []string{c.topic}
		if err := consumerGroup.Consume(ctx, topics, groupHandler); err != nil {
			log.Printf("[Kafka] Erro ao consumir: %v", err)
			return err
		}

		// Se o context foi cancelado, sai do loop
		if ctx.Err() != nil {
			log.Printf("[Kafka] Consumer cancelado para tópico: %s", c.topic)
			return nil
		}
	}
}

// Close fecha o consumer
func (c *Consumer) Close() error {
	if c.consumerGroup != nil {
		log.Printf("[Kafka] Fechando consumer para tópico: %s", c.topic)
		return c.consumerGroup.Close()
	}
	return nil
}

// Producer representa um produtor Kafka usando Sarama
type Producer struct {
	producer sarama.SyncProducer
}

// NewProducer cria um novo produtor Kafka
func NewProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal // Aguarda confirmação do líder
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_6_0_0

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	log.Printf("[Kafka] Produtor criado para brokers: %v", brokers)

	return &Producer{
		producer: producer,
	}, nil
}

// Publish publica uma mensagem em um tópico específico
func (p *Producer) Publish(topic string, key, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	_, _, err := p.producer.SendMessage(msg)
	return err
}

// Close fecha o produtor
func (p *Producer) Close() error {
	if p.producer != nil {
		log.Printf("[Kafka] Fechando produtor")
		return p.producer.Close()
	}
	return nil
}
