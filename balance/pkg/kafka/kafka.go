package kafka

import (
	"balance/pkg/logger"
	"context"
	"strings"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
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

// MessageHandler é a função que processa cada mensagem com correlationID
type MessageHandler func(key, value []byte, correlationID string) error

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
	log := logger.Instance()
	log.Info().Str("topic", h.topic).Msg("Consumer group session iniciada")
	return nil
}

// Cleanup é executado no final da sessão, depois de todos os ConsumeClaim
func (h ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log := logger.Instance()
	log.Info().Str("topic", h.topic).Msg("Consumer group session finalizada")
	return nil
}

// ConsumeClaim processa mensagens de uma partição específica
func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	log := logger.Instance()

	msgCount := 0

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			msgCount++

			// Extrai ou gera correlationID dos headers
			correlationID := extractOrGenerateCorrelationID(message.Headers)

			// Log das primeiras 10 mensagens e depois a cada 1000
			if msgCount <= 10 || msgCount%1000 == 0 {
				log.Info().
					Str("topic", h.topic).
					Int("message_count", msgCount).
					Int32("partition", message.Partition).
					Int64("offset", message.Offset).
					Str("correlation_id", correlationID).
					Msg("Mensagem recebida")
			}

			// Processa a mensagem com correlationID
			if err := h.handler(message.Key, message.Value, correlationID); err != nil {
				log.Error().
					Str("topic", h.topic).
					Int64("offset", message.Offset).
					Str("correlation_id", correlationID).
					Err(err).
					Msg("Erro ao processar mensagem")
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

	log := logger.Instance()

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

	log.Info().Strs("brokers", c.brokers).Str("topic", c.topic).Str("group_id", c.groupID).Msg("Conectando aos brokers")

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
				log := logger.Instance()
				log.Error().Err(err).Msg("Erro no consumer group")
			}
		}
	}()

	// Loop principal do consumer
	log.Info().Str("topic", c.topic).Msg("Iniciando consumo de mensagens")

	for {
		// Consome do tópico
		topics := []string{c.topic}
		if err := consumerGroup.Consume(ctx, topics, groupHandler); err != nil {
			log := logger.Instance()
			log.Error().Err(err).Msg("Erro ao consumir mensagens")
			return err
		}

		// Se o context foi cancelado, sai do loop
		if ctx.Err() != nil {
			log := logger.Instance()
			log.Info().Str("topic", c.topic).Msg("Consumer cancelado")
			return nil
		}
	}
}

// Close fecha o consumer
func (c *Consumer) Close() error {
	if c.consumerGroup != nil {
		log := logger.Instance()
		log.Info().Str("topic", c.topic).Msg("Encerrando consumer")
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

	log := logger.Instance()
	log.Info().Strs("brokers", brokers).Msg("Produtor criado")

	return &Producer{
		producer: producer,
	}, nil
}

// Publish publica uma mensagem em um tópico específico
func (p *Producer) Publish(topic string, key, value []byte) error {
	return p.PublishWithHeaders(topic, key, value, nil)
}

// PublishWithHeaders publica uma mensagem em um tópico específico com headers
func (p *Producer) PublishWithHeaders(topic string, key, value []byte, headers map[string]string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	// Adiciona headers se fornecidos
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

// Close fecha o produtor
func (p *Producer) Close() error {
	if p.producer != nil {
		log := logger.Instance()
		log.Info().Msg("Fechando produtor")
		return p.producer.Close()
	}
	return nil
}
