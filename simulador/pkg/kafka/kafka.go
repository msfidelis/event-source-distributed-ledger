package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer encapsula a lógica de publicação no Kafka
type Producer struct {
	writer *kafkago.Writer
	topic  string
}

// NewProducer cria um novo producer para um tópico específico
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafkago.LeastBytes{},
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafkago.RequireOne, // Mudado de RequireAll para RequireOne
			Async:                  true,               // Habilita modo assíncrono
			BatchSize:              100,                // Batch de 100 mensagens
			BatchTimeout:           10 * time.Millisecond,
			Compression:            kafkago.Snappy, // Compressão para reduzir network I/O
		},
		topic: topic,
	}
}

// Publish publica uma mensagem no Kafka
func (p *Producer) Publish(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("erro ao serializar mensagem: %w", err)
	}

	msg := kafkago.Message{
		Key:   []byte(key),
		Value: data,
	}

	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("erro ao publicar mensagem: %w", err)
	}

	// Log reduzido - apenas a cada 1000 mensagens para não sobrecarregar
	return nil
}

// PublishBatch publica múltiplas mensagens em batch
func (p *Producer) PublishBatch(ctx context.Context, messages []kafkago.Message) error {
	err := p.writer.WriteMessages(ctx, messages...)
	if err != nil {
		return fmt.Errorf("erro ao publicar batch: %w", err)
	}
	return nil
}

// Close fecha o writer
func (p *Producer) Close() error {
	return p.writer.Close()
}

// ParseBrokers converte string de brokers separada por vírgula em slice
func ParseBrokers(brokersStr string) []string {
	return strings.Split(brokersStr, ",")
}
