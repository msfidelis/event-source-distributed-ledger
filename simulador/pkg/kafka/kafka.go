package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"simulador/pkg/config"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer encapsula a lógica de publicação no Kafka
type Producer struct {
	writer *kafkago.Writer
	topic  string
	config *config.Config
}

// NewProducer cria um novo producer para um tópico específico
func NewProducer(cfg *config.Config, topic string) *Producer {
	// Mapeia compression string para kafka-go compression
	var compression kafkago.Compression
	switch cfg.Kafka.Compression {
	case "gzip":
		compression = kafkago.Gzip
	case "snappy":
		compression = kafkago.Snappy
	case "lz4":
		compression = kafkago.Lz4
	case "zstd":
		compression = kafkago.Zstd
	default:
		compression = kafkago.Snappy
	}

	// Mapeia RequiredAcks
	var requiredAcks kafkago.RequiredAcks
	switch cfg.Kafka.RequiredAcks {
	case -1:
		requiredAcks = kafkago.RequireAll
	case 0:
		requiredAcks = kafkago.RequireNone
	case 1:
		requiredAcks = kafkago.RequireOne
	default:
		requiredAcks = kafkago.RequireOne
	}

	return &Producer{
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(cfg.Kafka.Brokers...),
			Topic:                  topic,
			Balancer:               &kafkago.LeastBytes{},
			AllowAutoTopicCreation: true,
			RequiredAcks:           requiredAcks,
			Async:                  cfg.Kafka.Async,
			BatchSize:              cfg.Kafka.BatchSize,
			BatchTimeout:           cfg.Kafka.BatchTimeout,
			Compression:            compression,
		},
		topic:  topic,
		config: cfg,
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
