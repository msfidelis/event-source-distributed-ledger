package mongodb

import (
	"context"
	"log"
	"statement/pkg/config"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Client struct {
	client   *mongo.Client
	database *mongo.Database
	config   *config.Config
}

func NewClient(cfg *config.Config) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.MongoDB.ConnectTimeout)
	defer cancel()

	uri := cfg.GetMongoURI()
	clientOptions := options.Client().ApplyURI(uri)

	// Set pool size options
	clientOptions.SetMaxPoolSize(cfg.MongoDB.MaxPoolSize)
	clientOptions.SetMinPoolSize(cfg.MongoDB.MinPoolSize)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	log.Printf("[MongoDB] Conexão estabelecida com sucesso: %s", cfg.MongoDB.Database)

	db := client.Database(cfg.MongoDB.Database)

	return &Client{
		client:   client,
		database: db,
		config:   cfg,
	}, nil
}

func (c *Client) InitIndexes() error {
	collection := c.database.Collection("transactions")

	// Índice composto para queries por conta_id ordenadas por data
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "conta_id", Value: 1},
			{Key: "ocorrido_em", Value: -1},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.config.MongoDB.QueryTimeout)
	defer cancel()

	name, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		log.Printf("[MongoDB] Erro ao criar índice: %v", err)
		return err
	}

	log.Printf("[MongoDB] Índice criado: %s", name)

	return nil
}

func (c *Client) InsertTransaction(transaction interface{}) error {
	collection := c.database.Collection("transactions")

	ctx, cancel := context.WithTimeout(context.Background(), c.config.MongoDB.QueryTimeout)
	defer cancel()

	_, err := collection.InsertOne(ctx, transaction)
	if err != nil {
		// Se for erro de duplicata (c\u00f3digo 11000), ignora silenciosamente
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return err
	}

	return nil
}

func (c *Client) Close() error {
	if c.client != nil {
		log.Printf("[MongoDB] Fechando conex\u00e3o")
		ctx, cancel := context.WithTimeout(context.Background(), c.config.MongoDB.QueryTimeout)
		defer cancel()
		return c.client.Disconnect(ctx)
	}
	return nil
}
