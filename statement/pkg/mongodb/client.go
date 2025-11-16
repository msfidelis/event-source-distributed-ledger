package mongodb

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Client struct {
	client   *mongo.Client
	database *mongo.Database
}

func NewClient(uri, database string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	log.Printf("[MongoDB] Conexão estabelecida com sucesso: %s", database)

	db := client.Database(database)

	return &Client{
		client:   client,
		database: db,
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := collection.InsertOne(ctx, transaction)
	if err != nil {
		// Se for erro de duplicata (código 11000), ignora silenciosamente
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return err
	}

	return nil
}

func (c *Client) Close() error {
	if c.client != nil {
		log.Printf("[MongoDB] Fechando conexão")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return c.client.Disconnect(ctx)
	}
	return nil
}
