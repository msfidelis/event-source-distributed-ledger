package mongodb

import (
	"context"
	"fmt"
	"time"

	"statement-api/pkg/config"
	"statement-api/pkg/logger"
	"statement-api/pkg/observability"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Client struct {
	client   *mongo.Client
	database *mongo.Database
	config   *config.Config
}

type Transaction struct {
	ID           string    `bson:"_id" json:"id"`
	ContaID      string    `bson:"conta_id" json:"conta_id"`
	EventID      int       `bson:"event_id" json:"event_id"`
	Tipo         string    `bson:"tipo" json:"tipo"`
	Valor        float64   `bson:"valor" json:"valor"`
	Descricao    string    `bson:"descricao" json:"descricao"`
	BalanceAfter float64   `bson:"balance_after" json:"balance_after"`
	OcorridoEm   time.Time `bson:"ocorrido_em" json:"ocorrido_em"`
	ConfirmedAt  time.Time `bson:"confirmed_at" json:"confirmed_at"`
}

type StatementResult struct {
	Transactions []Transaction `json:"transactions"`
	Page         int           `json:"page"`
	ItemsPerPage int           `json:"items_per_page"`
	TotalItems   int64         `json:"total_items"`
	TotalPages   int           `json:"total_pages"`
}

func NewClient(cfg *config.Config) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.MongoDB.ConnectTimeout)
	defer cancel()

	uri := cfg.GetMongoURI()
	clientOptions := options.Client().ApplyURI(uri)

	clientOptions.SetMaxPoolSize(cfg.MongoDB.MaxPoolSize)
	clientOptions.SetMinPoolSize(cfg.MongoDB.MinPoolSize)
	clientOptions.SetPoolMonitor(&event.PoolMonitor{
		Event: func(e *event.PoolEvent) {
			switch e.Type {
			case event.ConnectionCreated:
				observability.MongoConnectionPoolSize.Inc()
			case event.ConnectionClosed:
				observability.MongoConnectionPoolSize.Dec()
			case event.GetSucceeded:
				observability.MongoConnectionPoolInUse.Inc()
			case event.ConnectionReturned:
				observability.MongoConnectionPoolInUse.Dec()
			}
		},
	})

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	log := logger.New()
	log.Info().Str("database", cfg.MongoDB.Database).Msg("Conexão estabelecida com sucesso")

	db := client.Database(cfg.MongoDB.Database)

	return &Client{
		client:   client,
		database: db,
		config:   cfg,
	}, nil
}

func (c *Client) GetStatements(ctx context.Context, contaID string, startDate, endDate time.Time, page, itemsPerPage int) (*StatementResult, error) {
	collection := c.database.Collection("transactions")

	filter := bson.M{
		"conta_id": contaID,
		"ocorrido_em": bson.M{
			"$gte": startDate,
			"$lte": endDate,
		},
	}

	ctx, cancel := context.WithTimeout(ctx, c.config.MongoDB.QueryTimeout)
	defer cancel()

	countStart := time.Now()
	totalItems, err := collection.CountDocuments(ctx, filter)
	countResult := "success"
	if err != nil {
		countResult = "error"
	}
	observability.MongoOperationDurationSeconds.WithLabelValues("count_documents", "transactions", countResult).Observe(time.Since(countStart).Seconds())
	if err != nil {
		return nil, fmt.Errorf("mongo count_documents conta_id=%s: %w", contaID, err)
	}

	skip := int64((page - 1) * itemsPerPage)

	findOptions := options.Find().
		SetSort(bson.D{{Key: "ocorrido_em", Value: -1}}).
		SetSkip(skip).
		SetLimit(int64(itemsPerPage))

	findStart := time.Now()
	cursor, err := collection.Find(ctx, filter, findOptions)
	findResult := "success"
	if err != nil {
		findResult = "error"
	}

	// Métricas
	observability.
		MongoOperationDurationSeconds.
		WithLabelValues("find", "transactions", findResult).
		Observe(time.Since(findStart).
			Seconds())

	if err != nil {
		return nil, fmt.Errorf("mongo find conta_id=%s page=%d: %w", contaID, page, err)
	}
	defer cursor.Close(ctx)

	var transactions []Transaction
	if err := cursor.All(ctx, &transactions); err != nil {
		observability.
			MongoOperationDurationSeconds.
			WithLabelValues("cursor_decode", "transactions", "error").Observe(0)
		return nil, fmt.Errorf("mongo cursor_decode conta_id=%s: %w", contaID, err)
	}

	if transactions == nil {
		transactions = []Transaction{}
	}
	observability.
		StatementTransactionsReturned.
		Observe(float64(len(transactions)))

	// Calcula total de páginas
	totalPages := int(totalItems) / itemsPerPage
	if int(totalItems)%itemsPerPage != 0 {
		totalPages++
	}

	return &StatementResult{
		Transactions: transactions,
		Page:         page,
		ItemsPerPage: itemsPerPage,
		TotalItems:   totalItems,
		TotalPages:   totalPages,
	}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, nil)
}

func (c *Client) Close() error {
	if c.client != nil {
		log := logger.New()
		log.Info().Str("database", c.config.MongoDB.Database).Msg("Fechando conexão")
		ctx, cancel := context.WithTimeout(context.Background(), c.config.MongoDB.QueryTimeout)
		defer cancel()
		return c.client.Disconnect(ctx)
	}
	return nil
}
