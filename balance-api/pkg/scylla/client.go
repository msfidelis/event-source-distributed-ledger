package scylla

import (
	"balance-api/pkg/config"
	"context"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/sony/gobreaker/v2"
)

const balanceQuery = `SELECT balance FROM balances WHERE id = ?`

type Client struct {
	session *gocql.Session
	config  *config.Config
	cb      *gobreaker.CircuitBreaker[float64]
}

func NewClient(cfg *config.Config) (*Client, error) {
	cluster := gocql.NewCluster(cfg.Scylla.Hosts...)
	cluster.Keyspace = cfg.Scylla.Keyspace

	// Set consistency level
	switch cfg.Scylla.Consistency {
	case "ONE":
		cluster.Consistency = gocql.One
	case "QUORUM":
		cluster.Consistency = gocql.Quorum
	case "ALL":
		cluster.Consistency = gocql.All
	case "LOCAL_QUORUM":
		cluster.Consistency = gocql.LocalQuorum
	default:
		cluster.Consistency = gocql.Quorum
	}

	cluster.Timeout = cfg.Scylla.Timeout
	cluster.ConnectTimeout = cfg.Scylla.ConnectTimeout
	cluster.ProtoVersion = cfg.Scylla.ProtoVersion
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: cfg.Scylla.RetryAttempts}

	// Set authentication if provided
	if cfg.Scylla.Username != "" && cfg.Scylla.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Scylla.Username,
			Password: cfg.Scylla.Password,
		}
	}

	log.Printf("[ScyllaDB] Conectando aos hosts: %v", cfg.Scylla.Hosts)
	log.Printf("[ScyllaDB] Keyspace: %s, Consistency: %s", cfg.Scylla.Keyspace, cfg.Scylla.Consistency)

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	log.Printf("[ScyllaDB] Conexão estabelecida com sucesso")

	cb := gobreaker.NewCircuitBreaker[float64](gobreaker.Settings{
		Name:        "scylla-balance",
		MaxRequests: 5,
		Interval:    30 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.Requests >= 20 &&
				float64(counts.TotalFailures)/float64(counts.Requests) >= 0.5
		},
	})

	return &Client{
		session: session,
		config:  cfg,
		cb:      cb,
	}, nil
}

func (c *Client) GetBalance(ctx context.Context, id gocql.UUID) (float64, error) {
	return c.cb.Execute(func() (float64, error) {
		var balance float64
		if err := c.session.Query(balanceQuery, id).WithContext(ctx).Idempotent(true).Scan(&balance); err != nil {
			return 0, err
		}
		return balance, nil
	})
}

func (c *Client) Ping(ctx context.Context) error {
	return c.session.Query("SELECT now() FROM system.local").WithContext(ctx).Exec()
}

func (c *Client) Close() {
	if c.session != nil {
		log.Printf("[ScyllaDB] Fechando conexão")
		c.session.Close()
	}
}
