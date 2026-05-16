package scylla

import (
	"balance-api/pkg/config"
	"context"
	"log"

	"github.com/gocql/gocql"
)

type Client struct {
	session *gocql.Session
	config  *config.Config
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

	return &Client{
		session: session,
		config:  cfg,
	}, nil
}

func (c *Client) GetBalance(ctx context.Context, id gocql.UUID) (float64, error) {
	var balance float64
	query := `SELECT balance FROM balances WHERE id = ?`
	if err := c.session.Query(query, id).WithContext(ctx).Scan(&balance); err != nil {
		return 0, err
	}
	return balance, nil
}

func (c *Client) Close() {
	if c.session != nil {
		log.Printf("[ScyllaDB] Fechando conexão")
		c.session.Close()
	}
}
