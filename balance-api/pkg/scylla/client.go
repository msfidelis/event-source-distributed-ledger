package scylla

import (
	"log"
	"time"

	"github.com/gocql/gocql"
)

type Client struct {
	session *gocql.Session
}

func NewClient(hosts []string) (*Client, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "balance_ks"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.ProtoVersion = 4
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

	log.Printf("[ScyllaDB] Conectando aos hosts: %v", hosts)

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	log.Printf("[ScyllaDB] Conexão estabelecida com sucesso")

	return &Client{session: session}, nil
}

func (c *Client) GetBalance(id string) (float64, error) {
	var balance float64

	query := `SELECT balance FROM balances WHERE id = ?`

	uuid, err := gocql.ParseUUID(id)
	if err != nil {
		return 0, err
	}

	if err := c.session.Query(query, uuid).Scan(&balance); err != nil {
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
