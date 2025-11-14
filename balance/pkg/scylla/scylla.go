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

func (c *Client) InitSchema() error {
	// Cria keyspace se não existir
	createKeyspace := `
		CREATE KEYSPACE IF NOT EXISTS balance_ks
		WITH replication = {
			'class': 'SimpleStrategy',
			'replication_factor': 1
		}
	`

	if err := c.session.Query(createKeyspace).Exec(); err != nil {
		log.Printf("[ScyllaDB] Erro ao criar keyspace: %v", err)
		return err
	}

	log.Printf("[ScyllaDB] Keyspace 'balance_ks' criado/verificado")

	// Cria tabela de saldos
	createTable := `
		CREATE TABLE IF NOT EXISTS balance_ks.balances (
			id UUID PRIMARY KEY,
			balance DOUBLE,
			version INT
		)
	`

	if err := c.session.Query(createTable).Exec(); err != nil {
		log.Printf("[ScyllaDB] Erro ao criar tabela: %v", err)
		return err
	}

	log.Printf("[ScyllaDB] Tabela 'balances' criada/verificada")

	return nil
}

func (c *Client) InsertInitialBalance(id gocql.UUID, balance float64, version int) error {
	query := `INSERT INTO balance_ks.balances (id, balance, version) VALUES (?, ?, ?) IF NOT EXISTS`

	if err := c.session.Query(query, id, balance, version).Exec(); err != nil {
		log.Printf("[ScyllaDB] Erro ao inserir dados iniciais para conta %s: %v", id, err)
		return err
	}

	return nil
}

func (c *Client) InsertBalance(id gocql.UUID, balance float64, version int) error {
	query := `UPDATE balance_ks.balances SET balance = ?, version = ? WHERE id = ? IF version < ?`

	if err := c.session.Query(query, balance, version, id, version).Exec(); err != nil {
		log.Printf("[ScyllaDB] Erro ao atualizar o saldo para conta %s: %v", id, err)
		return err
	}

	return nil
}

func (c *Client) GetBalance(id gocql.UUID) (float64, int, error) {
	var balance float64
	var version int

	query := `SELECT balance, version FROM balance_ks.balances WHERE id = ?`

	if err := c.session.Query(query, id).Scan(&balance, &version); err != nil {
		return 0, 0, err
	}

	return balance, version, nil
}

func (c *Client) Close() {
	if c.session != nil {
		log.Printf("[ScyllaDB] Fechando conexão")
		c.session.Close()
	}
}
