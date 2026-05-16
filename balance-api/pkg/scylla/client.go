package scylla

import (
	"balance-api/pkg/config"
	"balance-api/pkg/logger"
	"balance-api/pkg/observability"
	"context"
	"time"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
	"github.com/sony/gobreaker/v2"
)

const balanceQuery = `SELECT balance FROM balances WHERE id = ?`

type Client struct {
	session *gocql.Session
	config  *config.Config
	cb      *gobreaker.CircuitBreaker[float64]
	log     zerolog.Logger
}

func NewClient(cfg *config.Config) (*Client, error) {
	cluster := gocql.NewCluster(cfg.Scylla.Hosts...)
	cluster.Keyspace = cfg.Scylla.Keyspace

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

	if cfg.Scylla.Username != "" && cfg.Scylla.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Scylla.Username,
			Password: cfg.Scylla.Password,
		}
	}

	log := logger.Instance()

	log.Info().
		Strs("hosts", cfg.Scylla.Hosts).
		Str("keyspace", cfg.Scylla.Keyspace).
		Str("consistency", cfg.Scylla.Consistency).
		Msg("conectando ao scylladb")

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	log.Info().Msg("conexao com scylladb estabelecida")

	cb := gobreaker.NewCircuitBreaker[float64](gobreaker.Settings{
		Name:        "scylla-balance",
		MaxRequests: 5,
		Interval:    30 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.Requests >= 20 &&
				float64(counts.TotalFailures)/float64(counts.Requests) >= 0.5
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			observability.CircuitBreakerTransitions.WithLabelValues(name, from.String(), to.String()).Inc()
			switch to {
			case gobreaker.StateClosed:
				observability.CircuitBreakerState.WithLabelValues(name).Set(0)
			case gobreaker.StateHalfOpen:
				observability.CircuitBreakerState.WithLabelValues(name).Set(1)
			case gobreaker.StateOpen:
				observability.CircuitBreakerState.WithLabelValues(name).Set(2)
			}
		},
	})

	// estado inicial: closed
	observability.CircuitBreakerState.WithLabelValues("scylla-balance").Set(0)

	return &Client{
		session: session,
		config:  cfg,
		cb:      cb,
		log:     log,
	}, nil
}

func (c *Client) GetBalance(ctx context.Context, id gocql.UUID) (float64, error) {
	start := time.Now()

	balance, err := c.cb.Execute(func() (float64, error) {
		var balance float64
		if err := c.session.Query(balanceQuery, id).WithContext(ctx).Idempotent(true).Scan(&balance); err != nil {
			return 0, err
		}
		return balance, nil
	})

	duration := time.Since(start).Seconds()
	result := "success"
	if err != nil {
		result = "error"
	}
	observability.ScyllaQueriesTotal.WithLabelValues("get_balance", result).Inc()
	observability.ScyllaQueryDurationSeconds.WithLabelValues("get_balance").Observe(duration)

	return balance, err
}

func (c *Client) Ping(ctx context.Context) error {
	return c.session.Query("SELECT now() FROM system.local").WithContext(ctx).Exec()
}

func (c *Client) Close() {
	if c.session != nil {
		c.log.Info().Msg("fechando conexao com scylladb")
		c.session.Close()
	}
}
