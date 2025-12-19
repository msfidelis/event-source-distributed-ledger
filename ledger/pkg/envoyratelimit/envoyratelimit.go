package envoyratelimit

import (
	"context"
	"fmt"
	"sync"

	ratelimitpb "github.com/envoyproxy/go-control-plane/envoy/extensions/common/ratelimit/v3"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client encapsula a conexão gRPC com o Envoy Rate Limiter
type Client struct {
	conn   *grpc.ClientConn
	client pb.RateLimitServiceClient
}

var (
	instance *Client
	once     sync.Once
	initErr  error
)

// GetInstance retorna a instância singleton do cliente Rate Limiter
func GetInstance(addr string) (*Client, error) {
	once.Do(func() {
		instance, initErr = newClient(addr)
	})
	return instance, initErr
}

// newClient cria uma nova conexão com o Envoy Rate Limiter via gRPC (privado)
func newClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("erro ao conectar ao Envoy Rate Limiter: %w", err)
	}

	client := pb.NewRateLimitServiceClient(conn)

	return &Client{
		conn:   conn,
		client: client,
	}, nil
}

// NewClient cria uma nova conexão com o Envoy Rate Limiter via gRPC
// Deprecated: Use GetInstance para obter a instância singleton
func NewClient(addr string) (*Client, error) {
	return GetInstance(addr)
}

// ShouldRateLimit verifica se uma requisição deve ser limitada
func (c *Client) ShouldRateLimit(ctx context.Context, domain string, key string, value string) (bool, error) {
	req := &pb.RateLimitRequest{
		Domain: domain,
		Descriptors: []*ratelimitpb.RateLimitDescriptor{
			{
				Entries: []*ratelimitpb.RateLimitDescriptor_Entry{
					{
						Key:   key,
						Value: value,
					},
				},
			},
		},
		HitsAddend: 1,
	}

	resp, err := c.client.ShouldRateLimit(ctx, req)
	if err != nil {
		return false, fmt.Errorf("erro ao verificar rate limit: %w", err)
	}

	// OverallCode OK significa que a requisição pode prosseguir
	// OverallCode OVER_LIMIT significa que atingiu o limite
	allowed := resp.OverallCode == pb.RateLimitResponse_OK

	return allowed, nil
}

// Close fecha a conexão gRPC
func (c *Client) Close() error {
	return c.conn.Close()
}
