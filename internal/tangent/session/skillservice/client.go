package skillservice

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/tansive/tansive-internal/internal/tangent/session/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type Client struct {
	conn   *grpc.ClientConn
	client proto.SkillServiceClient
}

type ClientOption func(*clientConfig)

type clientConfig struct {
	dialTimeout time.Duration
	maxRetries  int
	retryDelay  time.Duration
}

func WithDialTimeout(timeout time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.dialTimeout = timeout
	}
}

func WithMaxRetries(maxRetries int) ClientOption {
	return func(c *clientConfig) {
		c.maxRetries = maxRetries
	}
}

func WithRetryDelay(delay time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.retryDelay = delay
	}
}

func NewClient(opts ...ClientOption) (*Client, error) {
	config := &clientConfig{
		dialTimeout: 5 * time.Second,
		maxRetries:  3,
		retryDelay:  100 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(config)
	}

	socketPath, err := GetSocketPath()
	if err != nil {
		return nil, fmt.Errorf("failed to get socket path: %w", err)
	}

	var conn *grpc.ClientConn
	var lastErr error

	for i := 0; i < config.maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), config.dialTimeout)
		defer cancel()

		conn, err = grpc.DialContext(
			ctx,
			"unix://"+socketPath,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err == nil {
			break
		}
		lastErr = err

		if i < config.maxRetries-1 {
			log.Debug().
				Err(err).
				Int("attempt", i+1).
				Int("max_attempts", config.maxRetries).
				Msg("failed to connect to skill service, retrying")
			time.Sleep(config.retryDelay)
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to connect to skill service after %d attempts: %w", config.maxRetries, lastErr)
	}

	return &Client{
		conn:   conn,
		client: proto.NewSkillServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) InvokeSkill(ctx context.Context, sessionID, invocationID, skillName string, args map[string]interface{}) (*proto.SkillResult, error) {
	argsStruct, err := structpb.NewStruct(args)
	if err != nil {
		return nil, fmt.Errorf("failed to convert args to struct: %w", err)
	}

	invocation := &proto.SkillInvocation{
		SessionId:    sessionID,
		InvocationId: invocationID,
		SkillName:    skillName,
		Args:         argsStruct,
	}

	result, err := c.client.InvokeSkill(ctx, invocation)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			return nil, fmt.Errorf("skill invocation failed: %s", st.Message())
		}
		return nil, fmt.Errorf("skill invocation failed: %w", err)
	}

	return result, nil
}
