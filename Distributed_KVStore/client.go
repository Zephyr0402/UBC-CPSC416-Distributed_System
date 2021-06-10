package distkvs

import (
	"errors"
	"example.org/cpsc416/a6/kvslib"
	"github.com/DistributedClocks/tracing"
)

const ChCapacity = 100

type ClientConfig struct {
	ClientId         string
	FrontEndAddr     string
	TracerServerAddr string
	TracerSecret     []byte
	ClientID         string
}

type Client struct {
	NotifyChannel kvslib.NotifyChannel
	id            string
	frontEndAddr  string
	kvs           *kvslib.KVS
	tracer        *tracing.Tracer
	initialized   bool
	tracerConfig  tracing.TracerConfig
}

func NewClient(config ClientConfig, kvs *kvslib.KVS) *Client {
	tracerConfig := tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.ClientID,
		Secret:         config.TracerSecret,
	}
	client := &Client{
		id:           config.ClientID,
		frontEndAddr: config.FrontEndAddr,
		kvs:          kvs,
		tracerConfig: tracerConfig,
		initialized:  false,
	}
	return client
}

func (c *Client) Initialize() error {
	if c.initialized {
		return errors.New("client has been initialized before")
	}
	c.tracer = tracing.NewTracer(c.tracerConfig)

	ch, err := c.kvs.Initialize(c.tracer, c.id, c.frontEndAddr, ChCapacity)
	c.NotifyChannel = ch
	c.initialized = true
	return err
}

func (c *Client) Get(clientId string, key string) (uint32, error) {
	return c.kvs.Get(c.tracer, clientId, key)
}

func (c *Client) Put(clientId string, key string, value string) (uint32, error) {
	return c.kvs.Put(c.tracer, clientId, key, value)
}

func (c *Client) Close() error {
	if err := c.kvs.Close(); err != nil {
		return err
	}
	if c.tracer != nil {
		if err := c.tracer.Close(); err != nil {
			return err
		}
	}

	c.initialized = false
	return nil
}
