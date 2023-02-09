package goxCache

import (
	"context"
	"fmt"

	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
)

//go:generate mockgen -source=api.go -destination=./mocks/mock_api.go -package=mockGoxCache

// Cache error
type CacheError struct {
	Err       error
	Message   string
	ErrorCode string
}

var ErrNoOpCacheError = errors.New("no_op_cache")

// Configuration to setup a cache
type Config struct {
	Name       string
	Prefix     string              `yaml:"prefix"`
	Type       string              `yaml:"type"`
	Endpoint   string              `yaml:"endpoint"`
	Endpoints  []string            `yaml:"endpoints"`
	Disabled   bool                `yaml:"disabled"`
	Clustered  bool                `yaml:"clustered"`
	Properties gox.StringObjectMap `yaml:"properties"`
}

// Configuration to setup a cache registry
type Configuration struct {
	Disabled   bool                `yaml:"disabled"`
	Properties gox.StringObjectMap `yaml:"properties"`
	Providers  map[string]Config   `yaml:"caches"`
}

type SubscribeCallbackFunc func(data gox.StringObjectMap) error

type Cache interface {
	// If this cache is enabled or not
	IsEnabled() bool

	// If the underlying server backing this cache is running or not e.g. for redis backed cache it will check if
	// redis server is running or not
	IsRunning(ctx context.Context) (bool, error)

	// Put a key with given name. TTL=0 means never expire
	Put(ctx context.Context, key string, data interface{}, ttlInSec int) (string, error)

	// Put multiple key,value pairs with given name. No TTL supported
	MPut(ctx context.Context, dataMap map[string]interface{}) error

	// Get data for given key
	Get(ctx context.Context, key string) (interface{}, string, error)

	// Get data for a list of keys
	MGet(ctx context.Context, keys []string) ([]interface{}, []string, error)

	// Get data for given key and convert it to StringObjectMap before returning it
	// Error will be returned if key is not found or if value cannot be converted to StringObjectMap
	GetAsMap(ctx context.Context, key string) (gox.StringObjectMap, string, error)

	// Publish data to this cache which other client can subscribe
	Publish(ctx context.Context, data gox.StringObjectMap) (interface{}, error)

	// Subscribe to data in this cache
	Subscribe(ctx context.Context, callback SubscribeCallbackFunc) error

	// Delete a key from this cache
	Delete(ctx context.Context, key string) error

	// Close and shutdown underlying connections
	Close() error
}

type Registry interface {

	// Register a new cache with given config
	RegisterCache(config *Config) (Cache, error)

	// Get a cache with name, error if cache was not registered before get
	GetCache(name string) (Cache, error)

	// Run a health check and give final result
	// If a registry is disabled then we get status=ok. If individual caches are disabled they are marked as status=ok
	HealthCheck(ctx context.Context) (gox.StringObjectMap, error)

	// Shutdown the registry i.e. stop all underlying connections
	Close() error
}

// Give a error as string
func (e *CacheError) Error() string {
	if e.Err == nil {
		return ""
	} else {
		return fmt.Sprintf("error=%s, erroCode=%s", e.Err.Error(), e.ErrorCode)
	}
}

// Build string representation
func (e *CacheError) Unwrap() error {
	return e.Err
}
