package goxCache

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
)

//go:generate mockgen -source=api.go -destination=./mocks/mock_api.go -package=mockGoxCache

type CacheError struct {
	Err       error
	Message   string
	ErrorCode string
}

var ErrNoOpCacheError = errors.New("no_op_cache")

type Config struct {
	Name       string
	Prefix     string              `yaml:"prefix"`
	Type       string              `yaml:"type"`
	Endpoint   string              `yaml:"endpoint"`
	Disabled   bool                `yaml:"disabled"`
	Properties gox.StringObjectMap `yaml:"properties"`
}

type Configuration struct {
	Disabled   bool                `yaml:"disabled"`
	Properties gox.StringObjectMap `yaml:"properties"`
	Providers  map[string]Config   `yaml:"caches"`
}

type SubscribeCallbackFunc func(data gox.StringObjectMap) error

type Cache interface {
	IsRunning(ctx context.Context) (bool, error)
	Put(ctx context.Context, key string, data interface{}, ttlInSec int) (string, error)
	Get(ctx context.Context, key string) (interface{}, string, error)
	GetAsMap(ctx context.Context, key string) (gox.StringObjectMap, string, error)
	Publish(ctx context.Context, data gox.StringObjectMap) (interface{}, error)
	Subscribe(ctx context.Context, callback SubscribeCallbackFunc) error
	Close() error
}

type Registry interface {
	RegisterCache(config *Config) (Cache, error)
	GetCache(name string) (Cache, error)
	Close() error
}

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
