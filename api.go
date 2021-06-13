package goxCache

import (
	"context"
	"github.com/devlibx/gox-base"
)

//go:generate mockgen -source=api.go -destination=./mocks/mock_api.go -package=mockGoxCache

type Config struct {
	Name       string
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

type Cache interface {
	IsRunning(ctx context.Context) (bool, error)
	Put(ctx context.Context, key string, data interface{}, ttlInSec int) error
	Get(ctx context.Context, key string) (interface{}, error)
	GetAsMap(ctx context.Context, key string) (gox.StringObjectMap, error)
	Close() error
}

type Registry interface {
	RegisterCache(config *Config) (Cache, error)
	GetCache(name string) (Cache, error)
	Close() error
}
