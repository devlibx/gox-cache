package inmemoryCache

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	goxCache "github.com/devlibx/gox-cache/v2"
	"github.com/dgraph-io/ristretto/v2"
	_ "github.com/dgraph-io/ristretto/v2"
	"time"
)

type inMemoryCache struct {
	config *goxCache.Config
	cache  *ristretto.Cache[string, interface{}]
}

func NewInMemoryCache(cf gox.CrossFunction, config *goxCache.Config) (goxCache.Cache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, interface{}]{
		NumCounters: int64(config.Properties.IntOrDefault("num_counters", 1e7)),
		MaxCost:     int64(config.Properties.IntOrDefault("max_cost", 1<<30)),
		BufferItems: int64(config.Properties.IntOrDefault("buffer_items", 64)),
	})
	if err != nil {
		return nil, err
	}

	return &inMemoryCache{
		config: config,
		cache:  cache,
	}, nil
}

func (i *inMemoryCache) IsEnabled() bool {
	return !i.config.Disabled
}

func (i *inMemoryCache) IsRunning(ctx context.Context) (bool, error) {
	return !i.config.Disabled, nil
}

func (i *inMemoryCache) Put(ctx context.Context, key string, data interface{}, ttlInSec int) (string, error) {
	ttl := time.Duration(ttlInSec) * time.Second
	if ttlInSec <= 0 {
		ttl = time.Duration(24*365*10) * time.Hour
	}
	i.cache.SetWithTTL(key, data, 1, ttl)
	i.cache.Wait()
	return key, nil
}

func (i *inMemoryCache) Get(ctx context.Context, key string) (interface{}, string, error) {
	data, found := i.cache.Get(key)
	if !found {
		return nil, "", errors.New("key not found")
	}
	return data, key, nil
}

func (i *inMemoryCache) Delete(ctx context.Context, key string) error {
	i.cache.Del(key)
	return nil
}

func (i *inMemoryCache) Close() error {
	i.cache.Close()
	return nil
}

func (i *inMemoryCache) MPut(ctx context.Context, dataMap map[string]interface{}) error {
	return errors.New("not implemented")
}

func (i *inMemoryCache) MGet(ctx context.Context, keys []string) ([]interface{}, []string, error) {
	return nil, nil, errors.New("not implemented")
}

func (i *inMemoryCache) GetAsMap(ctx context.Context, key string) (gox.StringObjectMap, string, error) {
	return nil, "", errors.New("not implemented")
}

func (i *inMemoryCache) Publish(ctx context.Context, data gox.StringObjectMap) (interface{}, error) {
	return nil, errors.New("not implemented")
}

func (i *inMemoryCache) Subscribe(ctx context.Context, callback goxCache.SubscribeCallbackFunc) error {
	return errors.New("not implemented")
}
