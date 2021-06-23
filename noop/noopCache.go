package noopCache

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base"
	goxCache "github.com/devlibx/gox-cache"
	"go.uber.org/zap"
)

type noOpCacheImpl struct {
	logger *zap.Logger
}

func (n noOpCacheImpl) IsEnabled() bool {
	return false
}

func (n noOpCacheImpl) Publish(ctx context.Context, data gox.StringObjectMap) (interface{}, error) {
	return nil, nil
}

func (n noOpCacheImpl) Subscribe(ctx context.Context, callback goxCache.SubscribeCallbackFunc) error {
	return nil
}

func (n noOpCacheImpl) IsRunning(ctx context.Context) (bool, error) {
	return false, nil
}

func (n noOpCacheImpl) Put(ctx context.Context, key string, data interface{}, ttlInSec int) (string, error) {
	return key, &goxCache.CacheError{
		Err:       goxCache.ErrNoOpCacheError,
		Message:   fmt.Sprintf("[expected error] key not stored in  NOOP cache: key=%s", key),
		ErrorCode: "no_op_cache",
	}
}

func (n noOpCacheImpl) Get(ctx context.Context, key string) (interface{}, string, error) {
	return nil, key, &goxCache.CacheError{
		Err:       goxCache.ErrNoOpCacheError,
		Message:   fmt.Sprintf("[expected error] key not found in  NOOP cache: key=%s", key),
		ErrorCode: "no_op_cache",
	}
}

func (n noOpCacheImpl) GetAsMap(ctx context.Context, key string) (gox.StringObjectMap, string, error) {
	return nil, key, &goxCache.CacheError{
		Err:       goxCache.ErrNoOpCacheError,
		Message:   fmt.Sprintf("[expected error] key not found in  NOOP cache: key=%s", key),
		ErrorCode: "no_op_cache",
	}
}

func (n noOpCacheImpl) Close() error {
	return nil
}

func NewNoOpCache(cf gox.CrossFunction, config *goxCache.Config) (goxCache.Cache, error) {
	c := &noOpCacheImpl{
		logger: cf.Logger().Named("cache.noop"),
	}
	return c, nil
}
