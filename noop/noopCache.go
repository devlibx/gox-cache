package noopCache

import (
	"context"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	goxCache "github.com/devlibx/gox-cache"
	"go.uber.org/zap"
)

type noOpCacheImpl struct {
	logger *zap.Logger
}

func (n noOpCacheImpl) IsRunning(ctx context.Context) (bool, error) {
	return false, nil
}

func (n noOpCacheImpl) Put(ctx context.Context, key string, data interface{}, ttlInSec int) error {
	return nil
}

func (n noOpCacheImpl) Get(ctx context.Context, key string) (interface{}, error) {
	return nil, errors.New("[expected error] key not found in  NOOP cache: key=%s", key)
}

func (n noOpCacheImpl) GetAsMap(ctx context.Context, key string) (gox.StringObjectMap, error) {
	return nil, errors.New("[expected error] key not found in  NOOP cache: key=%s", key)
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
