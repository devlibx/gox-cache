package cache

import (
	"context"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-cache"
	noopCache "github.com/devlibx/gox-cache/noop"
	redisCache "github.com/devlibx/gox-cache/redis"
	"go.uber.org/zap"
	"strings"
	"sync"
)

type registryImpl struct {
	gox.CrossFunction
	ctx         context.Context
	caches      map[string]goxCache.Cache
	closeDoOnce sync.Once
}

func (r *registryImpl) Close() error {
	r.closeDoOnce.Do(func() {
		for _, c := range r.caches {
			_ = c.Close()
		}
	})
	return nil
}

func (r *registryImpl) RegisterCache(config *goxCache.Config) (goxCache.Cache, error) {
	if config.Enabled {
		switch strings.ToLower(config.Type) {
		case "redis":
			cache, err := redisCache.NewRedisCache(r.CrossFunction, &goxCache.Config{})
			if err != nil {
				return nil, errors.Wrap(err, "failed to register cache to registry: name=%s", config.Name)
			} else {
				r.caches[config.Name] = cache
				return cache, err
			}
		}
	} else {
		cache, _ := noopCache.NewNoOpCache(r.CrossFunction, config)
		r.caches[config.Name] = cache
	}
	return nil, errors.New("failed to register cache to registry: name=%s", config.Name)
}

func (r *registryImpl) GetCache(name string) (goxCache.Cache, error) {
	if c, ok := r.caches[name]; ok {
		return c, nil
	} else {
		cache, _ := noopCache.NewNoOpCache(r.CrossFunction, &goxCache.Config{Name: name})
		r.caches[name] = cache
		return cache, nil
	}
}

func NewRegistry(ctx context.Context, cf gox.CrossFunction, configuration goxCache.Configuration) (goxCache.Registry, error) {
	r := &registryImpl{
		ctx:           ctx,
		CrossFunction: cf,
		caches:        map[string]goxCache.Cache{},
		closeDoOnce:   sync.Once{},
	}

	for name, c := range configuration.Providers {
		c.Name = name
		if _, err := r.RegisterCache(&c); err != nil {
			return nil, err
		}
	}

	go func() {
		<-ctx.Done()
		err := r.Close()
		if err != nil {
			cf.Logger().Error("failed to close cache registry", zap.Error(err))
		}
	}()

	return r, nil
}
