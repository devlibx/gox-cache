package redisCache

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/metrics"
	goxCache "github.com/devlibx/gox-cache"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
)

type redisCacheImpl struct {
	gox.CrossFunction
	config          *goxCache.Config
	redisClient     *redis.Client
	closeDoOnce     sync.Once
	logger          *zap.Logger
	prefix          string
	putCounter      metrics.Counter
	putCounterError metrics.Counter
	putTimer        metrics.Timer
	getCounter      metrics.Counter
	getCounterError metrics.Counter
	getTimer        metrics.Timer
}

func (r *redisCacheImpl) IsRunning(ctx context.Context) (bool, error) {
	result, err := r.redisClient.Ping(ctx).Result()
	if err != nil {
		return false, err
	} else if strings.ToUpper(result) == "PONG" {
		return true, nil
	}
	return false, nil
}

func (r *redisCacheImpl) Put(ctx context.Context, key string, data interface{}, ttlInSec int) error {
	t := r.putTimer.Start()
	defer t.Stop()

	var ttl time.Duration
	if ttlInSec > 0 {
		ttl = 100000 * time.Hour
	} else {
		ttl = time.Duration(ttlInSec) * time.Second
	}

	status := r.redisClient.Set(ctx, r.buildKeyName(key), data, ttl)
	result, err := status.Result()
	if err != nil {
		r.putCounterError.Inc(1)
		return errors.Wrap(err, "failed to put key in cache: name=%s, key=%s", r.config.Name, key)
	} else {
		r.putCounter.Inc(1)
		r.logger.Debug("key stored in cache", zap.String("name", r.config.Name), zap.String("key", key), zap.String("result", result))
		return nil
	}
}

func (r *redisCacheImpl) Get(ctx context.Context, key string) (interface{}, error) {
	t := r.getTimer.Start()
	defer t.Stop()

	result, err := r.redisClient.Get(ctx, r.buildKeyName(key)).Bytes()
	if err != nil {
		r.getCounterError.Inc(1)
		return nil, errors.Wrap(err, "failed to get key from cache: name=%s, key=%s", r.config.Name, key)
	} else {
		r.getCounter.Inc(1)
		r.logger.Debug("got key from cache", zap.String("name", r.config.Name), zap.String("key", key), zap.ByteString("result", result))
		return result, nil
	}
}

func (r *redisCacheImpl) GetAsMap(ctx context.Context, key string) (gox.StringObjectMap, error) {
	data, err := r.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	b := data.([]byte)
	result, err := gox.StringObjectMapFromString(string(b))
	if err != nil {
		r.getCounter.Inc(1)
		return nil, errors.Wrap(err, "failed to get key from cache: name=%s, key=%s", r.config.Name, key)
	}
	return result, nil
}

func (r *redisCacheImpl) Close() error {
	var err error
	r.closeDoOnce.Do(func() {
		err = r.redisClient.Close()
	})
	return err
}

func (r *redisCacheImpl) buildKeyName(key string) string {
	return fmt.Sprintf("%s_%s", r.prefix, key)
}

func (r *redisCacheImpl) Ping(ctx context.Context) (string, error) {
	return r.redisClient.Ping(ctx).Result()
}

func NewRedisCache(cf gox.CrossFunction, config *goxCache.Config) (goxCache.Cache, error) {
	prefix := config.Properties.StringOrDefault("prefix", "default")
	c := &redisCacheImpl{
		CrossFunction:   cf,
		config:          config,
		closeDoOnce:     sync.Once{},
		logger:          cf.Logger().Named("cache.redis").Named(prefix),
		prefix:          prefix,
		putCounter:      cf.Metric().Counter(prefix + "_put"),
		putCounterError: cf.Metric().Counter(prefix + "_put_error"),
		putTimer:        cf.Metric().Timer(prefix + "_put_time"),
		getCounter:      cf.Metric().Counter(prefix + "_get"),
		getCounterError: cf.Metric().Counter(prefix + "_get_error"),
		getTimer:        cf.Metric().Timer(prefix + "_get_time"),
	}

	c.redisClient = redis.NewClient(&redis.Options{
		Addr:     config.Endpoint,
		Password: config.Properties.StringOrEmpty("password"),
		DB:       config.Properties.IntOrZero("db"),
	})

	return c, nil
}
