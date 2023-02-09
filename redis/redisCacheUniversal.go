package redisCache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/metrics"
	"github.com/devlibx/gox-base/serialization"
	"github.com/devlibx/gox-base/util"
	goxCache "github.com/devlibx/gox-cache"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// NewUniversalClient
type universalRedisCacheImpl struct {
	gox.CrossFunction
	config             *goxCache.Config
	putTimeoutMs       int
	getTimeoutMs       int
	redisClient        redis.UniversalClient
	pubSubTopicName    string
	closeDoOnce        sync.Once
	closed             bool
	logger             *zap.Logger
	prefix             string
	putCounter         metrics.Counter
	putCounterError    metrics.Counter
	putTimer           metrics.Timer
	getCounter         metrics.Counter
	getCounterError    metrics.Counter
	deleteCounter      metrics.Counter
	deleteCounterError metrics.Counter
	getTimer           metrics.Timer
}

func (r *universalRedisCacheImpl) IsEnabled() bool {
	return !r.config.Disabled
}

func (r *universalRedisCacheImpl) IsRunning(ctx context.Context) (bool, error) {
	result, err := r.redisClient.Ping(ctx).Result()
	if err != nil {
		return false, err
	} else if strings.ToUpper(result) == "PONG" {
		return true, nil
	}
	return false, nil
}

func (r *universalRedisCacheImpl) Put(ctx context.Context, key string, data interface{}, ttlInSec int) (string, error) {
	t := r.putTimer.Start()
	defer t.Stop()

	ctxWithTimeout, cf := context.WithTimeout(ctx, time.Duration(r.putTimeoutMs)*time.Millisecond)
	defer cf()

	var ttl time.Duration
	if ttlInSec > 0 {
		ttl = 100000 * time.Hour
	} else {
		ttl = time.Duration(ttlInSec) * time.Second
	}

	keyToStore := r.buildKeyName(key)
	status := r.redisClient.Set(ctxWithTimeout, keyToStore, data, ttl)
	result, err := status.Result()
	if err != nil {
		r.putCounterError.Inc(1)
		return keyToStore, errors.Wrap(err, "failed to put key in cache: name=%s, key=%s, internalKeyUsedToStore=%s", r.config.Name, key, keyToStore)
	} else {
		r.putCounter.Inc(1)
		r.logger.Debug("key stored in cache", zap.String("name", r.config.Name), zap.String("key", key), zap.String("internalKeyUsedToStore", keyToStore), zap.String("result", result))
		return keyToStore, nil
	}
}

func (r *universalRedisCacheImpl) MPut(ctx context.Context, dataMap map[string]interface{}) error {
	t := r.putTimer.Start()
	defer t.Stop()

	ctxWithTimeout, cf := context.WithTimeout(ctx, time.Duration(r.putTimeoutMs)*time.Millisecond)
	defer cf()

	prefixedDataMap, allKeys, internalKeysToStore := r.buildMapForMultiPut(dataMap)

	var status *redis.StatusCmd
	status = r.redisClient.MSet(ctxWithTimeout, prefixedDataMap)

	result, err := status.Result()

	if err != nil {
		r.putCounterError.Inc(1)
		return errors.Wrap(err, "failed to put keys in cache name=%s, key=%v, internalKeyUsedToStore=%v", r.config.Name, allKeys, internalKeysToStore)
	} else {
		r.putCounter.Inc(1)
		r.logger.Debug("key stored in cache", zap.String("name", r.config.Name), zap.Any("keys", allKeys), zap.String("result", result))
		return nil
	}
}

func (r *universalRedisCacheImpl) Get(ctx context.Context, key string) (interface{}, string, error) {
	t := r.getTimer.Start()
	defer t.Stop()

	ctxWithTimeout, cf := context.WithTimeout(ctx, time.Duration(r.getTimeoutMs)*time.Millisecond)
	defer cf()

	keyToStore := r.buildKeyName(key)
	result, err := r.redisClient.Get(ctxWithTimeout, keyToStore).Bytes()
	if err != nil {
		r.getCounterError.Inc(1)
		return nil, keyToStore, errors.Wrap(err, "failed to get key from cache: name=%s, key=%s, internalKeyUsedToStore=%s", r.config.Name, key, keyToStore)
	} else {
		r.getCounter.Inc(1)
		r.logger.Debug("got key from cache", zap.String("name", r.config.Name), zap.String("key", key), zap.String("internalKeyUsedToStore", keyToStore), zap.ByteString("result", result))
		return result, keyToStore, nil
	}
}

func (r *universalRedisCacheImpl) MGet(ctx context.Context, keys []string) ([]interface{}, []string, error) {
	t := r.getTimer.Start()
	defer t.Stop()

	ctxWithTimeout, cf := context.WithTimeout(ctx, time.Duration(r.getTimeoutMs)*time.Millisecond)
	defer cf()

	keysToStore := []string{}
	for _, key := range keys {
		keysToStore = append(keysToStore, r.buildKeyName(key))
	}

	result, err := r.redisClient.MGet(ctxWithTimeout, keysToStore...).Result()
	if err != nil {
		r.getCounterError.Inc(1)
		return nil, keysToStore, errors.Wrap(err, "failed to get key from cache: name=%s, key=%v, internalKeyUsedToStore=%v", r.config.Name, keys, keysToStore)
	} else {
		r.getCounter.Inc(1)
		r.logger.Debug("got keys from cache", zap.String("name", r.config.Name), zap.Any("key", keys), zap.Any("internalKeysUsedToStore", keysToStore), zap.Any("result", result))
		return result, keysToStore, nil
	}

}

func (r *universalRedisCacheImpl) GetAsMap(ctx context.Context, key string) (gox.StringObjectMap, string, error) {
	data, keyToStore, err := r.Get(ctx, key)
	if err != nil {
		return nil, keyToStore, err
	}

	b := data.([]byte)
	result, err := gox.StringObjectMapFromString(string(b))
	if err != nil {
		r.getCounter.Inc(1)
		return nil, keyToStore, errors.Wrap(err, "failed to get key from cache: name=%s, key=%s", r.config.Name, key)
	}
	return result, keyToStore, nil
}

func (r *universalRedisCacheImpl) Publish(ctx context.Context, data gox.StringObjectMap) (interface{}, error) {

	dataStr, err := serialization.Stringify(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert input to string: name=%s", r.config.Name)
	}

	publishErr := r.redisClient.Publish(ctx, r.pubSubTopicName, dataStr)
	if publishErr != nil {
		return publishErr, errors.Wrap(publishErr.Err(), "failed to publish event to redis: name=%s, channelName=%s", r.config.Name, r.pubSubTopicName)
	} else {
		return publishErr, nil
	}
}

func (r *universalRedisCacheImpl) Subscribe(ctx context.Context, callback goxCache.SubscribeCallbackFunc) error {

	// Wait for confirmation that subscription is created before publishing anything.
	pubSub := r.redisClient.Subscribe(ctx, r.pubSubTopicName)
	_, err := pubSub.Receive(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to setup subscribe in redis: name=%s, channelName=%s", r.config.Name, r.pubSubTopicName)
	}

	// Get message channel
	messageChannel := pubSub.Channel()

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)

	exitLoop:
		for {
			select {
			case <-ctx.Done():
				break exitLoop

			case <-ticker.C:
				if r.closed {
					break exitLoop
				}

			case msg, open := <-messageChannel:
				if open {
					payload, err := gox.StringObjectMapFromString(msg.Payload)
					if err != nil {
						r.logger.Error("error in reading data from redis pub/sub", zap.String("payload", msg.Payload))
					} else {
						err := callback(payload)
						if err != nil {
							r.logger.Error("error in subscriber callback for redis pub/sub: data=%s", zap.String("payload", msg.Payload))
						}
					}
				} else {
					break exitLoop
				}
			}
		}

		_ = pubSub.Close()
		ticker.Stop()
		r.logger.Info("closing pub/sub loop")
	}()
	return nil
}

func (r *universalRedisCacheImpl) Delete(ctx context.Context, key string) error {
	t := r.getTimer.Start()
	defer t.Stop()

	ctxWithTimeout, cf := context.WithTimeout(ctx, time.Duration(r.getTimeoutMs)*time.Millisecond)
	defer cf()

	keyToStore := r.buildKeyName(key)

	err := r.redisClient.Del(ctxWithTimeout, keyToStore).Err()
	if err != nil {
		r.deleteCounterError.Inc(1)
		return errors.Wrap(err, "failed to delete key from cache: name=%s, key=%s, internalKeyUsedToStore=%s", r.config.Name, key, keyToStore)
	} else {
		r.deleteCounter.Inc(1)
		r.logger.Debug("deleted key from cache", zap.String("name", r.config.Name), zap.String("key", key), zap.String("internalKeyUsedToStore", keyToStore))
		return nil
	}
}

func (r *universalRedisCacheImpl) Close() error {
	var err error
	r.closeDoOnce.Do(func() {
		err = r.redisClient.Close()
		r.closed = true
	})
	return err
}

func (r *universalRedisCacheImpl) buildKeyName(key string) string {
	return fmt.Sprintf("%s_%s", r.prefix, key)
}

func (r *universalRedisCacheImpl) buildMapForMultiPut(dataMap map[string]interface{}) (map[string]interface{}, []string, []string) {
	prefixedDataMap := map[string]interface{}{}
	var allKeys []string
	var internalKeysToStore []string
	for key, value := range dataMap {
		internalKeyToStore := r.buildKeyName(key)
		prefixedDataMap[internalKeyToStore] = value
		allKeys = append(allKeys, key)
		internalKeysToStore = append(internalKeysToStore, internalKeyToStore)
	}
	return prefixedDataMap, allKeys, internalKeysToStore
}

func (r *universalRedisCacheImpl) Ping(ctx context.Context) (string, error) {
	return r.redisClient.Ping(ctx).Result()
}

func NewRedisCacheV1(cf gox.CrossFunction, config *goxCache.Config) (goxCache.Cache, error) {
	if config.Properties == nil {
		config.Properties = gox.StringObjectMap{}
	}

	// Set prefix key
	prefix := ""
	if !util.IsStringEmpty(config.Prefix) {
		prefix = config.Prefix + "_" + config.Properties.StringOrDefault("prefix", "default")
	} else {
		prefix = config.Properties.StringOrDefault("prefix", "default")
	}

	c := &universalRedisCacheImpl{
		CrossFunction:      cf,
		config:             config,
		closeDoOnce:        sync.Once{},
		logger:             cf.Logger().Named("cache.redis").Named(config.Name).Named(prefix),
		pubSubTopicName:    fmt.Sprintf("%s_%s_%s", prefix, config.Name, "pub_sub_topic"),
		prefix:             prefix,
		putCounter:         cf.Metric().Counter(prefix + "_put"),
		putCounterError:    cf.Metric().Counter(prefix + "_put_error"),
		putTimer:           cf.Metric().Timer(prefix + "_put_time"),
		getCounter:         cf.Metric().Counter(prefix + "_get"),
		getCounterError:    cf.Metric().Counter(prefix + "_get_error"),
		deleteCounter:      cf.Metric().Counter(prefix + "_delete"),
		deleteCounterError: cf.Metric().Counter(prefix + "_delete_error"),
		getTimer:           cf.Metric().Timer(prefix + "_get_time"),
		putTimeoutMs:       config.Properties.IntOrDefault("put_timeout_ms", 10),
		getTimeoutMs:       config.Properties.IntOrDefault("get_timeout_ms", 10),
	}

	addresses := make([]string, 0)
	addresses = append(addresses, config.Endpoint)
	if config.Clustered {
		addresses = append(addresses, config.Endpoint)
	}

	c.redisClient = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        addresses,
		Username:     config.Properties.StringOrEmpty("user"),
		Password:     config.Properties.StringOrEmpty("password"),
		ReadTimeout:  time.Duration(config.Properties.IntOrDefault("read_timeout", 100)) * time.Millisecond,
		WriteTimeout: time.Duration(config.Properties.IntOrDefault("write_timeout", 100)) * time.Millisecond,
	})

	return c, nil
}
