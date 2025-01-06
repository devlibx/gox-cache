package redisCache

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	"github.com/devlibx/gox-base/v2/test"
	goxCache "github.com/devlibx/gox-cache/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"os"
	"testing"
	"time"
)

var endpoint = "localhost:6379"
var clustered = false
var tlsVal = false

func TestRedisCache(t *testing.T) {
	if os.Getenv("REDIS_HOST") != "" {
		endpoint = os.Getenv("REDIS_HOST")
		clustered = true
		tlsVal = true
	}

	defer goleak.VerifyNone(t)
	id := uuid.NewString()
	cf, _ := test.MockCf(t)
	c, err := NewRedisCache(cf, &goxCache.Config{
		Name:       "dummy",
		Type:       "redis",
		Endpoint:   endpoint,
		Clustered:  clustered,
		TlsEnabled: tlsVal,
		Properties: map[string]interface{}{
			"prefix":         "TestRedisCache_" + id,
			"put_timeout_ms": 1000,
			"get_timeout_ms": 1000,
			"read_timeout":   1000,
			"write_timeout":  1000,
			"password":       os.Getenv("REDIS_PASSWORD"),
		},
	})
	assert.NoError(t, err)
	defer c.Close()

	ctx, cn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cn()

	result, err := c.IsRunning(ctx)
	if err != nil {
		t.Skip("redis is not running, skip this test: result=", result)
		return
	}
	fmt.Println("redis is running: result", result)

	_, err = c.Put(ctx, id, "value_"+id, 0)
	assert.NoError(t, err)

	valueOfKey, actualKeyInRedis, err := c.Get(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, []byte("value_"+id), valueOfKey)
	fmt.Println("value=", serialization.StringifySuppressError(valueOfKey, "na"), "actualKeyInRedis=", actualKeyInRedis)

	err = c.Delete(ctx, id)
	assert.NoError(t, err)
	valueOfKey, _, err = c.Get(ctx, id)
	assert.Error(t, err)
}

func TestRedisCache_Ttl(t *testing.T) {
	defer goleak.VerifyNone(t)
	id := uuid.NewString()
	cf, _ := test.MockCf(t)
	c, err := NewRedisCache(cf, &goxCache.Config{
		Name:       "dummy",
		Type:       "redis",
		Endpoint:   endpoint,
		Clustered:  clustered,
		Properties: map[string]interface{}{"prefix": "TestRedisCache_Ttl_" + id, "put_timeout_ms": 1000, "get_timeout_ms": 1000},
	})
	assert.NoError(t, err)
	defer c.Close()

	ctx, cn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cn()

	result, err := c.IsRunning(ctx)
	if err != nil {
		t.Skip("redis is not running, skip this test: result=", result)
		return
	}
	fmt.Println("redis is running: result", result)

	_, err = c.Put(ctx, id, "value_"+id, 1)
	assert.NoError(t, err)

	var notFoundError error
	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Second)
		_, _, notFoundError = c.Get(ctx, id)
		if notFoundError != nil {
			fmt.Println("Got not found error in index=", i)
			break
		}
	}
	assert.Error(t, notFoundError, "we must get a key not found error")

}

func TestRedisCache_PubSub(t *testing.T) {
	defer goleak.VerifyNone(t)

	id := uuid.NewString()
	cf, _ := test.MockCf(t)
	c, err := NewRedisCache(cf, &goxCache.Config{
		Name:       "dummy",
		Type:       "redis",
		Endpoint:   endpoint,
		Clustered:  clustered,
		Properties: map[string]interface{}{"prefix": "TestRedisCache_" + id, "put_timeout_ms": 1000, "get_timeout_ms": 1000},
	})
	assert.NoError(t, err)
	defer c.Close()

	ctx, cn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cn()

	result, err := c.IsRunning(ctx)
	if err != nil {
		t.Skip("redis is not running, skip this test: result=", result)
		return
	}
	fmt.Println("redis is running: result", result)

	gotMessage := false
	ctx1, cn1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cn1()
	err = c.Subscribe(ctx, func(data gox.StringObjectMap) error {
		if id == data.StringOrEmpty("data") {
			fmt.Printf("TestRedisCache_PubSub - got message in redis pubSub: message=%v \n", data)
			gotMessage = true
			cn1()
		}
		return nil
	})
	assert.NoError(t, err)

	data, err := c.Publish(ctx, gox.StringObjectMap{"data": id})
	fmt.Println(data)
	assert.NoError(t, err)

	<-ctx1.Done()
	assert.True(t, gotMessage)

}

func BenchmarkPutGet(t *testing.B) {
	// defer goleak.VerifyNone(t)
	id := uuid.NewString()
	cf, _ := test.MockCf(t)
	c, err := NewRedisCache(cf, &goxCache.Config{
		Name:       "dummy",
		Type:       "redis",
		Endpoint:   endpoint,
		Clustered:  clustered,
		Properties: map[string]interface{}{"prefix": "TestRedisCache_" + id, "put_timeout_ms": 1000, "get_timeout_ms": 1000},
	})
	assert.NoError(t, err)
	defer c.Close()

	ctx, cn := context.WithTimeout(context.Background(), 100*time.Second)
	defer cn()

	result, err := c.IsRunning(ctx)
	if err != nil {
		t.Skip("redis is not running, skip this test: result=", result)
		return
	}
	fmt.Println("redis is running: result", result)

	for i := 0; i < t.N; i++ {
		_, err = c.Put(ctx, id, "value_"+id, 0)
		assert.NoError(t, err)

		valueOfKey, _, err := c.Get(ctx, id)
		assert.NoError(t, err)
		assert.Equal(t, []byte("value_"+id), valueOfKey)
	}
}
