package inmemoryCache

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base/v2/test"
	goxCache "github.com/devlibx/gox-cache/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"testing"
	"time"
)

func TestInMemoryCache(t *testing.T) {
	defer goleak.VerifyNone(t)
	id := uuid.NewString()
	cf, _ := test.MockCf(t)
	c, err := NewInMemoryCache(cf, &goxCache.Config{
		Name: "dummy",
		Type: "inmemory",
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

	valueOfKey, _, err := c.Get(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, "value_"+id, valueOfKey.(string))

	err = c.Delete(ctx, id)
	assert.NoError(t, err)
	valueOfKey, _, err = c.Get(ctx, id)
	assert.Error(t, err)
}

func TestRedisCache_Ttl(t *testing.T) {
	defer goleak.VerifyNone(t)
	id := uuid.NewString()
	cf, _ := test.MockCf(t)
	c, err := NewInMemoryCache(cf, &goxCache.Config{
		Name: "dummy",
		Type: "inmemory",
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

func BenchmarkPutGet(t *testing.B) {
	// defer goleak.VerifyNone(t)
	id := uuid.NewString()
	cf, _ := test.MockCf(t)
	c, err := NewInMemoryCache(cf, &goxCache.Config{
		Name: "dummy",
		Type: "inmemory",
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
