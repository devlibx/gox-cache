### How to use

Refer to ```cache/api_impl_test.go``` for detailed usage

##### Define cache config in Yaml

```yaml
# Default Put/Get timeout = 10ms
cacheConfig:
  enabled: true
  caches:
    testRedis:
      type: redis
      endpoint: localhost:6379
      enabled: true
      put_timeout_ms: 10
      get_timeout_ms: 10
```

##### Initialize cache registry

type dummyConfig struct { Configuration goxCache.Configuration `yaml:"cacheConfig"`
}

func TestRegistry(t *testing.T) { id := uuid.NewString()
cf, _ := test.MockCf(t)

	// Read config from YAML file
	conf := dummyConfig{}
	err := serialization.ReadYamlFromString(dummyConfigString, &conf)
	assert.NoError(t, err)

	registry, err := NewRegistry(context.TODO(), cf, conf.Configuration)
	assert.NoError(t, err)
	redisCacheObject, err := registry.GetCache("testRedis")

	// Context to timeout if it takes lot of time
	ctx, cn := context.WithTimeout(context.Background(), time.Second)
	defer cn()

	// You can check if cache is running e.g. redis is running
	result, err := redisCacheObject.IsRunning(ctx)
	if err != nil {
		t.Skip("redis is not running, skip this test: result=", result)
		return
	}
	fmt.Println("redis is running: result", result)

	// Put data in cache - TTL=0 means never expire
	err = redisCacheObject.Put(ctx, id, "value_"+id, 0)
	assert.NoError(t, err)

	// Get data
	valueOfKey, err := redisCacheObject.Get(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, []byte("value_"+id), valueOfKey)

}

```