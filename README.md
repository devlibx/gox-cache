### How to use

Refer to ```cache/api_impl_test.go``` for detailed usage

##### Define cache config in Yaml

```yaml
# Default Put/Get timeout = 10ms
cacheConfig:
  caches:
    testRedis:
      type: redis
      endpoint: localhost:6379
      clustered: false
      properties:
        put_timeout_ms: 10
        get_timeout_ms: 10
```
<b>Put clustered=true if you are using clustered redis

Other redis properties:

```yaml
properties:
  put_timeout_ms: 10
  get_timeout_ms: 10
  db: 0
  password: your_password
  prefix: some_prefix       // all keys are prefixed with this string to avoid key colission 
```

##### Initialize cache registry

```go
type dummyConfig struct { 
	Configuration goxCache.Configuration `yaml:"cacheConfig"`
}

func TestRegistry(t *testing.T) { id := uuid.NewString()
    cf, _ := test.MockCf(t)

	// Read config from YAML file
	conf := dummyConfig{}
	err := serialization.ReadYamlFromString(dummyConfigString, &conf)
	assert.NoError(t, err)

	registry, err := NewRegistry(context.TODO(), cf, conf.Configuration)
	assert.NoError(t, err)
	
	// Get cache by name 
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
	_, err = redisCacheObject.Put(ctx, id, "value_"+id, 0)
	assert.NoError(t, err)

	// Get data
	valueOfKey, _, err := redisCacheObject.Get(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, []byte("value_"+id), valueOfKey)
}
```