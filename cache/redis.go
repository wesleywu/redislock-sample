package cache

import (
	"context"
	"os"
	"time"

	"github.com/bsm/redislock"
	_ "github.com/gogf/gf/contrib/nosql/redis/v2"
	"github.com/gogf/gf/v2/database/gredis"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gutil"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

const (
	ConfigNodeNameRedis    = "redis"
	defaultPoolMinIdle     = 5
	defaultPoolMaxIdle     = 10
	defaultPoolMaxActive   = 100
	defaultPoolIdleTimeout = 10 * time.Second
	defaultPoolWaitTimeout = 10 * time.Second
	defaultPoolMaxLifeTime = 600 * time.Second
	defaultMaxRetries      = -1
)

type RedisCacheOpt struct {
	ConfigName     string // key for [redis] in config, default value is "default"
	WithLock       bool   // whether returns a redis-lock bound with the client, default true
	TracingEnabled bool   // whether enable otel tracing, default true
}

type RedisCache struct {
	Client redis.UniversalClient
	Locker *redislock.Client
}

var (
	defaultOps = RedisCacheOpt{
		ConfigName:     "default",
		WithLock:       true,
		TracingEnabled: true,
	}
)

func NewRedisCache(ctx context.Context, opt ...RedisCacheOpt) (*RedisCache, error) {
	var (
		cacheOpt    RedisCacheOpt
		redisClient redis.UniversalClient
		redisLocker *redislock.Client
	)
	if len(opt) > 0 {
		cacheOpt = opt[0]
	} else {
		cacheOpt = defaultOps
	}
	config, err := getGfRedisConfig(ctx, cacheOpt.ConfigName)
	if err != nil {
		g.Log().Warningf(ctx, "%+v", err)
		config, _ = gredis.ConfigFromMap(g.Map{
			"Address": "127.0.0.1:6379",
		})
	}
	fillWithDefaultConfiguration(config)
	universalOpts := &redis.UniversalOptions{
		Addrs:           gstr.SplitAndTrim(config.Address, ","),
		Password:        config.Pass,
		DB:              config.Db,
		MaxRetries:      defaultMaxRetries,
		PoolSize:        config.MaxActive,
		MinIdleConns:    config.MinIdle,
		MaxIdleConns:    config.MaxIdle,
		ConnMaxLifetime: config.MaxConnLifetime,
		ConnMaxIdleTime: config.IdleTimeout,
		PoolTimeout:     config.WaitTimeout,
		DialTimeout:     config.DialTimeout,
		ReadTimeout:     config.ReadTimeout,
		WriteTimeout:    config.WriteTimeout,
		MasterName:      config.MasterName,
		TLSConfig:       config.TLSConfig,
	}

	if universalOpts.MasterName != "" {
		redisSentinel := universalOpts.Failover()
		redisSentinel.ReplicaOnly = config.SlaveOnly
		redisClient = redis.NewFailoverClient(redisSentinel)
	} else if len(universalOpts.Addrs) > 1 {
		redisClient = redis.NewClusterClient(universalOpts.Cluster())
	} else {
		redisClient = redis.NewClient(universalOpts.Simple())
	}
	if redisClient == nil {
		return nil, gerror.Newf("failed to initialize redis client by config: %+v", universalOpts)
	}
	if cacheOpt.TracingEnabled {
		if err := redisotel.InstrumentTracing(redisClient); err != nil {
			g.Log().Errorf(ctx, "failed to trace redis via otel: %+v", err)
		}
	}

	info := redisClient.Info(ctx)
	if info.Err() != nil {
		return nil, info.Err()
	}

	if cacheOpt.WithLock {
		redisLocker = redislock.New(redisClient)
	}

	g.Log().Info(ctx, "redis cache provider initialized")

	return &RedisCache{
		Client: redisClient,
		Locker: redisLocker,
	}, nil
}

func fillWithDefaultConfiguration(config *gredis.Config) {
	if config.MinIdle == 0 {
		config.MinIdle = defaultPoolMinIdle
	}
	// The MaxIdle is the most important attribute of the connection pool.
	// Only if this attribute is set, the created connections from client
	// can not exceed the limit of the server.
	if config.MaxIdle == 0 {
		config.MaxIdle = defaultPoolMaxIdle
	}
	// This value SHOULD NOT exceed the connection limit of redis server.
	if config.MaxActive == 0 {
		config.MaxActive = defaultPoolMaxActive
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = defaultPoolIdleTimeout
	}
	if config.WaitTimeout == 0 {
		config.WaitTimeout = defaultPoolWaitTimeout
	}
	if config.MaxConnLifetime == 0 {
		config.MaxConnLifetime = defaultPoolMaxLifeTime
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = -1
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = -1
	}
}

func getGfRedisConfig(ctx context.Context, group string) (*gredis.Config, error) {
	var err error
	if !g.Config().Available(ctx) {
		wd, _ := os.Getwd()
		return nil, gerror.Newf("no config available at current working directory %s", wd)
	}
	var (
		configMap   map[string]interface{}
		redisConfig *gredis.Config
	)
	if configMap, err = g.Config().Data(ctx); err != nil {
		g.Log().Errorf(ctx, `retrieve config data map failed: %+v`, err)
	}
	if _, v := gutil.MapPossibleItemByKey(configMap, ConfigNodeNameRedis); v != nil {
		configMap = gconv.Map(v)
	}
	if len(configMap) > 0 {
		if v, ok := configMap[group]; ok {
			redisConfig, err = gredis.ConfigFromMap(gconv.Map(v))
			if err != nil {
				return nil, err
			}
			return redisConfig, nil
		}
	}
	return redisConfig, nil
}
