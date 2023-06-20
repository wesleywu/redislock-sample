package main

import (
	"context"
	"sync"
	"time"

	"github.com/WesleyWu/redislock-sample/cache"
	"github.com/bsm/redislock"
	"github.com/gogf/gf/v2/database/gredis"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/redis/go-redis/v9"
)

const (
	redisConfigName = "default"
	lockTimeOut     = 10 * time.Second
)

func main() {
	ctx := gctx.New()
	redisCache, err := cache.NewRedisCache(ctx)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup

	g.Log().Info(ctx, "=======================================================")
	g.Log().Info(ctx, "Now demonstrating use various lock for each go routines")
	g.Log().Info(ctx, "=======================================================")
	for index := 0; index < 100; index++ {
		wg.Add(1)
		itemKey := "item" + gconv.String(index)
		go func(ctx context.Context, redisCache *cache.RedisCache, itemKey string) {
			defer wg.Done()
			err1 := processItem(ctx, redisCache, itemKey, false)
			if err1 != nil {
				g.Log().Errorf(ctx, "%+v", err1)
			}
		}(ctx, redisCache, itemKey)
	}
	wg.Wait()

	g.Log().Info(ctx, "===================================================")
	g.Log().Info(ctx, "Now demonstrating use same lock for all go routines")
	g.Log().Info(ctx, "===================================================")
	for index := 0; index < 100; index++ {
		wg.Add(1)
		itemKey := "item" + gconv.String(index)
		go func(ctx context.Context, redisCache *cache.RedisCache, itemKey string) {
			defer wg.Done()
			err1 := processItem(ctx, redisCache, itemKey, true)
			if err1 != nil {
				g.Log().Errorf(ctx, "%+v", err1)
			}
		}(ctx, redisCache, itemKey)
	}
	wg.Wait()
}

func processItem(ctx context.Context, redisCache *cache.RedisCache, itemKey string, useSameLock bool) error {
	var (
		redisClient   redis.UniversalClient
		redisClientGf *gredis.Redis
		lockerClient  *redislock.Client
		lock          *redislock.Lock
		lockName      string
		lockObtained  = false
		err           error
	)
	redisClientGf = g.Redis(redisConfigName)
	if redisClientGf == nil {
		return gerror.New("无法初始化goframe redis client实例")
	}
	redisClient = redisCache.Client
	if redisClient == nil {
		return gerror.New("没有初始化redis client 实例")
	}
	lockerClient = redisCache.Locker
	if lockerClient == nil {
		return gerror.New("没有初始化redis locker实例")
	}

	// demonstrate locking
	if useSameLock {
		lockName = "LockXXX"
	} else {
		lockName = "Lock:" + itemKey
	}
	for !lockObtained {
		lock, err = lockerClient.Obtain(ctx, lockName, lockTimeOut, nil)
		if err == redislock.ErrNotObtained {
			time.Sleep(20 * time.Millisecond)
			continue
		} else if err != nil {
			g.Log().Errorf(ctx, "Error obtaining lock \"%s\".", lockName)
			return err
		}
		lockObtained = true
	}

	defer func(lock *redislock.Lock, ctx context.Context) {
		_ = lock.Release(ctx)
	}(lock, ctx)

	// simulating processing
	g.Log().Infof(ctx, "Processed itemKey %s", itemKey)
	redisClient = redisCache.Client
	if redisClient == nil {
		return gerror.New("没有初始化redis client 实例")
	}

	itemValue := "item value blah blah"
	// set value for given key using RedisCache.Client
	statusCmd := redisClient.Set(ctx, itemKey, itemValue, 3600*time.Second)
	if statusCmd.Err() != nil {
		return statusCmd.Err()
	}

	// retrieve value for given key using gredis.Redis
	itemValueVar, err := redisClientGf.Get(ctx, itemKey)
	if err != nil {
		return err
	}
	if itemValueVar.String() != itemValue {
		return gerror.Newf("key % should have been set value as %v, but was retrieved: %v", itemKey, itemValue, itemValueVar.String())
	}

	// delete key using gredis.Redis
	_, err = redisClientGf.Del(ctx, itemKey)
	if err != nil {
		return err
	}

	// retrieve value for given key using RedisCache.Client
	stringCmd := redisClient.Get(ctx, itemKey)
	err = stringCmd.Err()
	if err == redis.Nil {
		// expected error, do nothing
	} else if err != nil {
		return err
	} else {
		return gerror.Newf("key % should have been deleted, but value was retrieved: %v", itemKey, stringCmd.Val())
	}
	time.Sleep(100 * time.Millisecond)
	// end of processing

	return nil
}
