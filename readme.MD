# redis-lock sample

演示如何优雅的通过同一配置，同时使用 redis客户端 和 redis分布式锁

* redis-client 库使用 github.com/redis/go-redis/v9 v9.0.5
* redis-lock 库使用 github.com/bsm/redislock v0.9.3
* 兼容 github.com/gogf/gf/v2 v2.4.x 的 redis 配置和库（目前使用 go-redis/v8）