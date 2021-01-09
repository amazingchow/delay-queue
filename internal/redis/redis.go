package redis

import (
	"errors"
	"sync"
	"time"

	"github.com/FZambia/sentinel"
	"github.com/gomodule/redigo/redis"

	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
)

type RedisPoolSingleton struct {
	p *redis.Pool
}

var instance *RedisPoolSingleton
var onceOpen sync.Once
var onceClose sync.Once

func GetOrCreateInstance(cfg *conf.Redis) *RedisPoolSingleton {
	onceOpen.Do(func() {
		sntnl := &sentinel.Sentinel{
			Addrs:      cfg.SentinelEndpoints,
			MasterName: cfg.SentinelMasterName,
			Dial: func(addr string) (redis.Conn, error) {
				conn, err := redis.Dial(
					"tcp",
					addr,
					redis.DialConnectTimeout(time.Duration(cfg.RedisConnectTimeout)*time.Millisecond),
					redis.DialReadTimeout(time.Duration(cfg.RedisReadTimeout)*time.Millisecond),
					redis.DialWriteTimeout(time.Duration(cfg.RedisWriteTimeout)*time.Millisecond),
					redis.DialPassword(cfg.SentinelPassword),
				)
				if err != nil {
					return nil, err
				}
				return conn, nil
			},
		}
		instance = &RedisPoolSingleton{}
		instance.p = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				addr, err := sntnl.MasterAddr()
				if err != nil {
					return nil, err
				}
				conn, err := redis.Dial(
					"tcp",
					addr,
					redis.DialPassword(cfg.RedisMasterPassword),
				)
				if err != nil {
					return nil, err
				}
				return conn, nil
			},
			TestOnBorrow: func(conn redis.Conn, t time.Time) error {
				if !sentinel.TestRole(conn, "master") {
					return errors.New("Role check failed")
				} else {
					return nil
				}
			},
			MaxIdle:     cfg.RedisPoolMaxIdle,
			MaxActive:   cfg.RedisPoolMaxActive,
			IdleTimeout: 240 * time.Second,
			Wait:        true,
		}
	})
	return instance
}

func ReleaseInstance() {
	onceClose.Do(func() {
		if instance != nil {
			instance.p.Close()
		}
	})
}

// ExecCommand 执行redis命令, 执行完成后连接自动放回连接池.
func (inst *RedisPoolSingleton) ExecCommand(cmd string, args ...interface{}) (interface{}, error) {
	redis := inst.p.Get()
	defer redis.Close()
	return redis.Do(cmd, args...)
}
