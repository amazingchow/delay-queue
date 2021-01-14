package redis

import (
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/FZambia/sentinel"
	"github.com/gomodule/redigo/redis"

	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
)

type RedisConnPoolSingleton struct {
	db int
	p  *redis.Pool
}

var (
	instance  *RedisConnPoolSingleton
	onceOpen  sync.Once
	onceClose sync.Once
)

func GetOrCreateInstance(cfg *conf.RedisService) *RedisConnPoolSingleton {
	onceOpen.Do(func() {
		sntnl := &sentinel.Sentinel{
			Addrs:      cfg.SentinelEndpoints,
			MasterName: cfg.SentinelMasterName,
			Dial: func(addr string) (redis.Conn, error) {
				conn, err := redis.Dial(
					"tcp",
					addr,
					redis.DialConnectTimeout(time.Duration(cfg.RedisConnectTimeoutMsec)*time.Millisecond),
					redis.DialReadTimeout(time.Duration(cfg.RedisReadTimeoutMsec)*time.Millisecond),
					redis.DialWriteTimeout(time.Duration(cfg.RedisWriteTimeoutMsec)*time.Millisecond),
					redis.DialPassword(cfg.SentinelPassword),
				)
				if err != nil {
					return nil, err
				}
				return conn, nil
			},
		}
		instance = &RedisConnPoolSingleton{}
		instance.db = cfg.RedisDatabase
		instance.p = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				master, err := sntnl.MasterAddr()
				if err != nil {
					return nil, err
				}
				conn, err := redis.Dial(
					"tcp",
					master,
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
			MaxIdle:     cfg.RedisPoolMaxIdleConns,
			MaxActive:   cfg.RedisPoolMaxActiveConns,
			IdleTimeout: time.Duration(cfg.RedisPoolIdleConnTimeoutSec) * time.Second,
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

// ExecCommand 执行redis命令, 完成后自动归还连接.
func ExecCommand(inst *RedisConnPoolSingleton, debug bool, cmd string, args ...interface{}) (interface{}, error) {
	conn := getConn(inst, debug)
	defer conn.Close()
	return conn.Do(cmd, args...)
}

func getConn(inst *RedisConnPoolSingleton, debug bool) redis.Conn {
	conn := inst.p.Get()
	if debug {
		conn = redis.NewLoggingConn(conn, log.New(os.Stderr, "", 0), "redigo")
	}
	conn.Do("SELECT", inst.db) // nolint
	return conn
}
