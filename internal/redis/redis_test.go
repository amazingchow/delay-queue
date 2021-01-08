package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
)

func TestRedisClientCURD(t *testing.T) {
	fakeRedisCfg := &conf.Redis{
		SentinelEndpoints:   []string{"localhost:26379", "localhost:26380", "localhost:26381"},
		SentinelMasterName:  "mymaster",
		SentinelPassword:    "Pwd123!@",
		RedisMasterPassword: "sOmE_sEcUrE_pAsS",
		RedisPoolMaxIdle:    3,
		RedisPoolMaxActive:  64,
		RedisConnectTimeout: 500,
		RedisReadTimeout:    500,
		RedisWriteTimeout:   500,
	}

	inst := GetOrCreateInstance(fakeRedisCfg)

	_, err := inst.ExecCommand("EXISTS", "foo")
	assert.Empty(t, err)
	_, err = inst.ExecCommand("SET", "msg:hello", "Hello Redis!!!")
	assert.Empty(t, err)
	ret, err := inst.ExecCommand("GET", "msg:hello")
	assert.Empty(t, err)
	assert.Equal(t, "Hello Redis!!!", string(ret.([]byte)))

	ReleaseInstance()
}
