package delayqueue

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

func TestReadyQueueCURD(t *testing.T) {
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

	dq := &DelayQueue{
		redisCli: redis.GetOrCreateInstance(fakeRedisCfg),
	}
	fakeTopic := "inventory_service_line"
	fakeReadyQueue := fmt.Sprintf(DefaultReadyQueueNameFormatter, fakeTopic)
	// 先清理环境
	_, err := dq.redisCli.ExecCommand("DEL", fakeReadyQueue)
	assert.Empty(t, err)

	fakeTaskIds := []string{
		"ff74da2f-20c1-45c4-9570-a01b192f1c9d",
		"3fa03ec8-9d75-4002-914b-e7b79f25c323",
		"f58d644e-a2fc-44ce-851c-7530390cfce",
	}
	for _, taskId := range fakeTaskIds {
		err = dq.pushToReadyQueue(fakeTopic, taskId)
		assert.Empty(t, err)
	}

	next := 0
	for {
		taskId, err := dq.blockPopFromReadyQueue([]string{fakeTopic}, 1)
		assert.Empty(t, err)
		if taskId == "" {
			break
		}
		assert.Equal(t, fakeTaskIds[next], taskId)
		next++
	}
	assert.Equal(t, 3, next)

	redis.ReleaseInstance()
}
