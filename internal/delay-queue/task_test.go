package delayqueue

import (
	"testing"

	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
	"github.com/stretchr/testify/assert"
)

func TestTaskCURD(t *testing.T) {
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

	fakeTasks := []*Task{
		&Task{
			Topic: "shopping_cart_service_line",
			Id:    "ff74da2f-20c1-45c4-9570-a01b192f1c9d",
			Delay: 120,
			TTR:   180,
		},
		&Task{
			Topic: "order_service_line",
			Id:    "3fa03ec8-9d75-4002-914b-e7b79f25c323",
			Delay: 120,
			TTR:   180,
		},
		&Task{
			Topic: "inventory_service_line",
			Id:    "f58d644e-a2fc-44ce-851c-7530390cfcea",
			Delay: 120,
			TTR:   180,
		},
	}
	for _, task := range fakeTasks {
		err := dq.putTask(task.Id, task)
		assert.Empty(t, err)
	}
	for _, task := range fakeTasks {
		retTask, err := dq.getTask(task.Id)
		assert.Empty(t, err)
		assert.Equal(t, task.Topic, retTask.Topic)

		err = dq.delTask(task.Id)
		assert.Empty(t, err)

		retTask, err = dq.getTask(task.Id)
		assert.Empty(t, err)
		assert.Empty(t, retTask)
	}

	redis.ReleaseInstance()
}