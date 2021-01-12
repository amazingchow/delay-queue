package delayqueue

import (
	"testing"

	"github.com/stretchr/testify/assert"

	conf "github.com/amazingchow/photon-dance-delay-queue/internal/config"
	"github.com/amazingchow/photon-dance-delay-queue/internal/redis"
)

func TestTaskCURD(t *testing.T) {
	fakeRedisCfg := &conf.RedisService{
		SentinelEndpoints:       []string{"localhost:26379", "localhost:26380", "localhost:26381"},
		SentinelMasterName:      "mymaster",
		SentinelPassword:        "Pwd123!@",
		RedisMasterPassword:     "sOmE_sEcUrE_pAsS",
		RedisPoolMaxIdleConns:   3,
		RedisPoolMaxActiveConns: 64,
		RedisConnectTimeoutMsec: 500,
		RedisReadTimeoutMsec:    500,
		RedisWriteTimeoutMsec:   500,
	}
	redisCli := redis.GetOrCreateInstance(fakeRedisCfg)
	ctrl := NewTaskRWController()

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
		// 先清理环境
		_, err := redis.ExecCommand(redisCli, false, "DEL", task.Id)
		assert.Empty(t, err)

		err = ctrl.PutTask(redisCli, task.Id, task, true)
		assert.Empty(t, err)
	}
	for _, task := range fakeTasks {
		retTask, err := ctrl.GetTask(redisCli, task.Id, true)
		assert.Empty(t, err)
		assert.Equal(t, task.Topic, retTask.Topic)

		err = ctrl.DelTask(redisCli, task.Id, true)
		assert.Empty(t, err)

		retTask, err = ctrl.GetTask(redisCli, task.Id, true)
		assert.Empty(t, err)
		assert.Empty(t, retTask)
	}

	// 退出之前, 再次清理环境
	for _, task := range fakeTasks {
		_, err := redis.ExecCommand(redisCli, false, "DEL", task.Id)
		assert.Empty(t, err)
	}

	redis.ReleaseInstance()
}
