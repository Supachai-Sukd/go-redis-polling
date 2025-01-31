package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
)

 
const (
	JobQueue = "polling_queue"
	JobQueueRecovery = "polling_queue_recovery"
)

const (
	REPEAT_USER_TRANSFER_TIME  = 10
)

type PollingTask struct {
	ID            string `json:"id"`
	TransactionID string `json:"transaction_id"`
}

func NewRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // ไม่มีรหัสผ่าน
		DB:       0,  // ใช้ DB 0
	})
}

// EnqueueTask ฟังก์ชันเพิ่ม task เข้า queue
func EnqueueTask(client *redis.Client, payload PollingTask) error {
	ctx := context.Background()

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	err = client.LPush(ctx, JobQueue, data).Err()
	if err != nil {
		return err
	}

	err = client.Set(ctx, "pollingQueue:repeat:"+payload.ID, data, time.Duration(REPEAT_USER_TRANSFER_TIME)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}