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
	REPEAT_USER_TRANSFER_LIMIT = 3
)

// PollingTask โครงสร้างข้อมูลของ Task
type PollingTask struct {
	ID            string `json:"id"`
	TransactionID string `json:"transaction_id"`
}

// NewRedisClient สร้าง Redis client
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

	// แปลง payload เป็น JSON
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// เพิ่ม task เข้า queue
	err = client.LPush(ctx, JobQueue, data).Err()
	if err != nil {
		return err
	}

	// ตั้งค่าให้ task ทำงานซ้ำทุกๆ REPEAT_USER_TRANSFER_TIME วินาที
	err = client.Set(ctx, "pollingQueue:repeat:"+payload.ID, data, time.Duration(REPEAT_USER_TRANSFER_TIME)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}