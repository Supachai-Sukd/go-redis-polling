package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

// ใช้ task type เดียวกับที่ประกาศใน queue package
const TypeEmailDelivery = "email:deliver"

// redisOpt สำหรับเชื่อมต่อ Redis
var redisOpt = asynq.RedisClientOpt{
	Addr: "127.0.0.1:6379",
}

// EmailPayload คือข้อมูลสำหรับส่งอีเมล
type EmailPayload struct {
	Email   string `json:"email"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// JobPayload คือ payload ของ task ที่จะ process
type JobPayload struct {
	EmailPayload EmailPayload `json:"email"`
	Duration     int          `json:"duration"`
}

// handleEmailDeliveryTask เป็น handler สำหรับ process task
func handleEmailDeliveryTask(ctx context.Context, t *asynq.Task) error {
	var payload JobPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("cannot unmarshal payload: %v", err)
	}

	log.Printf("Processing task: Type=%s, Payload=%+v", t.Type(), payload)

	// จำลองการประมวลผล task ด้วยการ sleep ตามระยะเวลาที่ระบุ
	time.Sleep(time.Duration(payload.Duration) * time.Second)

	log.Println("Task processed successfully")
	return nil
}

// RunWorker เริ่ม worker เพื่อ process task จาก queue
func RunWorker() {
	srv := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"default": 1,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(TypeEmailDelivery, handleEmailDeliveryTask)

	log.Println("Worker started")
	if err := srv.Run(mux); err != nil {
		log.Fatalf("Could not run worker: %v", err)
	}
}