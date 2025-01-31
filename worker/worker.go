package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"go_polling_jobs/queue"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

// Transaction โครงสร้างข้อมูลของตาราง transactions
type Transaction struct {
	ID            uint   `gorm:"primaryKey"`
	TransactionID string `gorm:"column:transaction_id"`
	PollingStatus string `gorm:"column:polling_status"`
}

// PollingWorker โครงสร้าง Worker
type PollingWorker struct {
	DB    *gorm.DB
	Redis *redis.Client
}

const (
	RECOVERY_USER_TRANSFER_TIME = 10
)

// StartWorker เริ่มต้น worker
func StartWorker(db *gorm.DB, redisClient *redis.Client) {
	worker := &PollingWorker{
		DB:    db,
		Redis: redisClient,
	}

	log.Println("Starting worker...")
	for {
		worker.ProcessTask()
		time.Sleep(1 * time.Second) // หน่วงเวลา 1 วินาที
	}
}

// ProcessTask ฟังก์ชันประมวลผล task
func (w *PollingWorker) ProcessTask() {
	ctx := context.Background()

	// ดึง task ออกจาก queue
	result, err := w.Redis.BRPop(ctx, 0, queue.JobQueue).Result()
	if err != nil {
		log.Printf("Failed to pop task from queue: %v", err)
		return
	}

	// แปลง JSON เป็น struct
	var payload queue.PollingTask
	if err := json.Unmarshal([]byte(result[1]), &payload); err != nil {
		log.Printf("Failed to parse task payload: %v", err)
		return
	}

	log.Printf("Processing task: ID=%s, TransactionID=%s\n", payload.ID, payload.TransactionID)


	// สมมุติว่า task ล้มเหลว
	if err := w.handleTaskFailure(ctx, payload); err != nil {
		log.Printf("Task failed: %v", err)
		return
	}

	// สร้าง transaction ใหม่
	newTransaction := Transaction{
		TransactionID: payload.TransactionID,
		PollingStatus: "ProcessorCheckUserTransfer",
	}

	// บันทึกลงฐานข้อมูล
	if err := w.DB.Create(&newTransaction).Error; err != nil {
		log.Printf("Failed to insert new transaction: %v", err)
		return
	}

	log.Printf("Transaction created successfully: ID=%s", payload.TransactionID)
}


// handleTaskFailure จัดการเมื่อ task ล้มเหลว
func (w *PollingWorker) handleTaskFailure(ctx context.Context, payload queue.PollingTask) error {
	// สมมุติว่า task ล้มเหลว จริงๆลบทิ้งเลยและส่ง recovery ข้างล่างได้เลย
	if payload.TransactionID == "fail_task" {
		log.Print("Condition task failed")
		// ลบ task ออกจาก queue ปัจจุบัน
		if err := w.removeTaskFromQueue(ctx, payload); err != nil {
			return err
		}
		

		if err := w.moveTaskToRecoveryQueue(ctx, payload); err != nil {
			return err
		}

		return errors.New("task failed and moved to failed queue")
	}

	return nil
}

// removeTaskFromQueue ลบ task ออกจาก queue ปัจจุบัน
func (w *PollingWorker) removeTaskFromQueue(ctx context.Context, payload queue.PollingTask) error {
	// แปลง payload เป็น JSON
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// ลบ task ออกจาก queue ปัจจุบัน
	if err := w.Redis.LRem(ctx, queue.JobQueue, 0, data).Err(); err != nil {
		return err
	}

	log.Printf("Task removed from queue: ID=%s", payload.ID)
	return nil
}


 
// moveTaskToRecoveryQueue ย้าย task ไปยัง queue อื่น (polling_failed_queue)
func (w *PollingWorker) moveTaskToRecoveryQueue(ctx context.Context, payload queue.PollingTask) error {
	// แปลง payload เป็น JSON
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	if err := w.Redis.LPush(ctx, queue.JobQueueRecovery, data).Err(); err != nil {
		return err
	}

	err = w.Redis.Set(ctx, "pollingQueue:repeat:"+payload.ID, data, time.Duration(RECOVERY_USER_TRANSFER_TIME)*time.Second).Err()
	if err != nil {
		return err
	}

	log.Printf("Task moved to failed queue: ID=%s", payload.ID)
	return nil
}



