package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"go_polling_jobs/queue"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

type Transaction struct {
	ID            uint   `gorm:"primaryKey"`
	TransactionID string `gorm:"column:transaction_id"`
	PollingStatus string `gorm:"column:polling_status"`
}

type PollingWorker struct {
	DB    *gorm.DB
	Redis *redis.Client
}

const (
	RECOVERY_USER_TRANSFER_TIME = 10
)

func StartWorker(db *gorm.DB, redisClient *redis.Client) {
	worker := &PollingWorker{
		DB:    db,
		Redis: redisClient,
	}

	log.Println("Starting worker...")
	for {
		worker.ProcessTask()
		time.Sleep(1 * time.Second) // delay 1 วินาที
	}
}

// ProcessTask for process task
func (w *PollingWorker) ProcessTask() {
	ctx := context.Background()

	// get task from queue
	result, err := w.Redis.BRPop(ctx, 0, queue.JobQueue).Result()
	if err != nil {
		log.Printf("Failed to pop task from queue: %v", err)
		return
	}

	var payload queue.PollingTask
	if err := json.Unmarshal([]byte(result[1]), &payload); err != nil {
		log.Printf("Failed to parse task payload: %v", err)
		// example for handle task failed
		if err := w.handleTaskFailure(ctx, payload); err != nil {
			log.Printf("Task failed: %v", err)
			return
		}
		return
	}

	log.Printf("Processing task: ID=%s, TransactionID=%s\n", payload.ID, payload.TransactionID)

	newTransaction := Transaction{
		TransactionID: payload.TransactionID,
		PollingStatus: "ProcessorCheckUserTransfer",
	}

	if err := w.DB.Create(&newTransaction).Error; err != nil {
		log.Printf("Failed to insert new transaction: %v", err)
		return
	}

	log.Printf("Transaction created successfully: ID=%s", payload.TransactionID)
}

func (w *PollingWorker) handleTaskFailure(ctx context.Context, payload queue.PollingTask) error {
	log.Print("Condition task failed")
	// delete current task and then push to recovery queue with moveTaskToRecoveryQueue
	if err := w.removeTaskFromQueue(ctx, payload); err != nil {
		return err
	}

	if err := w.moveTaskToRecoveryQueue(ctx, payload); err != nil {
		return err
	}

	return nil

}

func (w *PollingWorker) removeTaskFromQueue(ctx context.Context, payload queue.PollingTask) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	if err := w.Redis.LRem(ctx, queue.JobQueue, 0, data).Err(); err != nil {
		return err
	}

	log.Printf("Task removed from queue: ID=%s", payload.ID)
	return nil
}

func (w *PollingWorker) moveTaskToRecoveryQueue(ctx context.Context, payload queue.PollingTask) error {
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
