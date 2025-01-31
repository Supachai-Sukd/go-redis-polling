package recovery




import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"go_polling_jobs/queue"
	"go_polling_jobs/worker"

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
	RECOVERY_LIMIT							= 3
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
	result, err := w.Redis.BRPop(ctx, 0, queue.JobQueueRecovery).Result()
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

	
	// ตรวจสอบ retry count
	retryCount, err := w.getRetryCount(ctx, payload.ID)
	if err != nil {
		log.Printf("Failed to get retry count: %v", err)
		return
	}

	// หาก retry เกินขีดจำกัด ให้หยุดประมวลผล
	if retryCount >= RECOVERY_LIMIT {
		log.Printf("Task reached retry limit: ID=%s, RetryCount=%d\n", payload.ID, retryCount)
		w.removeTaskFromQueue(ctx, payload)
		return
	}


	// สร้าง transaction ใหม่
	newTransaction := worker.Transaction{
		TransactionID: payload.TransactionID,
		PollingStatus: "ProcessorRecovery",
	}

	// บันทึกลงฐานข้อมูล
	if err := w.DB.Create(&newTransaction).Error; err != nil {
		log.Printf("Failed to insert new transaction: %v", err)
		return
	}

	// เพิ่ม retry count
	if err := w.incrementRetryCount(ctx, payload.ID); err != nil {
		log.Printf("Failed to increment retry count: %v", err)
		return
	}

	log.Printf("Transaction created successfully: ID=%s", payload.TransactionID)
}

func (w *PollingWorker) incrementRetryCount(ctx context.Context, taskID string) error {
	key := "retry_count:" + taskID
	_, err := w.Redis.Incr(ctx, key).Result()
	return err
}




// removeTaskFromQueue ลบ task ออกจาก queue ปัจจุบันเนื่องจากถึง limit
func (w *PollingWorker) removeTaskFromQueue(ctx context.Context, payload queue.PollingTask) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// ลบ task ออกจาก queue ปัจจุบัน
	if err := w.Redis.LRem(ctx, queue.JobQueueRecovery, 0, data).Err(); err != nil {
		return err
	}

	log.Printf("Task removed from queue: ID=%s", payload.ID)
	return nil
}


 




func (w *PollingWorker) getRetryCount(ctx context.Context, taskID string) (int, error) {
	key := "retry_count:" + taskID
	countStr, err := w.Redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil // หากไม่มีค่าใน Redis ให้ถือว่า retry count เป็น 0
	} else if err != nil {
		return 0, err
	}

	count, err := strconv.Atoi(countStr)
	if err != nil {
		return 0, err
	}

	return count, nil
}