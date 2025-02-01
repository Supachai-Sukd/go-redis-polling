package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"go_polling_jobs/queue"
	"go_polling_jobs/recovery"
	"go_polling_jobs/worker"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func GenerateRandomID() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%d", rand.Intn(1000000))
}

func GenerateTransactionID() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("txn_%d", rand.Intn(1000000))
}

func main() {
	dsn := "host=localhost user=postgres password=123456 dbname=go_polling port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	if err := db.AutoMigrate(&worker.Transaction{}); err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}
	log.Println("Database migrated successfully!")

	// connect Redis
	redisAddr := "localhost:6379"
	redisClient := queue.NewRedisClient(redisAddr)

	// Start worker process
	go worker.StartWorker(db, redisClient)
	go recovery.StartWorker(db, redisClient)

	for i := 0; i < 50; i++ {
		// add queue
		payload := queue.PollingTask{
			ID:            GenerateRandomID(),
			TransactionID: GenerateTransactionID(),
		}
		if err := queue.EnqueueTask(redisClient, payload); err != nil {
			log.Fatalf("Failed to enqueue task: %v", err)
		} else {
			log.Println("Task enqueued successfully!")
		}
	}

	select {} // Keep the program running
}
