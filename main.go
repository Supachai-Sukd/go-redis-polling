package main


import (
	"log"
	"net/http"

	"go_polling_jobs/queue"
	"go_polling_jobs/worker"
)

func main() {
	// เริ่ม worker ใน background (goroutine)
	go worker.RunWorker()

	// ตั้ง HTTP API endpoint สำหรับ enqueue task
	http.HandleFunc("/enqueue", queue.EnqueueHandler)

	log.Println("Server started on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

