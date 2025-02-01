package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hibiken/asynq"
)

// กำหนด task type (คล้าย job id ใน BullJS)
const TypeEmailDelivery = "email:deliver"

// redisOpt สำหรับเชื่อมต่อ Redis (asynq ต้องการ Redis เป็น backend)
var redisOpt = asynq.RedisClientOpt{
	Addr: "127.0.0.1:6379",
}

// EnqueueRequest คือข้อมูลที่รับมาจาก client ผ่าน HTTP POST
type EnqueueRequest struct {
	Times    int    `json:"times"`    // จำนวนครั้งที่ต้องการ enqueue task
	Duration int    `json:"duration"` // ระยะเวลาประมวลผล (วินาที)
	Email    string `json:"email"`    // Email address
	Subject  string `json:"subject"`  // หัวข้ออีเมล
	Body     string `json:"body"`     // เนื้อหาอีเมล
}

// EmailPayload คือข้อมูลอีเมลที่จะส่งใน task
type EmailPayload struct {
	Email   string `json:"email"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// JobPayload คือ payload ที่ส่งไปใน task queue
type JobPayload struct {
	EmailPayload EmailPayload `json:"email"`
	Duration     int          `json:"duration"`
}

// EnqueueHandler รับ HTTP POST request เพื่อ enqueue task
func EnqueueHandler(w http.ResponseWriter, r *http.Request) {
	// ตรวจสอบ HTTP method
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode request body
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// ตรวจสอบความถูกต้องของ input เบื้องต้น
	if req.Times <= 0 || req.Duration <= 0 ||
		req.Email == "" || req.Subject == "" || req.Body == "" {
		http.Error(w, "Missing or invalid fields", http.StatusBadRequest)
		return
	}

	// สร้าง asynq client
	client := asynq.NewClient(redisOpt)
	defer client.Close()

	// สร้าง payload สำหรับ task
	jobPayload := JobPayload{
		EmailPayload: EmailPayload{
			Email:   req.Email,
			Subject: req.Subject,
			Body:    req.Body,
		},
		Duration: req.Duration,
	}
	payloadBytes, err := json.Marshal(jobPayload)
	if err != nil {
		http.Error(w, "Failed to marshal job payload", http.StatusInternalServerError)
		return
	}

	// Enqueue task ตามจำนวนที่ระบุ
	//ctx := r.Context()
	for i := 0; i < req.Times; i++ {
		task := asynq.NewTask(TypeEmailDelivery, payloadBytes)
		// กำหนด options (MaxRetry, Timeout, Delay ก่อนประมวลผล)
		info, err := client.Enqueue(task,
			asynq.MaxRetry(10),
			asynq.Timeout(30*time.Second),
			asynq.ProcessIn(5*time.Second),
		)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to enqueue task: %v", err), http.StatusInternalServerError)
			return
		}
		log.Printf("Enqueued task: id=%s, queue=%s", info.ID, info.Queue)
	}

	// ส่ง response กลับไปยัง client
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Tasks enqueued successfully"))
}