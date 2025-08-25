package opensearchclient

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	tasks "atlasq/internal/tasks"
)

type LogEvent struct {
	Type        string      `json:"type"`
	OrderID     int64       `json:"order_id"`
	Message     string      `json:"message"`
	TenantID    int64       `json:"tenant_id"`
	WarehouseID int64       `json:"warehouse_id"`
	Items       interface{} `json:"items"`
	Status      string      `json:"status"`
	Timestamp   time.Time   `json:"timestamp"`
	Error       string      `json:"error,omitempty"`
	Level       string      `json:"level,omitempty"`
	DevName     string      `json:"dev_name,omitempty"`
}

// LogDebug: สำหรับ dev ใช้ log debug event
func LogDebug(devName, message string, payload interface{}) {
	event := LogEvent{
		Type:      "debug",
		Level:     "DEBUG",
		DevName:   devName,
		Message:   message,
		Timestamp: time.Now(),
	}
	// ถ้า payload เป็น map หรือ struct ที่ต้องการ log เพิ่มเติม
	if payload != nil {
		// marshal/unmarshal เพื่อแปลงเป็น map[string]interface{}
		b, err := json.Marshal(payload)
		if err == nil {
			var m map[string]interface{}
			if json.Unmarshal(b, &m) == nil {
				// ใส่ field เพิ่มใน event
				if v, ok := m["order_id"]; ok {
					if id, ok := v.(float64); ok {
						event.OrderID = int64(id)
					}
				}
				if v, ok := m["tenant_id"]; ok {
					if id, ok := v.(float64); ok {
						event.TenantID = int64(id)
					}
				}
				if v, ok := m["warehouse_id"]; ok {
					if id, ok := v.(float64); ok {
						event.WarehouseID = int64(id)
					}
				}
				if v, ok := m["items"]; ok {
					event.Items = v
				}
				if v, ok := m["status"]; ok {
					if s, ok := v.(string); ok {
						event.Status = s
					}
				}
				if v, ok := m["error"]; ok {
					if s, ok := v.(string); ok {
						event.Error = s
					}
				}
			}
		}
	}
	for _, sink := range enabledSinks {
		_ = sink.Write(event)
	}
}

type LogSink interface {
	Write(event LogEvent) error
}

// --- Webhook Sink ---
type WebhookSink struct {
	URL string
}

func (w *WebhookSink) Write(event LogEvent) error {
	body, _ := json.Marshal(event)
	resp, err := http.Post(w.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[WebhookSink] error: %v", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

// --- DB Sink (mock, log only) ---
type DBSink struct{}

func (d *DBSink) Write(event LogEvent) error {
	// TODO: implement real DB insert
	log.Printf("[DBSink] would insert: %+v", event)
	return nil
}

// --- App Log Sink ---
type AppLogSink struct{}

func (a *AppLogSink) Write(event LogEvent) error {
	log.Printf("[AppLogSink] %s | order_id=%d | %s | status=%s | error=%s", event.Type, event.OrderID, event.Message, event.Status, event.Error)
	return nil
}

// --- Sink registry and LogOrder ---
var enabledSinks []LogSink

func init() {
	// Enable sinks by env/config
	enabledSinks = []LogSink{}
	if url := os.Getenv("DASHBOARD_WEBHOOK_URL"); url != "" {
		enabledSinks = append(enabledSinks, &WebhookSink{URL: url})
	}
	if os.Getenv("ENABLE_DB_SINK") == "1" {
		enabledSinks = append(enabledSinks, &DBSink{})
	}
	enabledSinks = append(enabledSinks, &AppLogSink{}) // always enabled
}

// LogOrder: ใช้จาก worker, payload = tasks.DeductStockPayload
func LogOrder(p tasks.DeductStockPayload, status, message, errMsg string) {
	event := LogEvent{
		Type:        "order",
		OrderID:     p.OrderID,
		Message:     message,
		TenantID:    p.TenantID,
		WarehouseID: p.WarehouseID,
		Items:       p.Items,
		Status:      status,
		Timestamp:   time.Now(),
		Error:       errMsg,
	}
	for _, sink := range enabledSinks {
		_ = sink.Write(event)
	}
}
