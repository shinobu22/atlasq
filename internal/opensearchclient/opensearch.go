package opensearchclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	tasks "atlasq/internal/tasks"

	opensearch "github.com/opensearch-project/opensearch-go"
)

type LogEvent struct {
	Type        string      `json:"type"`
	OrderID     int64       `json:"order_id"`
	OrderNumber string      `json:"order_number,omitempty"`
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
		if err := sink.Write(event); err != nil {
			log.Printf("[opensearchclient] LogDebug sink error: %v", err)
		}
	}
}

type LogSink interface {
	Write(event LogEvent) error
}

// AliasForType returns the OpenSearch write alias for a given event type.
// This centralizes the alias mapping so callers (forwarder, sinks) reuse the same logic.
func AliasForType(typ string) string {
	switch typ {
	case "order":
		return "atlasq-hooks-write"
	case "debug":
		return "atlasq-debug-write"
	case "query":
		return "atlasq-queries-write"
	default:
		return "atlasq-all-write"
	}
}

// --- Webhook Sink ---
type WebhookSink struct {
	URL string
}

func (w *WebhookSink) Write(event LogEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("[WebhookSink] marshal error: %v", err)
		return err
	}

	// Build target URL. If sink URL contains only host (no path) we route by event.Type -> alias
	target := w.URL
	parsed, perr := url.Parse(w.URL)
	if perr == nil {
		// has no path or only root path -> construct path based on type
		path := strings.TrimRight(parsed.Path, "/")
		if path == "" || path == "/" {
			base := fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
			alias := AliasForType(event.Type)
			target = fmt.Sprintf("%s/%s/_doc", base, alias)
		}
	}

	// Add pipeline if provided via env
	if p := os.Getenv("OPENSEARCH_PIPELINE"); p != "" {
		if strings.Contains(target, "?") {
			target = target + "&pipeline=" + url.QueryEscape(p)
		} else {
			target = target + "?pipeline=" + url.QueryEscape(p)
		}
	}

	client := &http.Client{Timeout: 5 * time.Second}
	var lastErr error
	// simple retry
	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest(http.MethodPost, target, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			log.Printf("[WebhookSink] attempt=%d error=%v target=%s", i+1, err, target)
			time.Sleep(time.Duration(i+1) * 200 * time.Millisecond)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		// read response body for logging
		var respBuf bytes.Buffer
		_, _ = respBuf.ReadFrom(resp.Body)
		lastErr = fmt.Errorf("opensearch returned status=%d body=%s", resp.StatusCode, respBuf.String())
		log.Printf("[WebhookSink] attempt=%d %v", i+1, lastErr)
		time.Sleep(time.Duration(i+1) * 200 * time.Millisecond)
	}
	log.Printf("[WebhookSink] final error: %v", lastErr)
	return lastErr
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
	// If OPENSEARCH_URL provided, enable direct OpenSearch sink
	if os.Getenv("OPENSEARCH_URL") != "" {
		enabledSinks = append(enabledSinks, &OpenSearchSink{})
	}
	if url := os.Getenv("DASHBOARD_WEBHOOK_URL"); url != "" {
		enabledSinks = append(enabledSinks, &WebhookSink{URL: url})
	}
	if os.Getenv("ENABLE_DB_SINK") == "1" {
		enabledSinks = append(enabledSinks, &DBSink{})
	}
	enabledSinks = append(enabledSinks, &AppLogSink{}) // always enabled
}

// --- OpenSearch Sink (indexes documents directly) ---
type OpenSearchSink struct{}

var (
	osClient *opensearch.Client
	osOnce   sync.Once
	osIndex  string
)

func initOSClient() {
	osOnce.Do(func() {
		osIndex = os.Getenv("OPENSEARCH_LOG_INDEX")
		if osIndex == "" {
			osIndex = "atlasq-logs-write"
		}
		addr := os.Getenv("OPENSEARCH_URL")
		if addr == "" {
			return
		}
		c, err := opensearch.NewClient(opensearch.Config{Addresses: []string{addr}})
		if err != nil {
			log.Printf("opensearch init error: %v", err)
			return
		}
		osClient = c
	})
}

func (o *OpenSearchSink) Write(event LogEvent) error {
	initOSClient()
	if osClient == nil {
		return fmt.Errorf("opensearch client not initialized")
	}
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	log.Printf("[OpenSearchSink] marshaled event len=%d type=%s order_id=%d", len(b), event.Type, event.OrderID)

	// try a few times
	var lastErr error
	for i := 0; i < 3; i++ {
		log.Printf("[OpenSearchSink] attempt=%d index=%s", i+1, osIndex)
		res, err := osClient.Index(
			osIndex,
			bytes.NewReader(b),
			osClient.Index.WithContext(context.Background()),
		)
		if err != nil {
			lastErr = err
			log.Printf("[OpenSearchSink] attempt=%d error=%v", i+1, err)
		} else {
			defer res.Body.Close()
			bodyBytes, _ := io.ReadAll(res.Body)
			status := res.StatusCode
			log.Printf("[OpenSearchSink] attempt=%d response status=%d body=%s", i+1, status, string(bodyBytes))
			if res.IsError() {
				lastErr = fmt.Errorf("opensearch index error status=%d body=%s", status, string(bodyBytes))
			} else {
				return nil
			}
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	log.Printf("[OpenSearchSink] final error: %v", lastErr)
	return lastErr
}

// LogOrder: ใช้จาก worker, payload = tasks.DeductStockPayload
func LogOrder(p tasks.DeductStockPayload, status, message, errMsg string) {
	event := LogEvent{
		Type:        "order",
		OrderID:     p.OrderID,
		OrderNumber: p.OrderNumber,
		Message:     message,
		TenantID:    p.TenantID,
		WarehouseID: p.WarehouseID,
		Items:       p.Items,
		Status:      status,
		Timestamp:   time.Now(),
		Error:       errMsg,
	}
	for _, sink := range enabledSinks {
		if err := sink.Write(event); err != nil {
			log.Printf("[opensearchclient] LogOrder sink error: %v", err)
		}
	}
}
