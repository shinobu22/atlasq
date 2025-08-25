package opensearchclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	tasks "atlasq/internal/tasks"

	"github.com/opensearch-project/opensearch-go"
)

var (
	client *opensearch.Client
	once   sync.Once
	idx    string
)

func initClient() {
	once.Do(func() {
		idx = os.Getenv("OPENSEARCH_LOG_INDEX")
		if idx == "" {
			idx = "atlasq-logs-write"
		}
		addr := os.Getenv("OPENSEARCH_URL")
		if addr == "" {
			addr = "http://localhost:9200"
		}
		c, err := opensearch.NewClient(opensearch.Config{
			Addresses: []string{addr},
		})
		if err != nil {
			log.Printf("opensearch init error: %v", err)
			return
		}
		client = c
	})
}

func sendRaw(doc []byte) error {
	initClient()
	if client == nil {
		return fmt.Errorf("opensearch client not initialized")
	}

	// simple retry
	var lastErr error
	for i := 0; i < 3; i++ {
		res, err := client.Index(
			idx,
			bytes.NewReader(doc),
			client.Index.WithContext(context.Background()),
		)
		if err != nil {
			lastErr = err
		} else {
			defer res.Body.Close()
			if res.IsError() {
				b, _ := io.ReadAll(res.Body)
				lastErr = fmt.Errorf("opensearch index error: %s", string(b))
			} else {
				return nil
			}
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	return lastErr
}

// LogOrder builds a standardized log document from DeductStockPayload and sends it
func LogOrder(p tasks.DeductStockPayload, status, message, errMsg string) {
	doc := map[string]interface{}{
		"type":         "order",
		"order_id":     p.OrderID,
		"message":      message,
		"tenant_id":    p.TenantID,
		"warehouse_id": p.WarehouseID,
		"items":        p.Items,
		"status":       status,
		"timestamp":    time.Now().Format(time.RFC3339),
	}
	if errMsg != "" {
		doc["error"] = errMsg
	}
	b, _ := json.Marshal(doc)
	if err := sendRaw(b); err != nil {
		log.Printf("opensearch send failed: %v", err)
	}
}
