package main

import (
	opensearchclient "atlasq/internal/opensearchclient"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	opensearchURL  string
	opensearchUser string
	opensearchPass string
	pipeline       string
	addr           string
)

func init() {
	opensearchURL = os.Getenv("OPENSEARCH_URL")
	if opensearchURL == "" {
		opensearchURL = "http://localhost:9200"
	}
	opensearchUser = os.Getenv("OPENSEARCH_USER")
	opensearchPass = os.Getenv("OPENSEARCH_PASS")
	pipeline = os.Getenv("OPENSEARCH_PIPELINE")
	if pipeline == "" {
		pipeline = "atlasq-normalize"
	}
	addr = os.Getenv("FORWARDER_ADDR")
	if addr == "" {
		addr = ":8081"
	}
}

func main() {
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	http.HandleFunc("/webhook", webhookHandler)

	log.Printf("forwarder starting on %s -> %s (pipeline=%s)", addr, opensearchURL, pipeline)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

// webhookHandler receives a JSON body, inspects field `type`, and forwards to the appropriate OpenSearch alias.
func webhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("read body error: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// keep a copy for forwarding
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		// if not JSON object, still attempt to forward as-is to default alias
		m = map[string]interface{}{"type": "order"}
	}

	typ := "order"
	if v, ok := m["type"]; ok {
		if s, ok := v.(string); ok && s != "" {
			typ = s
		}
	}

	// Decide alias based on type (centralized mapping)
	alias := opensearchclient.AliasForType(typ)

	// Build URL: {opensearchURL}/{alias}/_doc?pipeline={pipeline}
	url := fmt.Sprintf("%s/%s/_doc", opensearchURL, alias)
	if pipeline != "" {
		url = url + "?pipeline=" + pipeline
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("create request error: %v", err)
		http.Error(w, "internal", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if opensearchUser != "" {
		req.SetBasicAuth(opensearchUser, opensearchPass)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("forward error: %v", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Read response for logging
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
		log.Printf("forwarded to %s, status=%d", alias, resp.StatusCode)
		return
	}

	log.Printf("opensearch returned status=%d body=%s", resp.StatusCode, string(respBody))
	http.Error(w, "forward failed", http.StatusBadGateway)
}
