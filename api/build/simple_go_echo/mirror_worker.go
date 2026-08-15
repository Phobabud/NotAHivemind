package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	payloadDir = "/job/payload"
	resultsDir = "/job/results"
)

func main() {
	log.Println("Starting persistent Go worker...")

	_ = os.MkdirAll(payloadDir, 0777)
	_ = os.MkdirAll(resultsDir, 0777)

	// Run the internal healthcheck server on port 3000 in the background
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		log.Println("Internal health server listening on :3000")
		if err := http.ListenAndServe(":3000", nil); err != nil {
			log.Fatalf("Health server crashed: %v", err)
		}
	}()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			processAvailableJobs()
		}
	}
}

func processAvailableJobs() {
	entries, err := os.ReadDir(payloadDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		payloadPath := filepath.Join(payloadDir, entry.Name())
		if err := processSingleJob(payloadPath, entry.Name()); err != nil {
			log.Printf("Error processing job %s: %v", entry.Name(), err)
		}
	}
}

func processSingleJob(payloadPath string, filename string) error {
	rawBytes, err := os.ReadFile(payloadPath)
	if err != nil {
		return err
	}

	var data map[string]interface{}
	if err := json.Unmarshal(rawBytes, &data); err != nil {
		_ = os.Remove(payloadPath)
		return err
	}

	tmpResultPath := filepath.Join(resultsDir, filename+".tmp")
	finalResultPath := filepath.Join(resultsDir, filename)

	// ATOMIC WRITE & SYNC
	f, err := os.Create(tmpResultPath)
	if err != nil {
		return err
	}
	if _, err := f.Write(rawBytes); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // Sync works perfectly here because it's writable!
		f.Close()
		return err
	}
	f.Close()

	// RENAME (Instantly visible to the Scheduler)
	if err := os.Rename(tmpResultPath, finalResultPath); err != nil {
		_ = os.Remove(tmpResultPath)
		return err
	}

	if err := os.Remove(payloadPath); err != nil {
		return err
	}

	log.Printf("Successfully processed and mirrored job: %s", filename)
	return nil
}
