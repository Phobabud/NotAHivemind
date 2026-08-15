package cluster

import (
	pb "NotAHiveMind/api/gen/cluster/v1"
	"NotAHiveMind/internal/models"
	"reflect"
	"testing"
)

func TestParseClusterStatus(t *testing.T) {
	t.Run("Nil Safety", func(t *testing.T) {
		result := ParseClusterStatus(nil)
		if result != nil {
			t.Errorf("Expected nil result for nil input, got %+v", result)
		}
	})

	t.Run("Valid Mapping", func(t *testing.T) {
		// Create a dummy protobuf response mimicking what the network would provide
		mockPb := &pb.ClusterStatusResponse{
			NodeId:               "cluster-worker-1",
			NodeAddress:          "127.0.0.1:9090",
			TotalNanoCpu:         16,
			TotalMemoryBytes:     32000,
			AvailableNanoCpu:     12,
			AvailableMemoryBytes: 16000,
			ActiveJobIds:         []string{"job-1", "job-2"},
		}

		// Run the translation
		result := ParseClusterStatus(mockPb)

		// Assertions to guarantee fields weren't mixed up during translation
		if result.NodeID != "cluster-worker-1" {
			t.Errorf("Expected NodeID 'cluster-worker-1', got '%s'", result.NodeID)
		}
		if result.AvailableCPU != 12 {
			t.Errorf("Expected AvailableCPU 12, got %d", result.AvailableCPU)
		}
		if len(result.ActiveJobIDs) != 2 || result.ActiveJobIDs[1] != "job-2" {
			t.Errorf("Expected ActiveJobIDs to map correctly, got %v", result.ActiveJobIDs)
		}
		if result.LastUpdated.IsZero() {
			t.Errorf("Expected LastUpdated to be populated with current time")
		}
	})
}

func TestParseJobEvent(t *testing.T) {
	t.Run("Nil Safety", func(t *testing.T) {
		result := ParseJobEvent(nil)
		if result != nil {
			t.Errorf("Expected nil result for nil input")
		}
	})

	t.Run("Valid Mapping With Payload", func(t *testing.T) {
		mockPayload := []byte(`{"result": "success", "accuracy": 0.99}`)
		mockPb := &pb.JobStatusResponse{
			JobId:      "job-test-1",
			Status:     pb.JobStatus_JOB_STATUS_COMPLETED,
			Priority:   5,
			ImageAlias: "ml-worker",
			Payload:    mockPayload,
		}

		result := ParseJobEvent(mockPb)

		if result.JobID != "job-test-1" {
			t.Errorf("Expected JobID 'job-test-1', got '%s'", result.JobID)
		}
		if result.Status != models.Completed {
			t.Errorf("Expected Status 'COMPLETED', got '%s'", result.Status)
		}
		if result.Priority != 5 {
			t.Errorf("Expected Priority 5, got %d", result.Priority)
		}
		if !reflect.DeepEqual(result.Payload, mockPayload) {
			t.Errorf("Expected Payload to be perfectly copied over")
		}
	})
}
