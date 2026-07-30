package service

import (
	pb "ClusterManager/api/gen/cluster/v1"
	"context"
	"time"

	"github.com/golang/glog"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ClusterStatus acts as the snapshot heartbeat. It calculates available resources on the fly.
func (s *ClusterServer) ClusterStatus(ctx context.Context, _ *emptypb.Empty) (*pb.ClusterStatusResponse, error) {
	s.lastContactFromScheduler = time.Now()
	usedCPU, usedMem := s.containers.UsedSpace()
	activeJobs := s.jobQueue.ActiveJobs()

	// Extract just the IDs to keep the payload incredibly small
	activeJobIDs := make([]string, 0, len(activeJobs))
	for _, job := range activeJobs {
		activeJobIDs = append(activeJobIDs, job.Id)
	}
	return &pb.ClusterStatusResponse{
		NodeId:          s.nodeID,
		NodeAddress:     s.nodeAddress,
		TotalCpu:        s.totalCPU,
		TotalMemory:     s.totalMemory,
		AvailableCpu:    s.totalCPU - usedCPU,
		AvailableMemory: s.totalMemory - usedMem,
		ActiveJobIds:    activeJobIDs,
	}, nil
}

// JobStatus streams terminal events (Completed/Failed) back to the connected Scheduler.
func (s *ClusterServer) JobStatus(_ *emptypb.Empty, stream pb.ClusterService_JobStatusServer) error {
	glog.V(1).Infof("Scheduler connected to JobStatus event stream.")

	for {
		select {
		case <-stream.Context().Done():
			glog.V(1).Infof("Scheduler disconnected from JobStatus stream.")
			return stream.Context().Err()

		case job, ok := <-s.statusCh:
			if !ok {
				glog.V(1).Infof("Internal status channel closed. Shutting down stream.")
				return nil
			}

			// Determine logical success based on your internal job state definitions
			success := "COMPLETED"
			if !job.Succeeded {
				success = "FAILED"
			}

			resp := &pb.JobStatusResponse{
				JobId:      job.Id,
				Status:     success,
				Priority:   int32(job.Priority),
				ImageAlias: job.Image.Alias,
				Payload:    job.Payload,
			}

			if err := stream.Send(resp); err != nil {
				glog.Errorf("Failed to push event for job %s to Scheduler: %v", job.Id, err)
				return err
			}

			glog.V(2).Infof("Pushed %s event for job %s to Scheduler.", success, job.Id)
		}
	}
}
