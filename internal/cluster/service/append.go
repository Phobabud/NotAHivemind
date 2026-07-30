package service

import (
	pb "ClusterManager/api/gen/cluster/v1"
	"ClusterManager/internal/cluster/core"
	"context"
	"time"

	"github.com/golang/glog"
)

// AppendJob evaluates incoming scheduler requests and adds them to the execution queue.
func (s *ClusterServer) AppendJob(ctx context.Context, req *pb.JobRequest) (*pb.JobResponse, error) {
	s.lastContactFromScheduler = time.Now()

	if !s.containers.Ping() {
		glog.Errorf("Rejected job [%s]: Docker daemon is offline or unreachable", req.JobId)
		return &pb.JobResponse{
			JobId:  req.JobId,
			Accept: false,
		}, nil
	}

	var targetImage *core.Image
	for _, img := range s.images {
		if img.Alias == req.ImageAlias {
			targetImage = img
			break
		}
	}

	if targetImage == nil {
		glog.V(2).Infof("Rejected job [%s]: unknown image alias %s", req.JobId, req.ImageAlias)
		return &pb.JobResponse{
			JobId:  req.JobId,
			Accept: false,
		}, nil
	}

	job := core.NewJob(req.JobId, targetImage, int(req.Priority), req.Payload)

	if err := s.jobQueue.AddJob(job); err != nil {
		glog.V(2).Infof("Rejected job %s: failed to add to queue: %v", req.JobId, err)
		return &pb.JobResponse{
			JobId:  req.JobId,
			Accept: false,
		}, nil
	}

	glog.V(2).Infof("Accepted job %s into pending queue.", req.JobId)
	return &pb.JobResponse{
		JobId:            req.JobId,
		Accept:           true,
		AssignedNodeId:   s.nodeID,
		AssignedNodePort: s.nodeAddress,
	}, nil
}
