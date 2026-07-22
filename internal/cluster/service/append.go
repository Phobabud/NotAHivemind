package service

import (
	pb "ClusterManager/api/gen/cluster/v1"
	"ClusterManager/internal/cluster/core"
	"context"

	"github.com/golang/glog"
)

// AppendJob evaluates incoming scheduling requests and adds them to the execution queue.
func (s *ClusterServer) AppendJob(ctx context.Context, req *pb.JobRequest) (*pb.JobResponse, error) {
	var targetImage *core.Image
	for _, img := range s.images {
		if img.Alias == req.ImageAlias {
			targetImage = img
			break
		}
	}

	if targetImage == nil {
		glog.Infof("Rejected job %s: unknown image alias %s", req.JobId, req.ImageAlias)
		return &pb.JobResponse{
			JobId:  req.JobId,
			Accept: false,
		}, nil
	}

	job := core.NewJob(req.JobId, targetImage, int(req.Priority), req.Payload)

	if err := s.jobQueue.AddJob(job); err != nil {
		glog.Infof("Rejected job %s: failed to add to queue: %v", req.JobId, err)
		return &pb.JobResponse{
			JobId:  req.JobId,
			Accept: false,
		}, nil
	}

	glog.Infof("Accepted job %s into pending queue.", req.JobId)
	return &pb.JobResponse{
		JobId:            req.JobId,
		Accept:           true,
		AssignedNodeId:   s.nodeID,
		AssignedNodePort: s.nodeAddress,
	}, nil
}
