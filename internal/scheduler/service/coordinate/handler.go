package coordinate

import (
	pb "ClusterManager/api/gen/scheduling/v1"
	"ClusterManager/internal/models"
	"ClusterManager/internal/scheduler/core"
	"ClusterManager/internal/scheduler/states"
	"context"
	"time"

	"github.com/golang/glog"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Handler implements the gRPC interface for Scheduler-to-Scheduler communication.
type Handler struct {
	pb.UnimplementedSchedulingCoordinationServiceServer

	state    *states.State
	cliConns []*Conn // Pool of peer connections we can actively talk to
}

// NewHandler creates the server-side listener for peer requests.
func NewHandler(s *states.State, peerConns []*Conn) *Handler {
	return &Handler{
		state:    s,
		cliConns: peerConns,
	}
}

// RequestJob handles a raw job submission (could be from an external client or another system).
func (h *Handler) RequestJob(ctx context.Context, req *pb.JobRequest) (*pb.JobResponse, error) {
	id := models.GenerateUniqueID(h.state.Id())
	h.state.AppendJob(&core.Job{
		Id:                id,
		ImageAlias:        *req.ImageAlias,
		CPURequirement:    req.CpuRequirement,
		MemoryRequirement: req.MemoryRequirement,
		Priority:          int(*req.Priority),
		Payload:           req.Payload,
		Response:          nil,
	})
	glog.V(2).Infof("Received new JobRequest. Assigned ID: %s", id)

	return &pb.JobResponse{
		JobId:  id,
		Accept: true,
	}, nil
}

// RequestJobStatus fields inquiries about a job's current standing.
func (h *Handler) RequestJobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	glog.V(2).Infof("Received status request for job: %s", req.JobId)

	job, err := h.state.Job(req.JobId)
	if err != nil {
		return &pb.JobStatusResponse{
			JobId:      req.JobId,
			Status:     "NOT_FOUND",
			Priority:   -1,
			ImageAlias: "",
			Payload:    nil,
		}, nil
	}

	return &pb.JobStatusResponse{
		JobId:      job.Id,
		Status:     job.Status.String(),
		Priority:   int32(job.Priority),
		ImageAlias: job.ImageAlias,
		Payload:    job.Payload,
	}, nil
}

// Redistribute receives a job that another scheduler couldn't process.
func (h *Handler) Redistribute(ctx context.Context, req *pb.RedistributeRequest) (*pb.RedistributeResponse, error) {
	glog.V(2).Infof("Received redistributed job %s from peer %s", req.JobId, req.OriginSchedulerId)

	// Can we actually handle it?
	if h.state.LeastUtilizedCluster(req.CpuRequirement, req.MemoryRequirement) == "" {
		glog.V(2).Infof("Rejected redistributed job %s: no immediate capacity available.", req.JobId)
		return &pb.RedistributeResponse{
			JobId:               req.JobId,
			AssignedSchedulerId: h.state.Id(),
			Accept:              false,
		}, nil
	}
	h.state.AppendJob(&core.Job{
		Id:                req.JobId,
		ImageAlias:        *req.ImageAlias,
		CPURequirement:    req.CpuRequirement,
		MemoryRequirement: req.MemoryRequirement,
		Priority:          int(*req.Priority),
		Payload:           req.Payload,
		Response:          nil,
	})

	return &pb.RedistributeResponse{
		JobId:               req.JobId,
		AssignedSchedulerId: h.state.Id(), // Acknowledge that we took ownership
		Accept:              true,
	}, nil
}

// Heartbeat receives load reports from peer schedulers and saves them to the local state.
func (h *Handler) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	newStates := make(map[string]*states.ClusterState)
	for _, usage := range req.ClusterUsage {
		cluster := states.ClusterState{
			ID:          usage.ClusterId,
			UsedCpu:     usage.CpuUsage,
			UsedMem:     usage.MemUsage,
			MaxCpu:      usage.TotalCpu,
			MaxMem:      usage.TotalMem,
			LastUpdated: time.Now(),
		}
		newStates[usage.ClusterId] = &cluster
	}
	h.state.UpdateSchedulerState(req.SchedulerId, newStates)
	return &emptypb.Empty{}, nil
}

func (h *Handler) ForEach(ctx context.Context, op func(ct context.Context, co *Conn) error) {
	for _, conn := range h.cliConns {
		if err := op(ctx, conn); err != nil {
			glog.Errorf("Error during operation on peer %s: %v", conn.nodeID, err)
		}
	}
}

func (h *Handler) OpConn(ctx context.Context, schedulerId string, op func(ct context.Context, co *Conn) error) error {
	for _, conn := range h.cliConns {
		if conn.nodeID != schedulerId {
			continue
		}

		if err := op(ctx, conn); err != nil {
			return err
		}
		break
	}
	return nil
}
