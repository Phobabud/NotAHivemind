package scheduler

import (
	"NotAHiveMind/internal/cluster/core"
	"context"
	"errors"
	"time"

	"github.com/golang/glog"
)

// PackNextJob will find the next job, and attempt to pack it in
func PackNextJob(ctx context.Context, config core.ConfigLoader, pool core.ContainerPool, queue core.JobQueue) error {
	maxCpu, maxMem := config.GetMachineLimits()
	usedCpu, usedMem := queue.ActiveUtilization()
	freeCpu := maxCpu - usedCpu
	freeMem := maxMem - usedMem

	queueSearchCtx, queueSearchCancel := context.WithTimeout(ctx, 1*time.Second)
	defer queueSearchCancel()
	nextJob := queue.NextBestJob(pool.AvailableImageNames(queueSearchCtx), freeCpu, freeMem)
	if nextJob == nil {
		return nil
	}

	containerInspectionCtx, containerInspectionCancel := context.WithTimeout(ctx, 5*time.Second)
	defer containerInspectionCancel()
	assignedContainer, err := pool.FindFreeContainer(containerInspectionCtx, nextJob.Image.Alias)

	switch {
	case err == nil: // Do nothing
	case errors.Is(err, core.ErrAllOccupied), errors.Is(err, core.ErrImageNotFound):
		if nextJob.Image.MemoryBytesLimit > freeMem || nextJob.Image.NanoCPULimit > freeCpu { // Not enough room to pack
			return nil
		}

		newContainer, err := pool.AddFromImage(nextJob.Image)
		if err != nil {
			return err
		}
		pool.TrackedLaunch(ctx, newContainer.Id())
		assignedContainer = newContainer
	default:
		return err
	}

	if err := queue.MarkRunning(nextJob.Id, assignedContainer.Id()); err != nil {
		return err
	}

	go assignJob(ctx, assignedContainer, *nextJob, queue)
	return nil
}

func assignJob(ctx context.Context, assignedContainer core.ContainerAPI, job core.Job, queue core.JobQueue) {
	if err := assignedContainer.WaitUntilReady(ctx, 15*time.Second); err != nil {
		_ = queue.RollbackJob(job.Id)
		assignedContainer.Release()
		glog.Errorf("Failed to wait for container [%s] to be ready for job [%s]: %v", assignedContainer.Id(), job.Id, err)
		return
	}

	if err := queue.MarkRunning(job.Id, assignedContainer.Id()); err != nil {
		_ = queue.RollbackJob(job.Id)
		assignedContainer.Release()
		glog.Errorf("Job [%s] failed to mark as running: %v", job.Id, err)
		return
	}

	if err := assignedContainer.AssignJob(job.Id, job.Payload); err != nil {
		_ = queue.RollbackJob(job.Id)
		glog.Errorf("Job [%s] failed to assign job: %v", job.Id, err)
		return
	}

	glog.V(2).Infof("Assigned job [%s] to container [%s].", job.Id, assignedContainer.Id())

	if !job.Image.Persistent {
		payload := assignedContainer.WaitForJobResult(ctx)
		if err := assignedContainer.FreeJob(); err != nil {
			glog.Errorf("Failed to free job [%s] from container [%s]: %v", job.Id, assignedContainer.Id(), err)
		}
		if err := queue.MarkCompleted(job.Id, payload, true); err != nil {
			glog.Errorf("Failed to mark job [%s] as completed: %v", job.Id, err)
		}
	}
}
