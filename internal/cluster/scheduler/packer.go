package scheduler

import (
	"ClusterManager/internal/cluster/core"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
)

// TODO: limit ctx

// PackNextJob will find the next job, and attempt to pack it in
func PackNextJob(ctx context.Context, config core.ConfigLoader, pool core.ContainerPool, queue core.JobQueue) error {
	maxCpu, maxMem := config.GetMachineLimits()
	usedCpu, usedMem := pool.UsedSpace()
	freeCpu := maxCpu - usedCpu
	freeMem := maxMem - usedMem

	preprocessCtx, preprocessCancel := context.WithTimeout(ctx, 5*time.Second)
	defer preprocessCancel()

	nextJob := queue.NextBestJob(pool.AvailableImageNames(preprocessCtx), freeCpu, freeMem)
	if nextJob == nil {
		nextJob = queue.NextLargestJob(freeCpu, freeMem)
		if nextJob == nil {
			if len(pool.AvailableImageNames(preprocessCtx)) == 0 {
				return nil
			}
			return nil
		}
	}

	assignedContainer, err := pool.FindFreeContainer(nextJob.Image.Alias)
	if errors.Is(err, core.ErrAllOccupied) || errors.Is(err, core.ErrImageNotFound) { // Only cases where we can safely pack
		if nextJob.Image.MemoryLimit > freeMem || nextJob.Image.CPULimit > freeCpu { // Not enough room to pack
			return nil
		}

		newContainer, err := pool.AddFromImage(nextJob.Image)
		if err != nil {
			return err
		}
		pool.TrackedLaunch(ctx, newContainer.Id())
		assignedContainer = newContainer
	}

	if err := queue.MarkRunning(nextJob.Id, assignedContainer.Id()); err != nil {
		return fmt.Errorf("job %s failed to mark job as running", nextJob.Id)
	}

	if err := assignedContainer.AssignJob(nextJob.Id, nextJob.Payload); err != nil {
		return err
	}

	glog.V(2).Infof("Assigned job [%s] to container [%s].", nextJob.Id, assignedContainer.Id())

	if !nextJob.Image.Persistent {
		go func() {
			payload := assignedContainer.WaitForJobResult(ctx)
			if err := assignedContainer.FreeJob(); err != nil {
				return
			}
			if err := queue.MarkCompleted(nextJob.Id, payload, true); err != nil {
				return
			}
		}()
	}

	return nil
}
