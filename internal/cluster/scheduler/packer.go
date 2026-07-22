package scheduler

import (
	"ClusterManager/internal/cluster/core"
	"context"
	"errors"
	"fmt"
	"time"
)

// TODO: limit ctx

// PackNextJob will find the next job, and attempt to pack it in
func PackNextJob(ctx context.Context, config core.ConfigLoader, pool core.ContainerPool, queue core.JobQueue) error {
	maxCpu, maxMem := config.GetMachineLimits()
	usedCpu, usedMem := pool.UsedSpace()
	freeCpu := maxCpu - usedCpu
	freeMem := maxMem - usedMem

	preprocessCtx, preprocessCancel := context.WithTimeout(ctx, 3*time.Minute)
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
		pool.TrackedLaunch(preprocessCtx, newContainer.Id())
		assignedContainer = newContainer
	}

	if err := queue.MarkRunning(nextJob.Id, assignedContainer.Id()); err != nil {
		return fmt.Errorf("job %s failed to mark job as running", nextJob.Id)
	}

	if err := assignedContainer.AssignJob(nextJob.Id, nextJob.Payload); err != nil {
		return err
	}

	return nil
}
