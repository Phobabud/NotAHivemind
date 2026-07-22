package scheduler

import (
	"ClusterManager/internal/cluster/core"
	"context"
	"time"
)

type HealthRule func(ctx context.Context, pool core.ContainerAPI, queue core.JobQueue) HealthAction

type HealthAction int

const (
	HealthContinue HealthAction = iota
	HealthSkip
	HealthDelete
)

func RunHealthRule(ctx context.Context, pool core.ContainerPool, queue core.JobQueue, actions ...HealthRule) error {
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	var deleteIDs []string

	if err := pool.ForEach(func(container core.ContainerAPI) error {
		for _, rule := range actions {
			action := rule(opCtx, container, queue)

			if action == HealthDelete {
				if container.HasJob() {
					_ = queue.MarkCompleted(container.JobID(), nil, false)
				}

				deleteIDs = append(deleteIDs, container.Id())
				if err := container.Delete(opCtx); err != nil {
					return err
				}
				break
			} else if action == HealthSkip {
				break
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Finalize deletions outside the loop to avoid map mutation deadlocks
	for _, id := range deleteIDs {
		_ = pool.Remove(id)
	}

	return nil
}

func HealthRuleUnresponsive() HealthRule {
	return func(ctx context.Context, container core.ContainerAPI, queue core.JobQueue) HealthAction {
		health, err := container.Health(ctx)
		if err != nil || health == 0 || health == 1 {
			return HealthContinue
		}

		return HealthDelete
	}
}

func HealthRuleTimedOut() HealthRule {
	return func(ctx context.Context, container core.ContainerAPI, queue core.JobQueue) HealthAction {
		if container.Image().Timeout <= 0 {
			return HealthContinue
		}

		if container.TotalJobs() > 0 && container.HasJob() && time.Since(container.LastAssignedJobTime()) > time.Second*time.Duration(container.Image().Timeout) {
			return HealthDelete
		}
		return HealthContinue
	}
}

func HealthRuleExceededJobLimits() HealthRule {
	return func(ctx context.Context, container core.ContainerAPI, queue core.JobQueue) HealthAction {
		if container.TotalJobs() > container.Image().MaxJobs && !container.HasJob() {
			return HealthDelete
		}
		return HealthContinue
	}
}
