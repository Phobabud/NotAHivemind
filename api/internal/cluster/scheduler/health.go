package scheduler

import (
	"NotAHiveMind/internal/cluster/core"
	"context"
	"time"

	"github.com/golang/glog"
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
				glog.V(3).Infof("Container [%s] failed a healthcheck with an action of Delete", container.Id())
				if container.State() == core.Running {
					_ = queue.MarkCompleted(container.JobID(), nil, false)
				}

				deleteIDs = append(deleteIDs, container.Id())
				if err := container.Cleanup(opCtx); err != nil {
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
		_ = pool.Remove(ctx, id)
	}

	return nil
}

func HealthRuleUnresponsive() HealthRule {
	return func(ctx context.Context, container core.ContainerAPI, queue core.JobQueue) HealthAction {
		health, err := container.Health(ctx)
		if err != nil || health == 0 || health == 1 || container.State() == core.Reserved {
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

		if container.TotalJobs() > 0 && container.State() == core.Free && time.Since(container.LastStateChange()) > time.Second*time.Duration(container.Image().Timeout) {
			return HealthDelete
		}
		return HealthContinue
	}
}

func HealthRuleExceededJobLimits() HealthRule {
	return func(ctx context.Context, container core.ContainerAPI, queue core.JobQueue) HealthAction {
		if container.TotalJobs() > container.Image().MaxJobs && container.State() == core.Free {
			return HealthDelete
		}
		return HealthContinue
	}
}
