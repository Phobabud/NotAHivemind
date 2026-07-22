package scheduler

import (
	"ClusterManager/internal/cluster/core"
	"fmt"
	"testing"
	"time"
)

var (
	smallImage  = &core.Image{Alias: "kuru kuru", CPULimit: 10, MemoryLimit: 20}
	middleImage = &core.Image{Alias: "kuru kuru2", CPULimit: 5, MemoryLimit: 40}
	bigImage    = &core.Image{Alias: "kuru kuru3", CPULimit: 50, MemoryLimit: 30}
)

func TestJobs_AddJob(t *testing.T) {
	job := core.Job{Id: "jobbyTest", Priority: 1, Image: middleImage}
	j := CreateJobsQueue()
	err := j.AddJob(&job)
	if err != nil {
		t.Fatalf("Job add job failed %v", err)
	}

	if len(j.pending) != 1 {
		t.Errorf("Job not found in Pending %v", j.pending)
	}

	job2 := core.Job{Id: "jobbyTest", Priority: 2, Image: middleImage}
	err = j.AddJob(&job2)
	if err == nil {
		t.Errorf("Job was added when it was supposed to fail, %v", err)
	}

	job3 := core.Job{Id: "jobbyTest2", Priority: 3, Image: smallImage}
	err = j.AddJob(&job3)
	if err != nil || len(j.pending) != 2 {
		t.Errorf("Job failed to be added when it is supposed to succeed, %v", err)
	}

	if j.pending[0].Id != "jobbyTest2" {
		t.Errorf("Job was added in an incorrect order")
	}

	job4 := core.Job{Id: "jobbyTest3", Priority: 3, Image: bigImage}
	err = j.AddJob(&job4)
	if err != nil || len(j.pending) != 3 {
		t.Errorf("Job failed to be added when it is supposed to succeed, %v", err)
	}

	if j.pending[0].Id != "jobbyTest3" || j.pending[1].Id != "jobbyTest2" {
		t.Errorf("Job was added in an incorrect order")
	}

	if len(j.active) != 0 {
		t.Errorf("Job somehow became active when no changes to be active were requested")
	}
}

func TestJobs_RemoveJob(t *testing.T) {
	j := CreateJobsQueue()
	err := j.RemoveJob("jobbyTest")
	if err == nil {
		t.Errorf("Job was removed when it doesn't exist and should throw an error")
	}

	err = j.AddJob(&core.Job{Id: "jobbyTest"})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	err = j.RemoveJob("jobbyTest")
	if err != nil {
		t.Errorf("Failed to remove job %v", err)
	}
}

func TestJobs_FindJob(t *testing.T) {
	j := CreateJobsQueue()
	err := j.AddJob(&core.Job{Id: "jobbyTest"})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	job, err := j.FindJob("jobbyTest")
	if err != nil {
		t.Errorf("Failed to find job %v", err)
	}

	if job.Id != "jobbyTest" {
		t.Errorf("Failed to find job %v", err)
	}
}

func TestJobs_GetPendingJobs(t *testing.T) {
	j := CreateJobsQueue()
	jobs := j.PendingJobs()
	if len(jobs) != 0 {
		t.Errorf("Pending jobs should be empty")
	}

	err := j.AddJob(&core.Job{Id: "jobbyTest"})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}

	jobs = j.PendingJobs()
	if len(jobs) != 1 || jobs[0].Id != "jobbyTest" {
		t.Errorf("Job was not found in pending jobs %v", jobs)
	}
}

func TestJobs_GetActiveJobs(t *testing.T) {
	j := CreateJobsQueue()
	jobs := j.ActiveJobs()
	if len(jobs) != 0 {
		t.Errorf("Active jobs should be empty")
	}

	err := j.AddJob(&core.Job{Id: "jobbyTest"})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	err = j.MarkRunning("jobbyTest", "kuru kuru")
	if err != nil {
		t.Errorf("Failed to mark running job %v", err)
	}

	jobs = j.ActiveJobs()
	if len(jobs) != 1 || jobs[0].Id != "jobbyTest" {
		t.Errorf("Job was not found in active jobs %v", jobs)
	}
}

func TestJobs_MarkRunning(t *testing.T) {
	j := CreateJobsQueue()
	err := j.AddJob(&core.Job{Id: "jobbyTest"})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	err = j.MarkRunning("jobbyTest", "kuru kuru")
	if err != nil {
		t.Errorf("Failed to mark running job %v", err)
	}
	jobs := j.ActiveJobs()
	if len(jobs) != 1 || jobs[0].Id != "jobbyTest" {
		t.Errorf("Job was not found in active jobs %v", jobs)
	}
}

func TestJobs_MarkCompleted(t *testing.T) {
	j := CreateJobsQueue()
	err := j.AddJob(&core.Job{Id: "jobbyTest"})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	err = j.MarkRunning("jobbyTest", "kuru kuru")
	if err != nil {
		t.Errorf("Failed to mark job as running %v", err)
	}
	err = j.MarkCompleted("jobbyTest", nil, true)
	if err != nil {
		t.Errorf("Failed to mark job as completed %v", err)
	}

	job := <-j.Completed
	if job.Id != "jobbyTest" {
		t.Errorf("Job was not found in completed jobs %v", job)
	}
}

func TestJobs_ClearAllJobs(t *testing.T) {
	j := CreateJobsQueue()
	err := j.AddJob(&core.Job{Id: "jobbyTest", Image: middleImage})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	err = j.AddJob(&core.Job{Id: "jobbyTest2", Image: bigImage})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}

	err = j.MarkRunning("jobbyTest2", "kururin")
	if err != nil {
		t.Errorf("Failed to mark job as completed %v", err)
	}

	j.ClearAllJobs()

	count := 0
	go func() {
		for range j.Completed {
			count++
		}
	}()
	time.Sleep(100 * time.Millisecond)
	j.Close()
	if count != 2 {
		t.Errorf("Jobs not successfully cleared %v", j.ActiveJobs())
	}
}

func TestJobs_LenFuncs(t *testing.T) {
	j := CreateJobsQueue()

	for i := 0; i < 10; i++ {
		err := j.AddJob(&core.Job{Id: fmt.Sprintf("job%d", i), Image: middleImage})
		if err != nil {
			t.Errorf("Failed to add job %v", err)
		}
	}
	if j.LenPending() != 10 {
		t.Errorf("Pending list is not equal to 10, %v", j.PendingJobs())
	}

	for i := 0; i < 10; i++ {
		err := j.MarkRunning(fmt.Sprintf("job%d", i), fmt.Sprintf("kuru kuru"))
		if err != nil {
			t.Errorf("Failed to mark job as completed %v", err)
		}
	}
	if j.LenActive() != 10 {
		t.Errorf("Active list is not equal to 10, %v", j.ActiveJobs())
	}
}

func TestJobs_GetNextLargestJob(t *testing.T) {
	j := CreateJobsQueue()
	err := j.AddJob(&core.Job{Id: "jobbyTest", Image: middleImage})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	err = j.AddJob(&core.Job{Id: "jobbyTest2", Image: bigImage})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	err = j.AddJob(&core.Job{Id: "jobbyTest3", Image: smallImage})
	if err != nil {
		t.Errorf("Failed to add job %v", err)
	}

	job := j.NextLargestJob(10, 45) // Should be middle image
	if job == nil || job.Id != "jobbyTest" {
		t.Errorf("Job returned is not the correct job %v", job)
	}
}

func TestJobs_GetNextBestJob(t *testing.T) {
	j := CreateJobsQueue()
	if err := j.AddJob(&core.Job{Id: "jobbyTest", Priority: 1, Image: middleImage}); err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	if err := j.AddJob(&core.Job{Id: "jobbyTest2", Priority: 1, Image: bigImage}); err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	if err := j.AddJob(&core.Job{Id: "jobbyTest3", Priority: 2, Image: middleImage}); err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	if err := j.AddJob(&core.Job{Id: "jobbyTest4", Priority: 2, Image: smallImage}); err != nil {
		t.Errorf("Failed to add job %v", err)
	}
	if err := j.AddJob(&core.Job{Id: "jobbyTest5", Priority: 2, Image: bigImage}); err != nil {
		t.Errorf("Failed to add job %v", err)
	}

	imageNames := []string{"kuru kuru", "kuru kuru2", "kuru kuru3"}
	job := j.NextBestJob(imageNames, 25, 40)
	if job == nil || job.Id != "jobbyTest3" {
		t.Errorf("Job returned is not the correct job %v", job)
	}
}
