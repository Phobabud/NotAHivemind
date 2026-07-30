package config

import (
	"ClusterManager/internal/cluster/core"
	"ClusterManager/internal/environment"
)

type Machine struct {
	MachineName        string `json:"machine_name"`
	MachineDescription string `json:"machine_description"`
	SchedulerPort      string `json:"scheduler_port"`
	LogDirectory       string `json:"log_directory"`
	LogVerbosity       int    `json:"log_verbosity"`
	JobPayloadDir      string `json:"job_payload_dir"`
	Limits             *Limits
	Images             Images
}

type Limits struct {
	MaxCPULimit    int64 `json:"max_cpu_limit"`    //Unit is in NanoCPUs, 1 CPU = 1,000,000,000 NanoCPUs
	MaxMemoryLimit int64 `json:"max_memory_limit"` //Unit is in bytes
}

type Images []*core.Image

// ImportEnvironment imports all the different types of configs files needed to run the program
func ImportEnvironment(config string, limitsFile string, imagesDirectory string) (*Machine, error) {
	machine, err := environment.ImportFile[Machine](config)
	if err != nil {
		return nil, err
	}

	limits, err := environment.ImportFile[Limits](limitsFile)
	if err != nil {
		return nil, err
	}
	machine.Limits = limits

	images, err := environment.ImportDirectory[core.Image](imagesDirectory, []string{"example"})
	if err != nil {
		return nil, err
	}
	machine.Images = images

	return machine, nil
}

func (m *Machine) GetMachineLimits() (int64, int64) {
	return m.Limits.MaxCPULimit, m.Limits.MaxMemoryLimit
}

func (m *Machine) GetImages() []*core.Image {
	return m.Images
}
