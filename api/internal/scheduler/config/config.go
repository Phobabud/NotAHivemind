package config

import "NotAHiveMind/internal/environment"

type SchedulerConfig struct {
	SchedulerID    string       `json:"scheduler_id"`
	SchedulerPort  string       `json:"scheduler_port"`
	LogDirectory   string       `json:"log_directory"`
	PeerSchedulers []PeerConfig `json:"peer_schedulers"`
	Clusters       []PeerConfig `json:"clusters"`
	RaftPorts      []string     `json:"raft_ports"`
}

type PeerConfig struct {
	PeerID   string `json:"peer_id"`
	PeerPort string `json:"peer_port"`
}

func ImportEnvironment(configFile string) (*SchedulerConfig, error) {
	config, err := environment.ImportFile[SchedulerConfig](configFile)
	if err != nil {
		return nil, err
	}
	return config, nil
}
