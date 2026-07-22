package config

import (
	"ClusterManager/internal/environment"
)

type NodeConfig struct {
	NodeID                string           `json:"node_id"`
	NodePort              string           `json:"node_port"`
	NodeAddress           string           `json:"node_address"`
	ConsensusLogDirectory string           `json:"consensus_log_directory"`
	LogDirectory          string           `json:"log_directory"`
	LogVerbosity          int              `json:"log_verbosity"`
	LogOutputToStdErr     string           `json:"log_output_location"`
	Peers                 []PeerNodeConfig `json:"peers"`
}

type PeerNodeConfig struct {
	NodeID      string `json:"node_id"`
	NodeAddress string `json:"node_address"`
}

func ImportEnvironment(configFile string) (*NodeConfig, error) {
	config, err := environment.ImportFile[NodeConfig](configFile)
	if err != nil {
		return nil, err
	}
	return config, nil
}
