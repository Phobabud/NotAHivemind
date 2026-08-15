package states

import (
	"context"
	"sort"
	"time"
)

type SchedulerState struct {
	ID            string
	ClusterStates map[string]*ClusterState
	LastUpdated   time.Time
}

type ClusterState struct {
	ID                string
	UsedCpu           int64
	UsedMem           int64
	MaxCpu            int64
	MaxMem            int64
	OptimisticUpdates int16
	LastUpdated       time.Time
}

func (s *State) AddLocalCluster(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters[id] = &ClusterState{
		ID:                id,
		UsedCpu:           0,
		UsedMem:           0,
		MaxCpu:            0,
		MaxMem:            0,
		OptimisticUpdates: 0,
		LastUpdated:       time.Now(),
	}
}

func (s *State) LocalClusters() []*ClusterState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	clusters := make([]*ClusterState, 0, len(s.clusters))
	for _, cluster := range s.clusters {
		clusters = append(clusters, cluster)
	}
	return clusters
}

func (s *State) LocalCluster(id string) *ClusterState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cluster, ok := s.clusters[id]
	if !ok {
		return nil
	}
	return cluster
}

// SoftUpdateCluster updates the cluster's resource usage optimistically, which isn't the actual cluster's state
func (s *State) SoftUpdateCluster(clusterID string, addCpu, addMem int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cluster, exists := s.clusters[clusterID]; exists {
		cluster.UsedCpu += addCpu
		cluster.UsedMem += addMem
		cluster.OptimisticUpdates++
	}
}

func (s *State) UpdateCluster(clusterID string, usedCpu, usedMem, maxCpu, maxMem int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters[clusterID] = &ClusterState{
		ID:                clusterID,
		UsedCpu:           usedCpu,
		UsedMem:           usedMem,
		MaxCpu:            maxCpu,
		MaxMem:            maxMem,
		OptimisticUpdates: int16(0),
		LastUpdated:       time.Now(),
	}
}

func (s *State) LeastUtilizedCluster(cpuReq, memReq int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	leastUsedID := ""
	leastUsedCpu := int64(-1)
	for id, cs := range s.clusters {
		cpuDiff := cs.MaxCpu - cs.UsedCpu
		memDiff := cs.MaxMem - cs.UsedMem
		if cpuDiff > cpuReq && memDiff > memReq && cpuDiff > leastUsedCpu {
			leastUsedCpu = cpuDiff
			leastUsedID = id
		}
	}
	return leastUsedID
}

func (s *State) StaleClusters(heartbeatTimeout time.Duration) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stale []string
	for id, cluster := range s.clusters {
		if time.Since(cluster.LastUpdated) > heartbeatTimeout {
			stale = append(stale, id)
		}
	}
	return stale
}

func (s *State) ForEachPeer(ctx context.Context, op func(c context.Context, state *SchedulerState) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, peer := range s.peers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := op(ctx, peer); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *State) AddPeerScheduler(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers = append(s.peers, &SchedulerState{
		ID:            id,
		ClusterStates: make(map[string]*ClusterState),
		LastUpdated:   time.Now(),
	})
}

func (s *State) PeerSchedulerState(id string) *SchedulerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, peer := range s.peers {
		if peer.ID == id {
			return peer
		}
	}
	return nil
}

func (s *State) UpdateSchedulerState(schedulerID string, clusterStates map[string]*ClusterState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, peer := range s.peers {
		if peer.ID == schedulerID {
			peer.LastUpdated = time.Now()

			peer.ClusterStates = clusterStates
			return true
		}
	}
	return false
}

func (s *State) StaleSchedulers(heartbeatTimeout time.Duration) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stale []string
	for _, peer := range s.peers {
		if time.Since(peer.LastUpdated) > heartbeatTimeout {
			stale = append(stale, peer.ID)
		}
	}
	return stale
}

func (s *State) LeastUtilizedPeer(heartbeatTimeout time.Duration, cpuRequirements, memRequirements int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	mostOpenId := ""
	greatestCPUDiff := int64(-1)
	for _, peer := range s.peers {
		if time.Since(peer.LastUpdated) > heartbeatTimeout {
			continue
		}

		for id, clusterState := range peer.ClusterStates {
			cpuDiff := clusterState.MaxCpu - clusterState.UsedCpu
			memDiff := clusterState.MaxMem - clusterState.UsedMem
			if cpuDiff >= cpuRequirements && memDiff >= memRequirements && cpuDiff > greatestCPUDiff {
				greatestCPUDiff = cpuDiff
				mostOpenId = id
			}
		}
	}
	return mostOpenId
}

func (s *State) TopFreePeers(limit int, heartbeatTimeout time.Duration, cpuReq, memReq int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Helper struct to map a peer to its best available cluster capacity
	type peerScore struct {
		peerID   string
		availCPU int64
	}
	var candidates []peerScore

	for _, peer := range s.peers {
		if time.Since(peer.LastUpdated) > heartbeatTimeout {
			continue
		}

		var bestAvailCPU int64 = -1

		for _, clusterState := range peer.ClusterStates {
			availCPU := clusterState.MaxCpu - clusterState.UsedCpu
			availMem := clusterState.MaxMem - clusterState.UsedMem

			if availCPU >= cpuReq && availMem >= memReq {
				if availCPU > bestAvailCPU {
					bestAvailCPU = availCPU
				}
			}
		}

		if bestAvailCPU != -1 {
			candidates = append(candidates, peerScore{
				peerID:   peer.ID, // Note: We return the PEER ID, not the cluster ID
				availCPU: bestAvailCPU,
			})
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].availCPU > candidates[j].availCPU
	})

	var results []string
	for i, c := range candidates {
		if i >= limit {
			break
		}
		results = append(results, c.peerID)
	}

	return results
}
