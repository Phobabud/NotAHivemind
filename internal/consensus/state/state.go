package state

import (
	pb "ClusterManager/api/gen/consensus/v1"
	"ClusterManager/internal/consensus/core"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// State manages the thread-safe consensus state of a Raft cluster.
type State struct {
	mu sync.RWMutex

	id       string
	address  string
	leaderID string

	role            core.Role
	connected       bool
	LastContact     time.Time
	ElectionTimeout time.Duration

	client pb.ConsensusCoordinationServiceClient
	conn   *grpc.ClientConn

	// Raft persistent state
	currentTerm int64
	votedFor    string
}

// New creates and initializes a new Raft state as a Follower.
func New(nodeID string, address string) *State {
	s := &State{
		id:      nodeID,
		address: address,
		mu:      sync.RWMutex{},
	}
	s.resetElectionTimer()
	s.RecordContact()
	s.BecomeFollower()
	s.Disconnected()
	return s
}

func (s *State) ID() string {
	return s.id
}

func (s *State) Address() string {
	return s.address
}

// BecomeFollower transitions the cluster to a Follower and resets the election timer.
func (s *State) BecomeFollower() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.role = core.Follower
	s.leaderID = ""
	s.resetElectionTimer()
}

// BecomeCandidate transitions the cluster to a Candidate and resets the election timer.
func (s *State) BecomeCandidate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.role = core.Candidate
	s.leaderID = ""
	s.resetElectionTimer()
}

// BecomeLeader transitions the cluster to a Leader.
func (s *State) BecomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.role = core.Leader
	s.ElectionTimeout = 0
}

// resetElectionTimer generates a new random timeout between 150ms and 300ms.
func (s *State) resetElectionTimer() {
	s.LastContact = time.Now()
	s.ElectionTimeout = time.Millisecond * time.Duration(rand.Intn(150)+150)
}

// Role returns the current cluster role safely.
func (s *State) Role() core.Role {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role
}

func (s *State) UpdateLeader(leaderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaderID = leaderID
}

func (s *State) LeaderID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaderID
}

// RecordContact logs contact from a valid leader to prevent elections.
func (s *State) RecordContact() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastContact = time.Now()
}

// IsElectionTimeout determines if the cluster has waited too long without a heartbeat.
func (s *State) IsElectionTimeout() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.role == core.Leader {
		return false
	}

	return time.Since(s.LastContact) > s.ElectionTimeout
}

func (s *State) TimeSinceLastContact() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Since(s.LastContact)
}

func (s *State) Connection() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

func (s *State) Connected() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = true
}

func (s *State) Disconnected() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = false
}

func (s *State) UpdateConnection(conn *grpc.ClientConn, client pb.ConsensusCoordinationServiceClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = conn
	s.client = client
}

func (s *State) Conn() *grpc.ClientConn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conn
}

func (s *State) Client() pb.ConsensusCoordinationServiceClient {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.client
}

// CurrentTerm returns the current cluster term.
func (s *State) CurrentTerm() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

// SetCurrentTerm thread-safely sets the cluster term.
func (s *State) SetCurrentTerm(term int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
}

// VotedFor returns the candidate ID voted for in the current term.
func (s *State) VotedFor() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.votedFor
}

// SetVotedFor updates the candidate ID voted for in the current term.
func (s *State) SetVotedFor(candidateID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = candidateID
}
