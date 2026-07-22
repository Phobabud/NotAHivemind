package core

import (
	pb "ClusterManager/api/gen/consensus/v1"
	"time"

	"google.golang.org/grpc"
)

type SelfState interface {
	ID() string
	Address() string

	BecomeFollower()
	BecomeCandidate()
	BecomeLeader()

	Role() Role
	UpdateLeader(string)
	LeaderID() string

	RecordContact()
	IsElectionTimeout() bool

	CurrentTerm() int64
	SetCurrentTerm(int64)
	VotedFor() string
	SetVotedFor(string)
}

type PeerState interface {
	ID() string
	Address() string

	BecomeFollower()
	BecomeLeader()

	Role() Role

	RecordContact()
	TimeSinceLastContact() time.Duration

	Connection() bool
	Connected()
	Disconnected()

	UpdateConnection(*grpc.ClientConn, pb.ConsensusCoordinationServiceClient)
	Conn() *grpc.ClientConn
	Client() pb.ConsensusCoordinationServiceClient
}
