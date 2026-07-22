package core

// Role defines the current Raft consensus role of the cluster.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// String implements the fmt.Stringer interface.
func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}
