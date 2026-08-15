package models

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type JobID string
type ContainerID string
type RequestID string

const (
	PrefixJob       = "job_"
	PrefixContainer = "con_"
	PrefixRequest   = "req_"
)

func NewJobID() JobID {
	return JobID(fmt.Sprintf("%s%s", PrefixJob, uuid.New().String()))
}

func NewContainerID() ContainerID {
	return ContainerID(fmt.Sprintf("%s%s", PrefixContainer, uuid.New().String()))
}

func NewRequestID() RequestID {
	return RequestID(fmt.Sprintf("%s%s", PrefixRequest, uuid.New().String()))
}

func IsJobID(id string) bool {
	return strings.HasPrefix(id, PrefixJob)
}

func (j JobID) String() string {
	return string(j)
}

func (c ContainerID) String() string {
	return string(c)
}

func (r RequestID) String() string {
	return string(r)
}

func ExtractRawUUID(prefixedID string) (uuid.UUID, error) {
	parts := strings.SplitN(prefixedID, "_", 2)
	if len(parts) != 2 {
		return uuid.Nil, fmt.Errorf("invalid semantic ID format")
	}
	return uuid.Parse(parts[1])
}
