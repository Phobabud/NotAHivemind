# Not A Hivemind
![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)
![gRPC](https://img.shields.io/badge/gRPC-1.79.3-blue?logo=grpc&logoColor=white&labelColor=231f20)
![Docker](https://img.shields.io/badge/Docker-Supported-2496ED?logo=docker)
![Active Testing](https://img.shields.io/badge/status-active--testing-blue)

This is an implementation of a distributed cluster manager designed to manage containerized workloads across multiple machines. The masterless design philosophy allows for high availability and scalability, while simplifying network communication (and the amount of code needed to maintain effectiveness).

Powered by a custom Raft engine, which SHOULD NOT be used for production environments. If you are looking for a production-ready distributed workload management system, I recommend looking into [Kubernetes](https://kubernetes.io/) or [Docker Swarm](https://docs.docker.com/engine/swarm/).

Loosely based on the design of [Borg](https://research.google/pubs/pub43438/).

## Architecture
```
  +----------------------------------------------------------------------------+
  |                                  Clients                                   |
  |                                                                            |
  +-------------------------------------+--------------------------------------+
  |                                                                            |
  |                                                                            |
  |                                                                            |
  |                         +-----------------------+                          |
  |                         |     Raft Consensus    |                          |
  |                         |        Engine         |                          |
  |                         +-----------+-----------+                          |
  |                                     ^                                      |
  |                                     |                                      |
  |                      State Commits & Log Replication                       |
  |                                     |                                      |
  |                                     v                                      |
  |           +-------------------------+--------------------------+           |
  |           ^                                                    ^           |
  |           |                                                    |           |
  v           v                                                    v           v
  +-----------------------+                            +-----------------------+
  |      Scheduler 1      |<==========================>|      Scheduler 2      |
  |                       |      Job Forwarding        |                       |
  +-----------+-----------+         & Gossip           +-----------+-----------+
              ^                                                    ^
       Deploy & Monitor                                     Deploy & Monitor
              v                                                    v
+---------+-------+---------+                        +---------+-------+---------+
|         |       |         |                        |         |       |         |
| Clust A |       | Clust B |                        | Clust C |       | Clust D |
|         |       |         |                        |         |       |         |
+---------+-------+---------+                        +---------+-------+---------+
```

Rather than schedulers electing a leader, they are designed to work with a gossip protocol. Sometimes, a scheduler may be the first to receive a job, and it will forward it to other schedulers to find a suitable cluster to run it on. The Raft engine is used to maintain consensus between schedulers, so that a job is not run multiple times when it shouldn't be. In this sense, the Raft engine is a commit log to maintain state and to reconstruct dead schedulers.

Schedulers have ownership of clusters, so other schedulers cannot deploy jobs to clusters they do not own. This is to prevent a single scheduler from being a bottleneck for the entire system, as well as lean into the structure of servers and simplify communication.

## Roadmap
- [x] Core system completion (~~Target: Early August 2026~~ Completed Late July 2026)
- [x] Core tests, involving node deaths, job failures, data loss, data compaction, and other major fail cases.
- [ ] Documentation and comment updates
  - Quickstart & Installation
- [ ] 12-month rigorous testing and optimization phase for distributed workloads
    - [ ] Custom Testing Software
    - [ ] Stress Testing
