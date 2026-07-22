# Not A Hivemind
![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)
![Docker](https://img.shields.io/badge/Docker-Supported-2496ED?logo=docker)
![Status](https://img.shields.io/badge/Status-Active_Development-success)

This is an implementation of a distributed cluster manager designed to manage containerized workloads across multiple machines. The masterless design philosophy allows for high availability and scalability, while simplifying network communication (and the amount of code needed to maintain effectiveness).

Powered by a custom Raft engine, which SHOULD NOT be used for production environments. If you are looking for a production-ready distributed workload management system, I recommend looking into [Kubernetes](https://kubernetes.io/) or [Docker Swarm](https://docs.docker.com/engine/swarm/).

Loosely based on the design of [Borg](https://research.google/pubs/pub43438/).

## Architecture
```
                                      +-------------------+
                                      |                   |
                                      |  Raft Consensus   |
                                      |      Engine       |
                                      |                   |
                                      +---------+---------+
                                                ^
                                                |
                                  State Commits & Log Replication
                                                |
            +-----------------------------------+-----------------------------------+
            |                                                                       |
            v                                                                       v
  +-------------------+                                                   +-------------------+
  |                   |                  Job Forwarding                   |                   |
  |  Local Scheduler  |<=================================================>| Local Scheduler 2 |
  |      (TODO)       |                     & Gossip                      |     (TODO)        |
  +---------+---------+                                                   +---------+---------+
  ^                                                                       ^
  | Deploy & Monitor                                                      | Deploy & Monitor
  | Tasks                                                                 | Tasks
  v                                                                       v
  +-------------------+                                                   +-------------------+
  |                   |                                                   |                   |
  |   Local Clusters  |                                                   |  Local Clusters 2 |
  |                   |                                                   |                   |
  +-------------------+                                                   +-------------------+
```
Rather than schedulers electing a leader, they are designed to work with a gossip protocol. Sometimes, a scheduler may be the first to receive a job, and it will forward it to other schedulers to find a suitable cluster to run it on. The Raft engine is used to maintain consensus between schedulers, so that a job is not run multiple times when it shouldn't be. In this sense, the Raft engine is a commit log to maintain state and to reconstruct dead schedulers.

## Roadmap
- [ ] Core system completion (Target: Early August 2026)
    - [ ] Local Scheduler implementation for task deployment & commits
      - [ ] Local Worker/Cluster upgrades to match scheduler requirements
    - [x] Custom Raft consensus implementation
    - [x] Initial worker node stress testing and recovery handling
- [ ] 12-month rigorous testing and optimization phase for distributed workloads
    - [ ] Custom Testing Software
    - [ ] Stress Testing
