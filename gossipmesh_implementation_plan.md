# GossipMesh Simulation & Benchmarking Design Document

## 1. System Architecture & Directory Structure

To ensure clean experimental boundaries, the project uses three isolated codebases for the node implementations. A central orchestrator manages the Docker containers, injects faults, and collects metrics.

```text
/gossipmesh-benchmark
├── /baseline_chord/          # Case 1: Pure Chord (No SWIM)
│   ├── Dockerfile
│   ├── main.go               # Entry point
│   ├── chord.go              # Ring, finger table, stabilize
│   ├── network.go            # RPC and message passing
│   └── metrics.go            # Logging and telemetry
├── /chord_standard_swim/     # Case 2: Chord + Global SWIM
│   ├── Dockerfile
│   ├── main.go 
│   ├── chord.go
│   ├── swim.go               # Global O(N) membership tracking
│   ├── controller.go         # GossipMesh event interceptor
│   ├── network.go
│   └── metrics.go
├── /chord_optimized_swim/    # Case 3: Chord + Scoped SWIM
│   ├── Dockerfile
│   ├── main.go
│   ├── chord.go
│   ├── swim.go               # Topology-aware O(log N) tracking
│   ├── controller.go         
│   ├── network.go
│   └── metrics.go
├── /orchestrator/            # Central Python/Go script
│   ├── runner.py             # Spawns containers, triggers faults
│   └── docker-compose.yml    # Network definition
└── /metrics/                 # Output CSVs and charts
```

---

## 2. Modular File Breakdown & Required Functions

Each implementation folder shares a common architectural pattern. Here is the detailed logic for each file:

### `main.go` (Entry Point)
*   **Purpose:** Reads environment variables (Node ID, bootstrap IP, mode), initializes all sub-modules, and starts background goroutines (event loops).
*   **Functions:** `main()`, `InitializeNode()`.

### `network.go` (RPC & Simulation Layer)
*   **Purpose:** Handles all UDP/TCP communications. Abstracts the network so faults (packet drops, delays) can be simulated easily.
*   **Functions:** 
    *   `StartListener(port)`: Listens for incoming UDP/TCP packets.
    *   `SendChordRPC(target, message)`: Synchronous/Asynchronous overlay messages.
    *   `SendSWIMMessage(target, message)`: Probes and gossip piggybacking.

### `chord.go` (DHT Layer)
*   **Purpose:** Core DHT routing, key mapping, and periodic maintenance.
*   **Functions:**
    *   `FindSuccessor(key)`: Greedy $O(\log N)$ lookup using the finger table.
    *   `ClosestPrecedingNode(key)`: Helper for routing jumps.
    *   `Stabilize()`: Periodic check of the immediate successor.
    *   `FixFingers()`: Periodic refresh of a random finger entry.
    *   `Join(bootstrap_node)`: Ring entry logic.

### `swim.go` (Membership Layer - Cases 2 & 3)
*   **Purpose:** Failure detection and gossip dissemination.
*   **Functions:**
    *   `Ping(target)`: Direct health check.
    *   `PingReq(target, helpers)`: Indirect check if `Ping` fails.
    *   `ProcessGossip(updates)`: Merges `SUSPECT`, `DEAD`, `ALIVE` events using incarnation numbers.
    *   `GetNextPingTarget()`: (Logic varies by case—see Section 3).

### `controller.go` (GossipMesh Layer - Cases 2 & 3)
*   **Purpose:** The bridge that translates SWIM events into Chord repairs.
*   **Functions:**
    *   `OnMembershipEvent(event)`: Intercepts SWIM outputs.
    *   `MarkReferencesDirty(node_id)`: Flags routing entries to bypass timeouts.
    *   `RepairSuccessor()`: Promotes a valid backup from the successor list.
    *   `RepairFinger(finger_index)`: Forces a targeted `FindSuccessor` for a broken finger.

### `metrics.go` (Telemetry)
*   **Purpose:** Safely logs events with simulated timestamps for the orchestrator.
*   **Functions:** `LogLookup(latency, success)`, `LogRepair(event_type, time)`, `DumpStorageStats()`.

---

## 3. The Three Experimental Cases

### Case 1: Baseline Chord (`/baseline_chord`)
*   **Behavior:** No SWIM or GossipMesh. `controller.go` and `swim.go` do not exist here.
*   **Failure Recovery:** Relies 100% on `Stabilize()` and `FixFingers()`. If a lookup hits a dead node, it waits for a network timeout, retries, and logs a high latency penalty.

### Case 2: Standard GossipMesh (`/chord_standard_swim`)
*   **SWIM Scope:** `swim.go` maintains an $O(N)$ list of all nodes. `GetNextPingTarget()` selects uniformly at random from the entire network.
*   **Integration:** `controller.go` actively listens. When SWIM detects a failure anywhere in the network, it triggers proactive repairs in `chord.go` (bypassing timeouts).

### Case 3: Optimized GossipMesh (`/chord_optimized_swim`)
*   **SWIM Scope:** `swim.go` tracks *only* $O(\log N + k)$ nodes. 
*   **Dynamic Tracking:** A new function, `UpdateTrackingSet()`, syncs the SWIM list with Chord. SWIM only probes nodes currently in the immediate neighborhood, the successor list, or the active finger table.
*   **Integration:** Identical rapid repair via `controller.go`, but without the massive $O(N)$ memory and messaging overhead.

---

## 4. Docker & Orchestration Setup

### Dockerfile (Template for each case)
```dockerfile
FROM golang:1.20-alpine
WORKDIR /app
COPY . .
RUN go build -o node .
CMD ["./node"]
```

### Fault Injection via Orchestrator (`runner.py`)
The orchestrator drives the experiment using Docker commands:
*   **Hard Crash:** `docker stop <container_name>`
*   **Gray Failure (Latency Spikes):** 
    ```bash
    docker exec <container_name> tc qdisc add dev eth0 root netem delay 500ms 50ms distribution normal
    ```
*   **Partition:** Using `iptables` to block subnets.

---

## 5. Benchmarking Metrics to Capture

The orchestrator will parse the logs generated by `metrics.go` to evaluate the hypothesis. You must capture:

1.  **Lookup Correctness:** $\%$ of lookups returning the true oracle successor.
2.  **Lookup Latency ($p50, p95, p99$):** Time taken to resolve keys. Expect massive spikes in Case 1 during churn, improved $p95$ in Case 2 and 3.
3.  **Routing Recovery Time:** Seconds elapsed from a node crash until 99% of lookups succeed again.
4.  **Storage Footprint:** Average number of membership records in RAM. (Case 3 must prove it uses significantly fewer records than Case 2).
5.  **Message Overhead:** Network traffic per node (bytes/sec and msgs/sec). Shows if Case 2 causes "repair storms" that Case 3 avoids.