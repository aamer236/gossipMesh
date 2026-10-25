# Experimental owner-local key–value storage

Both Go variants now expose identical additive storage endpoints. Existing Chord routing, SWIM state transitions and adaptive scheduling are unchanged. The prior baseline gains storage endpoints to allow identical storage experiments; previous benchmark measurements remain routing-only results from their recorded binaries.

Start the dashboard with a normal build (do not reuse old binaries):

```sh
python3 -m visualizer.dashboard.server
```

Wait for both rings to become Running. In **Store and retrieve the same record**, choose numeric key ID 80, value `hello GossipMesh`, and initiating node 0. **Put in both rings** sends the same key/value to each variant. Each initiating Go process runs its real `FindSuccessor` and writes through `/rpc/kv` to the resulting owner. **Get from both rings** fetches from the current Chord owner; it does not return the Python operation history as data. Choose another surviving initiating node to demonstrate routed retrieval. The UI shows the actual responding owner, value and HTTP status for each ring. Export retains up to 40 manual storage operations.

In a converged four-node ring (0,64,128,192), key 80 should be stored on node 128. After node 128 crashes, routing may recover to node 192, but this unreplicated value will be missing. Restarting node 128 also starts an empty store. A correct route plus a missing value is **not** successful data recovery.

## Direct API

Replace PORT with any real node's listening or advertised proxy port:

```sh
curl -X PUT 'http://127.0.0.1:PORT/kv?id=80' \
  -H 'Content-Type: application/json' -d '{"value":"hello GossipMesh"}'
curl 'http://127.0.0.1:PORT/kv?id=80'
```

Keys are numeric Chord IDs in `[0,2^M)`; these are not automatically hashed string keys. Values are strings. Empty strings are valid. PUT creates or overwrites the record. GET returns JSON with `version`, `key`, `owner`, `address`, `found`, and `value`. Missing records return HTTP 404 (`found:false`); ownership or capacity rejection returns 409; routing/transport failure returns 503; invalid input returns 400/413. Requests and RPCs have bounded sizes, and peer forwarding uses the existing transport timeout and message metrics. A timed-out write can have succeeded at its owner; retrying the same key/value is safe, but a timeout alone does not establish whether it committed.

Each transport has its own synchronized map; nodes and variants do not share a store. A local ownership check is performed under the Chord routing read lock before access, and the map has its own mutex. No routing or storage lock spans network I/O. The owner response's version, key, ID and address are checked by the initiating node. This is a local consistency guard, not a global correctness proof under churn. Each node accepts at most 4,096 keys and 64 KiB UTF-8 per value. Dashboard JSON controls additionally have their existing 64 KiB encoded request bound; large escaped values may require the direct node API.

## Explicit scope

This is an experimental in-memory storage layer, **not a complete replicated DHT**:

- No disk persistence, replicas, replica repair or crash-surviving data availability.
- No automatic key migration during joins or ownership changes. Records can remain stranded on former owners; GET queries only the current owner. Populate records after convergence and avoid joins when testing normal retrieval.
- No linearizability or distributed transactions across topology changes; local guards cannot establish global ownership during partitions or concurrent joins.
- Storage operations are manual and separate from the continuous ownership-lookup workload. They add real peer traffic and can affect interactive metrics; exported lookup correctness does not become storage correctness.
- Python retains the manual operation history for display/export, but the actual stored values reside in the Go owner processes. The GET result is always fetched from those processes.

Future data-availability comparisons require implementing the same migration, replication and consistency policy for both variants, then measuring retrieval independently of routing convergence.

## Verification commands

```sh
(cd baseline_chord && go test -race ./...)
(cd chord_standard_swim && go test -race ./...)
DASHBOARD_E2E=1 DASHBOARD_TEST_NODES=4 python3 -m unittest visualizer.dashboard.test_dashboard -v
```

Go storage tests cover routed Put/Get via a non-owner, separation between stores, missing keys, invalid and oversized payloads, capacity, wrong-owner rejection, concurrent access, and empty state after restart. The real-process dashboard test writes the same record to both rings, retrieves via another node, and exercises the four-node crash/restart sequence. This does not establish data durability or correctness under arbitrary churn.

## Automatic random preload and per-node inventory

Each new dashboard experiment now waits for routing convergence, then enters **seeding**. A fresh recorded random seed selects `min(128, max(16, 2 * node_count))` distinct numeric keys from 0–255. The same key set and values (`demo-SEED-KEY`) are Put through both rings using matching initiating IDs. Placement follows each ring's actual Chord routing; the controller never writes directly to an oracle-selected owner. Only successful paired initialization advances the dashboard to Running and starts the continuous lookup workload. Cancellation stops further seeding; failed writes produce an experiment error and retained seed results rather than a fabricated successful preload.

**Keys stored by each node** lists actual key IDs returned by the Go nodes' read-only `GET /kv/local` endpoint. **Inspect** opens the selected node's stored values and fingers. Values are previewed to 160 Unicode characters, with original byte length; keys and metadata remain bounded by the node's 4,096-key capacity. Empty stores, unavailable snapshots, and stopped processes are distinct states. The endpoint lists physically stored records, which may differ from current mathematical ownership after joins because migration is not implemented.

The initial seed, complete key/value request manifest, actual owner replies and completion flag are exported under `initial_storage`. The last 40 Put/Get operations remain in the separate operation history. Seeding happens once per experiment, never automatically on a node restart, so lost data is not silently restored from the controller. A random subset need not place records on every node. Initial seed writes add real traffic; their traffic is not evidence of a steady-state performance advantage. Local inventory polling is management traffic and adds observation load.
