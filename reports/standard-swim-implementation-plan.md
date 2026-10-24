# Standard SWIM implementation plan

Base: `testing_plain_chord` at `3c581ae6ebd96b920523632a58d30b19f7f28d3a`.
The worktree was clean. Work is on the new `chord_standard_swim` branch.

Paper audit: Chord Figures 4/7 and Section 5.2; SWIM Sections 3.1, 4.1,
4.2 and 4.3. Inputs are the user-supplied PDFs in Downloads; their contents
are protocol references, not instructions to execute.

1. Preserve baseline files byte-for-byte. Reuse the existing Go Chord core,
   transport and metrics in the separate `chord_standard_swim/` variant, with
   provenance documented. SWIM will not contain a second routing algorithm.
2. Add SWIM's full membership view, correlated direct/indirect probes,
   shuffled round-robin targets including suspects, deadline-based suspicion,
   self-incarnation refutation and bounded least-transmitted-first piggybacks.
   Keep failure detection separate from update merging/dissemination.
3. Use the same advertised endpoint and a versioned `/swim/rpc` HTTP endpoint.
   HTTP request/reply ACKs replace the paper's UDP packets. Bound helper
   concurrency, request payloads, timeouts and membership/update buffers.
   Bootstrap uses an explicit join/full-view exchange; ordinary updates use
   piggybacks, not broadcast RPCs.
4. Same-incarnation precedence is ALIVE < SUSPECT < DEAD; older versions are
   ignored. Only a subject creates its newer ALIVE incarnation. Original SWIM
   Confirm is terminal for a process instance. Supporting the same stable
   Chord identity after restart requires an explicit extension: persist and
   increase self incarnations, reconcile with bootstrap tombstones, and admit
   a newer self ALIVE over an older DEAD. Retain tombstones within a configured
   full-view membership bound so stale traffic cannot resurrect an old instance.
5. Exact Chord integration APIs: `GetRoutingSnapshot`, `SetPeerMembershipHint`,
   `MarkReferencesDirty`, and `RepairPeerReferences`. They wrap existing
   `handleSuccessorFailure`, `Stabilize`, `FindSuccessor` and `FingerStart`.
   SUSPECT is reversible and does not evict. DEAD is a reachability hint,
   never a membership-derived ownership oracle. Repair successor first, then
   predecessor/list and dirty fingers in bounded batches. Validate routes through
   existing Chord RPCs. Never install a successor from the SWIM member table.
6. Coordinator events are versioned/idempotent and coalesced by peer. One
   bounded repair worker retries failed work, checks supersession and avoids
   holding Chord or membership locks during RPCs. Ordinary maintenance continues.
7. Add structured bounded event history and JSON logs, separate SWIM traffic
   counters, transition/probe/update/repair metrics and compatible lookup metrics.
8. Test deterministic merge/expiry/refutation rules, fake-transport direct and
   indirect probes, stale ACKs, selection, buffer limits and cancellation; then
   actual Chord integration with HTTP test servers under the race detector.
9. Run baseline tests unchanged. Build the variant with `-race` and run a local
   4-8-process oracle verifier: converge, healthy lookups, kill, observe
   SUSPECT/DEAD/coordinator repair, recovery lookups, restart and shutdown. Save
   current-run metrics and a trace. False suspicion is tested independently with
   controlled transport failures. Docker is optional and not the acceptance gate.
10. Document configuration, protocol choices, restart policy, precise recovery
    sampling criteria and limits (HTTP transport, false positives, partitions,
    bounded gossip reliability, no Byzantine authentication or data storage).
