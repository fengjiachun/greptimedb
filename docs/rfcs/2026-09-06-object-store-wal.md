---
Feature Name: Object Store WAL
Tracking Issue: TBD
Date: 2026-09-06
Author: jeremyhi
---

# Summary

Add an object store backed write-ahead log as a first-class WAL provider, alongside Raft Engine and Kafka. A datanode batches the entries of all its regions into immutable objects under one prefix, creates each object conditionally under a monotonically increasing sequence number, and acknowledges a write only after the object holding it is durable. Recovery lists the objects, rebuilds an in-memory catalog from their footers, and replays each region from its own segments. The provider is standalone-only and marked experimental in this RFC; the persisted metadata and object format are designed so that lifting either restriction later does not require a migration.

# Motivation

GreptimeDB already stores data files in object storage. The WAL is the last component that still needs either local disk with durability guarantees (Raft Engine) or an external cluster (Kafka). Both are awkward for a cloud-native standalone deployment:

- Raft Engine ties the node to a persistent volume. Losing the volume loses unflushed writes; moving the node means moving the volume.
- Kafka is an operational dependency of its own, with topics, retention and a broker fleet to size, and it is disproportionate for a single node.

Object storage is already provisioned, already durable across zones, and already the place the rest of the data lives. An object store WAL lets a standalone node run with no local state that must survive a restart: on a new machine, point it at the same bucket and prefix and it replays. The cost is write latency in the hundreds of milliseconds, which is acceptable for the ingestion patterns GreptimeDB targets when writes are batched, and which the design keeps bounded by a single flush interval plus one PUT.

# Goals

- A WAL provider that needs nothing but an object store and a prefix.
- Exactly-once replay of acknowledged writes across graceful and ungraceful restarts.
- Region isolation: many regions share objects, but a region only ever replays its own entries.
- Recovery that never silently loses data: structural damage or a conflicting object stops the node; a corrupted segment is skipped at segment granularity only when configured to, and the affected region is marked and counted.
- Default behaviour of the existing providers unchanged; the new provider is opt-in through configuration.

# Non-goals

- Distributed mode. The provider is rejected when a datanode is controlled by a metasrv; the metasrv side has no allocation logic for it yet.
- Garbage collection of WAL objects, and persistence of per-region obsolete watermarks. Both are follow-ups; the design leaves room for them.
- Matching Raft Engine's write latency. The provider trades latency for the absence of local durable state.

# Details

## Identity

The provider is added to the four parallel WAL enums so that every resolution path handles it explicitly:

| Enum | Variant | Notes |
| --- | --- | --- |
| `DatanodeWalConfig` | `ObjectStore(ObjectStoreWalConfig)` | serde tag `experimental_object_store`; fields `storage_provider`, `prefix`, `flush_interval`, `max_batch_bytes` |
| `WalOptions` | `ObjectStore(ObjectStoreWalOptions { prefix })` | persisted per region with the stable tag `object_store`; flat key `wal.object_store.prefix` |
| `WalProvider` | `ObjectStore { prefix }` | allocates the per-region options in standalone; `start` is a no-op |
| `Provider` | `ObjectStore(Arc<ObjectStoreProvider { region_id, prefix }>)` | `is_remote_wal()` is true |

Two choices are deliberate:

- The persisted `WalOptions` carry the prefix. On reopen the log store compares the persisted prefix with the process configuration and fails fast on a mismatch, so an operator who changes the prefix cannot silently point a region at an empty WAL. Changing the prefix of an existing region requires an explicit migration, which is out of scope here.
- `Provider::ObjectStore` is classified as a remote WAL. Mito's remote-WAL branches (initial flushed entry id from `latest_entry_id`, `topic_latest_entry_id` after replay, region-scoped reads, no topic grouping on batch open) apply unchanged. This is a classification of replay semantics, not of physical location.

`DatanodeWalConfig::ObjectStore` cannot be converted into `MetasrvWalConfig`; that conversion failing is what rejects the provider in distributed mode. The standalone bootstrap matches the datanode WAL configuration before the metasrv conversion and constructs `WalProvider::ObjectStore` directly.

## Object format

Every object holds one sealed batch:

```text
header | segment(region 1) | ... | segment(region N) | footer | trailer
```

- The header carries the magic `GTWALOBJ`, format version 1, the object sequence number and a 16-byte writer instance id (diagnostic only).
- Each segment holds the entries of exactly one region, ordered by entry id; segments are ordered by region id. Ordering makes the encoding deterministic, which the identical-retry rule below depends on.
- The footer records, per segment, the region id, the entry id range, the entry count, the byte range and a CRC32 of the segment.
- The fixed-size trailer carries the magic `GTWALTRL`, the footer location, a CRC32 of the footer and a CRC32 of the whole object. A reader locates the footer by reading the trailer at the end of the object and can decode a single region's segment without touching the others.

Corruption is handled at two levels. Structural damage, meaning a trailer or footer that cannot be decoded, a version the reader does not know, or a header sequence that does not match the key, fails recovery: nothing can be trusted about such an object and continuing would only widen the damage. A segment whose CRC does not match is a data-level failure confined to one region; a read is retried once to rule out a truncated download, and then the behaviour follows `on_corrupted_segment`: `skip` (the default) skips that segment, records a WAL hole for the region with the object key and entry range, increments a metric and logs a warning, while every other region in the object replays normally; `fail` stops the node. Skipping loses only the unflushed entries of that region in that object; nothing already flushed is affected, and the loss is reported rather than hidden. The format is frozen at version 1; a compatibility fixture will pin it.

Object keys are the prefix followed by the zero-padded sequence number and a fixed suffix. Listing accepts only well-formed keys and returns them in sequence order.

## Catalog

The catalog is an in-memory index rebuilt at recovery: objects by sequence number, and per region the objects that hold its entries with their entry id ranges. Its invariants are enforced on insertion: a region's entry id ranges are strictly increasing along the object sequence, an object holds at most one segment per region, and an already indexed sequence number is rejected, whether or not the footer matches. The next sequence number is the largest indexed one plus one, checked for overflow; gaps in the sequence are tolerated.

## Conditional create

Objects are created with an if-not-exists put. Three outcomes matter:

| Outcome | Meaning |
| --- | --- |
| created | the batch is durable |
| exists with identical content | a retry of a batch that was durable before the client observed it; accepted |
| exists with different content | another writer or a corrupted history owns this sequence; rejected |

Deterministic encoding is what makes the middle row decidable.

## Entry ids

Entry ids are object-sequence-major: the high bits are the sequence number of the object that holds the entry, the low bits are the entry's position among that region's entries inside the object.

```text
entry_id = object_seq << 20 | position_in_object   (position < 2^20, the batch seals earlier otherwise)
```

This keeps two properties at once. Comparing `entry_id >> 20` with an object sequence answers "is everything in this object flushed" without consulting any index, which is what garbage collection and a future cross-node takeover need. And every entry still has a unique, strictly increasing id, so a flush that lands between two appends of the same region that ended up in the same object records exactly which entries it covered; replay from `flushed_entry_id` can never skip an unflushed entry or repeat a flushed one. A region's ids have gaps wherever other regions or other positions took the sequence, exactly as Kafka offsets do, and Mito already tolerates gaps.

`latest_entry_id(provider)` is the highest entry id durable under the prefix for any region, the counterpart of Kafka's topic high watermark; it is what a fresh region takes as its initial flushed entry id so that replay starts after everything that existed when the region was created.

## Log store

`ObjectStoreLogStore` implements the `LogStore` trait for `Provider::ObjectStore`.

**Construction and recovery.** The prefix, flush interval and batch size are validated, then the store lists the prefix, decodes each object, rebuilds the catalog, resumes the sequence number and records the largest accepted entry id per region. The first corrupted or conflicting object fails construction.

**Writing.** One background actor per store admits appended entries into an open batch and assigns entry ids as described under *Entry ids* above. The batch is sealed when it reaches `max_batch_bytes` or when `flush_interval` elapses, encoded, and handed to an uploader; the actor keeps admitting entries into the next batch while uploads are in flight, and up to a small fixed number of uploads may run concurrently. Batches are acknowledged in sequence order: a batch whose object is durable is not acknowledged until every earlier batch is durable too, so a region's history can never acquire a hole in the middle. When an earlier batch fails permanently, it and every later in-flight batch fail together and their sequence numbers roll back.

**Acknowledgement modes.** `ack_mode = "durable"` (the default in this RFC) returns from `append_batch` only after the object holding the entries is durable and indexed, so callers never see an entry id for an entry that is not durable. `ack_mode = "enqueued"` returns as soon as the entries are admitted, with their entry ids already assigned; the object is uploaded in the background. In that mode the recovery point objective is the unpersisted backlog, bounded by `max_unpersisted_bytes` and `max_unpersisted_age`: when either bound is reached, admission stalls until an upload completes; nothing is dropped and nothing is acknowledged early. The durable mode is the default because a write-ahead log is expected to mean durability on return; whether the enqueued mode should become the default is decided on the crash-gate and benchmark results rather than assumed.

**Failure matrix.**

| Situation | Sequence | Waiters of the batch | Store |
| --- | --- | --- | --- |
| create succeeds, or identical retry | advances | acknowledged | healthy |
| transient I/O error | unchanged | fail; entry ids roll back to the durable watermark so a retry writes the same object | healthy |
| conflicting object | unchanged | fail | poisoned |
| encoding or catalog error | unchanged | fail | poisoned |
| object sequence exhausted | unchanged | every pending waiter fails immediately | poisoned |

A poisoned store fails every `LogStore` operation except `stop` with the same terminal error; in particular `obsolete` can no longer move a watermark. Transient errors are surfaced to the caller rather than retried inside the store: the engine already owns write retries, and a store-level retry would only hide the latency.

**Stopping.** `stop` is idempotent and awaits the actor. A create that is already in flight runs to completion and, if it succeeds, acknowledges its now-durable entries; entries that never became durable receive a stopped error, including a batch whose in-flight create fails after stop began. Once stop has begun, queued appends are not admitted and no timer or seal triggered flush starts. Appends after stop, including empty ones, fail with the stopped error.

**Reading.** `read(provider, entry_id)` returns the region's entries with id at least `entry_id`, in order, decoding only that region's segments, and hides entries at or below the region's obsolete watermark. `obsolete` records the watermark in memory; the watermark is re-established after a restart because Mito calls `obsolete` with the manifest's `flushed_entry_id` when it opens a region, so no WAL-owned watermark state is persisted. `obsolete_all` hides the whole region. `latest_entry_id` is not affected by the watermark. Every method taking a provider requires `Provider::ObjectStore` with the store's prefix and the caller's region.

A `testing` cargo feature exposes hooks to wait for admitted appends and to seal the open batch, so engine tests can flush deterministically.

## Mito integration

Mito maps `WalOptions::ObjectStore { prefix }` to `Provider::object_store_provider(region_id, prefix)`; resolving object store options against a Raft Engine or Kafka log store reports the existing incompatible-provider error. Reads go through the region-scoped reader used for Kafka, so a namespace shared by many regions never leaks entries across regions even if a log store returned them. Object store regions are opened individually rather than grouped by topic. After replay, `topic_latest_entry_id` follows the existing remote-WAL rule: it is taken from the store's latest entry id only when the region's memtables are empty, otherwise it stays at the replay start, because that value feeds pruning and must never advance past unflushed data.

## Datanode wiring and lifecycle

Configuration is validated before any store or engine exists: the prefix must be non-empty, relative and free of empty or `..` components; `flush_interval` at least one second; `max_batch_bytes` positive; `storage_provider` empty for the default store or the name of a configured provider, resolved through the object store manager. Failures are `InvalidArguments` naming the field.

`DatanodeBuilder` keeps the store handle from the moment it is created and stops it if a later build step fails. On success the handle moves into `Datanode`, whose shutdown stops the store after the region server. Shutdown of the datanode and of the standalone instance is best-effort and ordered: every step runs, the first error is returned, so a failing region engine does not leave the WAL actor alive.

## Configuration

```toml
[wal]
provider = "experimental_object_store"
# Name of a configured storage provider; empty selects the default store.
storage_provider = ""
prefix = "wal"
flush_interval = "1s"
# Upper bound of one object. Object stores bill per request, and the
# cost-effective request size is 8 to 16 MiB; write latency is governed
# by flush_interval, not by this bound.
max_batch_bytes = "8MB"
# "durable": append returns after the object is durable.
# "enqueued": append returns on admission; the unpersisted backlog is
# bounded by the two limits below and admission stalls at the bound.
ack_mode = "durable"
max_unpersisted_bytes = "64MB"
max_unpersisted_age = "8s"
# "skip": a segment with a bad CRC is skipped and its region is marked.
# "fail": the node refuses to start.
on_corrupted_segment = "skip"
```

`config/config.md` is generated from the example files.

## Compatibility

- `WalOptions::ObjectStore` is a new persisted variant. Existing `raft_engine`, `kafka` and `noop` options decode unchanged; a region's options never change provider implicitly.
- The object format is version 1 and frozen. A version bump is a new decoder, not an in-place change.
- The configuration tag carries `experimental_`; the persisted tag does not, so promoting the provider to stable is a configuration rename with an alias, not a metadata migration.

## Evidence

The implementation on the proof-of-concept branch was accepted on the following evidence (entry ids were still per-region contiguous at that point; the object-sequence-major scheme replaces them without changing the object format):

- Deterministic recovery tests on a real `ObjectStoreLogStore` over an in-memory object store: two regions sharing a prefix with only one flushed, an abrupt drop after an object is durable but before any flush, a prefix mismatch on reopen, reopening on an empty prefix, and two consecutive restarts with writes and a flush between them. Each asserts the concrete flushed, replayed, manifest and latest entry ids and row-level scan equality.
- A standalone round trip against MinIO: five write batches produce five objects; a restart without flushing replays 20 rows from entry 1; after a flush a second restart replays nothing; rows are identical after each restart; restart wall time around 120 ms. The run is reproduced by `scripts/object-store-wal-minio.sh`, whose manifest fails if any measurement is missing.
- Unit coverage of every corruption class, the catalog invariants, the conditional create outcomes, every row of the failure matrix, every stop path including in-flight creates that succeed, fail transiently or conflict after stop began, and entry id exhaustion.
- A process-level crash gate: eight concurrent writers, SIGKILL at a random point of a 2 to 6 second window, restart on the same bucket, five cycles per run with replay accumulating; three runs, every acknowledged row present exactly once, no duplicates, no unacknowledged row surviving. Reproduced by `scripts/object-store-wal-crash-gate.sh`.

# Alternatives

## A generic external provider injected from outside the repository

An earlier approach kept the object store WAL outside the main crates and injected it through an `ExternalProvider` and a `resolve_provider` hook on the `LogStore` trait. It avoided touching the four enums, but every resolution path then had to special-case an opaque provider, distributed rejection had to be enforced by convention, and nothing in the repository could test the backend. Making the provider a first-class variant removed the hook, made every match exhaustive, and let the engine tests run against the real store.

## Reusing the Kafka provider with an object store "topic"

The Kafka code path assumes a broker assigns offsets and that a topic is shared across datanodes. Neither holds here: entry ids are assigned by the datanode, and one prefix belongs to one datanode. Reusing the Kafka provider would have inherited the topic tickers, the failover preconditions and the metasrv allocation logic, all of which are wrong for this design.

## One object per region per flush

Simpler catalog, but the number of requests scales with the number of regions instead of with time, which is the cost trap object stores punish. Batching all regions into one object per interval keeps request volume proportional to the flush interval regardless of region count.

# Drawbacks

- Write latency is roughly `flush_interval` plus one PUT, in the hundreds of milliseconds. Raft Engine on local disk is an order of magnitude faster.
- Every object costs a request; a small `flush_interval` multiplies cost. The default of one second and the 8 MiB bound are a compromise, not a measurement on GreptimeDB workloads yet.
- Without garbage collection, objects accumulate and recovery time grows linearly with their number.
- Object-sequence-major entry ids consume 20 bits per object for positions, which caps a region at about a million entries per object; the batch seals when the cap is reached. Sequence numbers are 44 bits wide as a result, enough for one object per second for half a million years.

# Alignment with cluster mode

Distributed mode is out of scope, but several decisions were taken so that the cluster design can build on this backend without a metadata migration:

- Entry ids are object-sequence-major (see *Entry ids*), so a per-region flush watermark can be compared with an object sequence directly. A cross-node takeover can therefore describe a region's WAL position as a chain of `(node prefix, first sequence, cutover sequence)` segments and replay them in order.
- Both acknowledgement modes are defined here. Cluster mode is expected to run `enqueued` for latency-sensitive workloads; the object layout and the recovery path are identical in both modes.
- Conflicting objects poison the store here because a standalone node has no coordinator. In cluster mode the datanode will carry a metasrv-issued generation in the object header so that a conditional-create conflict can be classified as a stale tail from an earlier generation, an idempotent retry of its own write, or a real second writer to be reported to the metasrv.
- Segment-level skipping with region marking is defined here; cluster mode can route the WAL hole event to the metasrv so that a follower or a takeover knows the region needs a fresh replica rather than a replay.
- Garbage collection derives its watermark from each region's manifest `flushed_entry_id`, which Mito hands to the store on region open; no additional persisted state is introduced.

# Future work

1. Recovery reads: fetch only trailers and footers with bounded concurrency; verify segment CRCs on read; probe the next sequence with a GET instead of listing.
2. Object-sequence-major entry ids, the pipelined uploader with in-order acknowledgement, and the `enqueued` mode with its backlog bounds, in that order, each landing as its own change with the crash gate run in both modes.
3. Segment-level corruption skipping with region marking and metrics.
4. Garbage collection of objects whose every region has been flushed past them, driven by the watermarks Mito re-establishes on open.
5. Metrics for flush latency, object count and size, replay duration; a fault matrix for network errors, unwritable buckets and missing objects.
6. A cost and latency comparison against Raft Engine with `sync_write = true`, to calibrate `flush_interval`, `max_batch_bytes` and the default acknowledgement mode.
7. Distributed mode, which needs metasrv-side allocation, per-datanode prefixes and a metasrv-issued generation in the object header.

# Unresolved questions

- Whether the writer instance id should be used to detect a second writer on the same prefix. Conditional creates already prevent overwrites, but a concurrent writer is only noticed when it wins a sequence number.
- Whether region-level `read` should fetch only the target segment with a range request rather than the whole object.
- Whether garbage collection should also run for regions that are closed on this node but still own objects under its prefix, or whether that is left to a cluster-level janitor.
