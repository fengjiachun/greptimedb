---
Feature Name: Object Store WAL
Tracking Issue: TBD
Date: 2026-09-06
Author: jeremyhi
---

# Summary

Add an object store backed write-ahead log as a first-class WAL provider, alongside Raft Engine and Kafka. A datanode batches the entries of all its regions into immutable objects under one prefix, creates each object conditionally under a monotonically increasing sequence number, and acknowledges a write only after the object holding it is durable. Recovery lists the objects, rebuilds an in-memory catalog from them, and replays each region from its own segments. The provider is standalone-only and marked experimental in this RFC; the persisted metadata and object format are designed so that lifting either restriction later does not require a migration.

This document describes two things and keeps them apart: what the implementation does today, and what is proposed on top of it. Every paragraph that describes proposed behaviour is introduced with the word *Proposed*; everything else describes the current code.

# Motivation

GreptimeDB already stores data files in object storage. The WAL is the last component that still needs either local disk with durability guarantees (Raft Engine) or an external cluster (Kafka). Both are awkward for a cloud-native standalone deployment:

- Raft Engine ties the node to a persistent volume. Losing the volume loses unflushed writes; moving the node means moving the volume.
- Kafka is an operational dependency of its own, with topics, retention and a broker fleet to size, and it is disproportionate for a single node.

Object storage is already provisioned, already durable across zones, and already the place the rest of the data lives. An object store WAL takes the WAL out of the local state that must survive a restart: unflushed writes are no longer tied to a volume, and a node restarted against the same bucket and prefix replays them. It does not yet make a standalone node independent of local disk altogether, because standalone keeps its metadata in a local key-value store under the data home regardless of the WAL provider; moving that metadata off local disk is a separate change. The cost is write latency: a write waits for its batch to seal, for earlier uploads to finish, and for one PUT. That is acceptable for the ingestion patterns GreptimeDB targets when writes are batched.

# Goals

- A WAL provider that needs nothing but an object store and a prefix.
- Exactly-once replay of acknowledged writes across graceful and ungraceful restarts.
- Region isolation: many regions share objects, but a region only ever replays its own entries.
- Recovery that never silently loses data. Today any corruption fails the node. *Proposed*: a corrupted segment is skipped at segment granularity only when configured to, and the affected region is marked and counted.
- Default behaviour of the existing providers unchanged; the new provider is opt-in through configuration.

# Non-goals

- Distributed mode. The provider is rejected when a datanode is controlled by a metasrv; the metasrv side has no allocation logic for it yet.
- Garbage collection of WAL objects, and persistence of per-region obsolete watermarks. Both are follow-ups; the design leaves room for them.
- Matching Raft Engine's write latency. The provider trades latency for the absence of local durable WAL state.
- Making standalone metadata independent of local disk. The metadata store is unchanged by this RFC.

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

A datanode that is configured with a metasrv client rejects the provider before any store is built: `DatanodeBuilder` returns an error stating that the object store WAL is standalone-only. Separately, `DatanodeWalConfig::ObjectStore` cannot be converted into `MetasrvWalConfig`, so a metasrv cannot be configured with it either. The standalone bootstrap matches the datanode WAL configuration before that conversion and constructs `WalProvider::ObjectStore` directly.

## Object format

Every object holds one sealed batch:

```text
header | segment(region 1) | ... | segment(region N) | footer | trailer
```

- The header carries the magic `GTWALOBJ`, format version 1, the object sequence number and a 16-byte writer instance id (diagnostic only).
- Each segment holds the entries of exactly one region, ordered by entry id; segments are ordered by region id. Ordering makes the encoding deterministic, which the identical-retry rule below depends on.
- The footer records, per segment, the region id, the entry id range, the entry count, the byte range and a CRC32 of the segment.
- The fixed-size trailer carries the magic `GTWALTRL`, the footer location, a CRC32 of the footer and a CRC32 of the whole object. A reader locates the footer by reading the trailer at the end of the object and can decode a single region's segment without touching the others.

Any corruption fails recovery today. The decoder verifies the trailer magic, the format version, the header sequence against the key, the footer CRC, that the segment byte ranges tile the object body exactly, every segment CRC and the whole-object CRC; the first object that fails any of these checks fails construction of the store, and a segment that fails its CRC on the read path fails that read. The format is frozen at version 1; a compatibility fixture will pin it.

*Proposed*: corruption is handled at two levels. Structural damage, meaning a trailer, footer or header that cannot be decoded, a version the reader does not know, a header sequence that does not match the key, or segment ranges that do not tile the body, keeps failing recovery: nothing can be trusted about such an object. A segment whose CRC does not match becomes a data-level failure confined to one region: the read is retried once to rule out a truncated download, then the behaviour follows `on_corrupted_segment`. `skip` (the proposed default) skips that segment, records a WAL hole for the region with the object key and entry range, increments a metric and logs a warning, while every other region in the object replays normally; `fail` stops the node. Skipping loses only the unflushed entries of that region in that object; nothing already flushed is affected, and the loss is reported rather than hidden. Under this proposal the whole-object CRC is no longer a recovery requirement, since a bad segment would fail it by construction; the footer CRC and the per-segment CRCs carry the verification, and the whole-object CRC stays in the trailer as a fixture-level check so the format does not change.

Object keys are `<prefix>/objects/<sequence>.wal`, where the sequence is zero-padded to 20 decimal digits; with prefix `wal` the first object is `wal/objects/00000000000000000000.wal`. Listing accepts only well-formed keys under `<prefix>/objects/` and returns them in sequence order.

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

Entry ids are assigned per region and are contiguous: when a batch admits entries of a region, each entry receives the region's previous id plus one, independently of the object sequence. A region's ids therefore have no gaps, and the object sequence cannot be derived from an entry id without the catalog.

`latest_entry_id(provider)` is region-scoped: the highest entry id durable under the prefix for the provider's region, or zero when the region has none. A freshly created region therefore starts at zero even if other regions under the prefix have entries. This differs from Kafka, whose high watermark is per topic; it is sufficient here because ids are per region, and Mito consumes the value only for the region that asked.

*Proposed*: entry ids become object-sequence-major. The high bits are the sequence number of the object that holds the entry, the low bits are the entry's position among that region's entries inside the object:

```text
entry_id = object_seq << 20 | position_in_object   (1 <= position < 2^20, the batch seals earlier otherwise)
```

Positions start at one, so entry id zero is never assigned: Mito treats zero as the watermark of a region that has no durable entry and replays from the next id, and the first object has sequence zero. This keeps two properties at once. `entry_id >> 20` names the object that holds an entry without consulting any index, which is what a future cross-node takeover needs to describe a WAL position as a range of object sequences. Garbage collection does not rely on that shortcut: whether an object is fully flushed is decided per segment from the footer, an object is deletable only when every segment's maximum entry id is at or below its region's flushed watermark, and the comparison is on the full id, so a watermark that stops in the middle of an object keeps the object. The rule is the same for objects written under either id scheme. And every entry still has a unique, strictly increasing id, so a flush that lands between two appends of the same region that ended up in the same object records exactly which entries it covered; replay from `flushed_entry_id` can never skip an unflushed entry or repeat a flushed one. A region's ids then have gaps wherever other regions or other positions took the sequence, exactly as Kafka offsets do, and Mito already tolerates gaps. The proposal does not change the object layout. It changes the entry id values that are assigned, and those values are encoded in two places that must agree: in each record inside its segment and in the footer's entry id range for that segment. `latest_entry_id` stays region-scoped.

*Proposed*: three rules make the scheme safe on a prefix that already holds objects and on a prefix that garbage collection has trimmed. On open, the next object sequence is the largest indexed sequence plus one, raised further if any region's largest existing entry id is at or above `next_seq << 20`, so that every new id is greater than every existing id of its region and the catalog's strictly increasing ranges hold; sequence gaps are already tolerated. Objects written under the contiguous scheme are not rewritten; they remain readable, their ids carry no object information, and only the footer-based rule above may reason about them. Garbage collection never deletes the object with the highest sequence number, so the sequence always resumes above everything that was ever written and no id is ever assigned twice. The raised sequence exists only in memory until an object is written at it, so on a prefix that still holds objects written under the contiguous scheme garbage collection deletes nothing until the first object written under the new scheme is durable; from then on that object, or a later one, is the retained highest-sequence object and the anchor is durable. No additional persisted state is introduced.

## Log store

`ObjectStoreLogStore` implements the `LogStore` trait for `Provider::ObjectStore`.

**Construction and recovery.** The prefix, flush interval and batch size are validated, then the store lists the prefix, fetches and decodes each object in sequence order, rebuilds the catalog, resumes the sequence number and records the largest accepted entry id per region. The first corrupted or conflicting object fails construction. Recovery is serial and reads whole objects; fetching only trailers and footers with bounded concurrency is listed under *Future work*.

**Writing.** One background actor per store admits appended entries into a single open batch and assigns entry ids as described under *Entry ids* above. The batch is sealed when its estimated size reaches `max_batch_bytes` or when `flush_interval` elapses. The actor then encodes the batch and awaits its conditional create inline: no further command is admitted until that create has completed. Callers wait on a bounded command channel, so under load an append can queue behind several earlier uploads.

*Proposed*: a pipelined uploader. The actor keeps admitting entries into the next batch while uploads are in flight, and up to a small fixed number of uploads run concurrently. Batches are acknowledged in sequence order: a batch whose object is durable is not acknowledged until every earlier batch is durable too, so the acknowledged prefix of a region's history never has a missing predecessor. When an earlier batch fails permanently, it and every later batch whose object was not created fail together and their sequence numbers roll back. A later object that is already durable cannot be rolled back; in that case the store poisons itself. The guarantee is therefore about acknowledged history only: recovery indexes every object under the prefix, so after such a failure it replays the entries of that later object although the earlier batch is absent. Those entries were never acknowledged, and this is the same outcome as a crash between an object's creation and its acknowledgement, which the engine already tolerates.

**Acknowledgement.** `append_batch` returns only after the object holding the entries is durable and indexed, so callers never see an entry id for an entry that is not durable. This is the only mode implemented.

*Proposed*: an `ack_mode` option. `durable` keeps the behaviour above and stays the default, because a write-ahead log is expected to mean durability on return. `enqueued` returns from `append_batch` as soon as the entries are admitted, with their entry ids already assigned, and uploads the object in the background. In that mode the recovery point objective is the unpersisted backlog. `max_unpersisted_bytes` and `max_unpersisted_age` are admission thresholds: when the backlog reaches either, new appends stall until an upload completes, so nothing is dropped and the backlog cannot grow without limit. They are not a hard bound on what a crash can lose: during an outage the writes already acknowledged stay in memory for as long as the outage lasts, however old they get, and a crash in that window loses them. Its failure contract differs from the durable one: a permanent upload failure can no longer be reported to a caller that has already returned, so it poisons the store, and `stop` uploads the remaining backlog before returning. It also needs a barrier that the durable mode gets for free: an entry id that Mito has been handed may not yet be durable, and a flush that persisted such an id as the manifest's `flushed_entry_id` would, after a crash before the object was created, leave a watermark that refers to an object which never existed, so that the sequence is reused on restart and later entries land below the watermark and are skipped by replay. In `enqueued` mode the store therefore exposes the highest durable entry id per region, and Mito's flush waits until the WAL is durable through the last entry it is about to flush before it writes the manifest; a manifest watermark can then only ever name a durable entry, and the restart allocation rule above keeps every new id above it. This barrier is part of the acceptance criteria for the mode: a crash after an enqueued acknowledgement and a flush attempt, before the object is created, followed by a restart, new writes and a second crash, must replay every acknowledged-and-durable entry. Whether `enqueued` should become the default is decided on the crash-gate and benchmark results rather than assumed.

**Failure matrix.**

| Situation | Sequence | Waiters of the batch | Store |
| --- | --- | --- | --- |
| create succeeds, or identical retry | advances | acknowledged | healthy |
| transient I/O error | unchanged | fail; entry ids roll back to the durable watermark, so the next batch takes the same sequence number | healthy |
| conflicting object | unchanged | fail | poisoned |
| encoding or catalog error | unchanged | fail | poisoned |
| create succeeds at the last representable sequence | cannot advance | acknowledged | poisoned: no later batch can be allocated a sequence, every later append fails |

A store cannot be constructed on a prefix whose largest object already carries the maximum sequence. A poisoned store fails every `LogStore` operation except `stop` with the same terminal error; in particular `obsolete` can no longer move a watermark. Transient errors are not retried inside the store, but a failed create is reconciled once: the store immediately reads the key back, accepts the batch if the object exists with identical bytes, rejects it as a conflict if the bytes differ, and reports the transient error only when the key is absent or that read fails too. So a lost response alone does not fail the append. When the append does fail, neither the store nor Mito keeps the batch: it surfaces through Mito to the caller of the write, and a retry is a new write by the caller, grouped with whatever else is admitted at that time, which may or may not reproduce the same bytes. Such a retry is therefore not idempotent at the request level. If the create had in fact succeeded and both its response and the read-back were lost, the next batch takes the same sequence number with different content, its conditional create conflicts, and the store poisons itself. Reconciling the sequence again before it is reused, after the initial read-back failed, would close this gap and is listed under *Future work*. Retrying the batch itself inside the store would only hide the latency.

**Stopping.** `stop` is idempotent and awaits the actor. A create that is already in flight runs to completion and, if it succeeds, acknowledges its now-durable entries; entries that never became durable receive a stopped error, including a batch whose in-flight create fails after stop began. Once stop has begun, queued appends are not admitted and no timer or seal triggered flush starts. Appends after stop, including empty ones, fail with the stopped error.

**Reading.** `read(provider, entry_id)` returns the region's entries with id at least `entry_id`, in order, decoding only that region's segments, and hides entries at or below the region's obsolete watermark. `obsolete` records the watermark in memory; the watermark is re-established after a restart because Mito calls `obsolete` with the manifest's `flushed_entry_id` when it opens a region, so no WAL-owned watermark state is persisted. `obsolete_all` hides the whole region. `latest_entry_id` is not affected by the watermark. Every method taking a provider requires `Provider::ObjectStore` with the store's prefix and the caller's region.

A `testing` cargo feature exposes hooks to wait for admitted appends and to seal the open batch, so engine tests can flush deterministically.

## Mito integration

Mito maps `WalOptions::ObjectStore { prefix }` to `Provider::object_store_provider(region_id, prefix)`; resolving object store options against a Raft Engine or Kafka log store reports the existing incompatible-provider error. Reads go through the region-scoped reader used for Kafka, so a namespace shared by many regions never leaks entries across regions even if a log store returned them. Object store regions are opened individually rather than grouped by topic. After replay, `topic_latest_entry_id` follows the existing remote-WAL rule: it is taken from the store's latest entry id only when the region's memtables are empty, otherwise it stays at the replay start, because that value feeds pruning and must never advance past unflushed data.

## Datanode wiring and lifecycle

The scalar options are validated before any store or engine exists: the prefix must be non-empty, relative and free of empty or `..` components; `flush_interval` at least one second; `max_batch_bytes` positive. `storage_provider` is resolved later, once the object store manager has been built and before the log store and the Mito engine are: empty selects the default store, otherwise the name must match a configured provider. Failures in both stages are `InvalidArguments` naming the field, and both happen before any WAL object is touched.

`DatanodeBuilder` keeps the store handle from the moment it is created and stops it if a later build step fails. On success the handle moves into `Datanode`, whose shutdown stops the store after the region server. Shutdown of the datanode and of the standalone instance is best-effort and ordered: every step runs, the first error is returned, so a failing region engine does not leave the WAL actor alive.

## Configuration

The options accepted today:

```toml
[wal]
provider = "experimental_object_store"
# Name of a configured storage provider; empty selects the default store.
storage_provider = ""
prefix = "wal"
# How long a batch may stay open before it is sealed; at least 1s.
flush_interval = "1s"
# Estimated batch size at which the open batch is sealed. It is a
# threshold, not an upper bound: a single append larger than this is
# admitted whole, and object framing adds bytes on top. Object stores
# bill per request, and the cost-effective request size is 8 to 16 MiB.
max_batch_bytes = "8MB"
```

`config/config.md` is generated from the example files.

*Proposed* additions, not accepted by the current configuration:

```toml
# "durable": append returns after the object is durable (the default).
# "enqueued": append returns on admission. New appends stall once the
# unpersisted backlog reaches either threshold below; the thresholds do
# not bound what a crash during an outage can lose.
ack_mode = "durable"
max_unpersisted_bytes = "64MB"
max_unpersisted_age = "8s"
# "skip": a segment with a bad CRC is skipped and its region is marked.
# "fail": the node refuses to start.
on_corrupted_segment = "skip"
```

## Compatibility

- `WalOptions::ObjectStore` is a new persisted variant. Existing `raft_engine`, `kafka` and `noop` options decode unchanged; a region's options never change provider implicitly.
- The object format is version 1 and frozen. A version bump is a new decoder, not an in-place change.
- The configuration tag carries `experimental_`; the persisted tag does not, so promoting the provider to stable is a configuration rename with an alias, not a metadata migration.
- *Proposed*: the object-sequence-major id scheme needs no migration of existing objects. A prefix written under the contiguous scheme keeps its objects; the sequence allocation rule under *Entry ids* keeps new ids above the old ones, and garbage collection reasons from footers, not from the id encoding.

## Evidence

The implementation was accepted on the following evidence, all of it reproducible from this repository:

- Deterministic recovery tests on a real `ObjectStoreLogStore` over an in-memory object store: two regions sharing a prefix with only one flushed, an abrupt drop after an object is durable but before any flush, and two consecutive restarts with writes and a flush between them, which together assert the concrete flushed, replayed, manifest and latest entry ids and row-level scan equality; a prefix mismatch on reopen, which asserts the rejection, both prefixes in the message and that the region was not opened; and reopening on an empty prefix, which asserts row counts and a clean open.
- A standalone round trip against MinIO: five write batches produce five objects; a restart without flushing replays 20 rows from entry 1; after a flush a second restart replays nothing; rows are identical after each restart; restart wall time around 120 ms. The run is reproduced by `scripts/object-store-wal-minio.sh`, whose manifest fails if any measurement is missing.
- Unit coverage of every corruption class, the catalog invariants, the conditional create outcomes, every stop path including in-flight creates that succeed, fail transiently or conflict after stop began, per-region entry id exhaustion, and the catalog refusing a next sequence past the maximum. Of the failure matrix, the transient and conflicting rows are exercised through the actor with fault injection; the encoding and catalog rows are covered only at the component level, where the encoder and the catalog reject the input, and the actor's handling of those errors (sequence unchanged, waiters failed, store poisoned) is described from the code; the last row, a successful create at the last representable object sequence followed by poisoning, is likewise described from the code and not exercised by a test.

A process-level crash gate (concurrent writers, SIGKILL at a random point of a window, restart on the same bucket, repeated cycles with replay accumulating) is delivered as a separate change with its own script and manifest. Its results are not part of the evidence above and are reported with that change.

# Alternatives

## A generic external provider injected from outside the repository

The backend could stay outside the main crates and be injected through an opaque provider variant plus a resolution hook on the `LogStore` trait. That avoids touching the four enums, but every resolution path then has to special-case an opaque value, distributed rejection can only be enforced by convention, and nothing in the repository can test the backend. Making the provider an explicit variant keeps every match exhaustive and lets the engine tests run against the real store, at the cost of one more arm in each enum.

## Reusing the Kafka provider with an object store "topic"

The Kafka code path assumes a broker assigns offsets and that a topic is shared across datanodes. Neither holds here: entry ids are assigned by the datanode, and one prefix belongs to one datanode. Reusing the Kafka provider would have inherited the topic tickers, the failover preconditions and the metasrv allocation logic, all of which are wrong for this design.

## One object per region per flush

Simpler catalog, but every region then produces its own timer-driven object, so the request rate scales with the number of regions even when they write little, which is the cost trap object stores punish. Sharing one batch across regions removes that per-region floor: at low volume there is one object per interval regardless of region count, and at high volume the count is governed by aggregate bytes and `max_batch_bytes`, which is the same request volume a single busy region would produce.

# Drawbacks

- Write latency is the time until the batch seals (up to `flush_interval`, one second by default), plus queueing behind earlier uploads, plus one PUT. Raft Engine on local disk is an order of magnitude faster.
- Every object costs a request; a small `flush_interval` multiplies cost. The default of one second and the 8 MiB sealing threshold are a compromise, not a measurement on GreptimeDB workloads yet.
- Without garbage collection, objects accumulate and recovery time grows linearly with their number.
- *Proposed* object-sequence-major entry ids consume 20 bits per object for positions, which caps a region at about a million entries per object; the batch seals when the cap is reached. Sequence numbers are 44 bits wide as a result, enough for one object per second for half a million years.

# Alignment with cluster mode

Distributed mode is out of scope, but several decisions were taken so that the cluster design can build on this backend without a metadata migration. All of them are *Proposed*; none is implemented at this snapshot.

**Key layout.** The operator configures only the prefix root. The store derives the prefix it writes under as `<prefix>/datanodes/<node_id>/epochs/<generation>`, so objects live at `<prefix>/datanodes/<node_id>/epochs/<generation>/objects/<sequence>.wal`. Standalone uses its configured node id and generation zero; in cluster mode the metasrv assigns both, and issues a new generation every time it hands a region's write ownership to a node. The persisted `WalOptions` of a region carry the full derived prefix, so a region's WAL location is self-describing and can be opened read-only by a node other than the one that wrote it. Putting the generation into the key rather than only into the object header means that a writer which lost its lease and keeps writing lands in its old epoch directory, where the new writer never creates objects, so the two can never contend for a sequence number; the generation in the header stays as a read-side check.

**Planned migration.** A region that moves while its source node is alive follows the existing migration flow, and the cheap path applies only when that flow proves the handoff: the source stops admitting writes, flushes, and the resulting manifest watermark covers the last acknowledged entry. Then the target opens the region from that watermark and starts writing under its own prefix, and no cross-node read is needed. If the final flush fails or times out, the fact that the source process is alive proves nothing about its unflushed tail; the migration then either takes the failover path below, with the source's prefix in the chain, or is aborted.

**Failover.** When the source is gone, the target must replay the region's unflushed tail from the source's prefix. The region's WAL position becomes a chain of segments whose last element is the target's own prefix. Each earlier segment is `(prefix, first sequence, cutover sequence, holes)`: the metasrv appends it to the region's `WalOptions` when it reassigns the region, after the source's lease has expired, and it records the exact set of sequences it observed under the source prefix at that moment, as the largest sequence plus the list of sequences below it that were absent. That set is the segment's replay history and is sealed: an object at one of the holes that becomes visible later, because the old writer's upload was still in flight, stays excluded, and an object above the cutover is ignored, so every open of the region replays the same source history. On open the target replays the earlier segments in order, reading only footers and its own region's segments through range reads, and never creates an object under a foreign prefix, so there is nothing to conflict with.

Sealing may only exclude objects whose writes were never acknowledged, which takes two fences. Admission is fenced by the lease mechanism Mito already has: when a region's lease expires the datanode demotes it to a follower, which refuses writes, and the metasrv waits for that deadline before it upgrades the candidate. Completion is fenced by the store: every conditional create runs under an upload deadline, a create that has not completed by its deadline is reported failed to its waiters and is never acknowledged afterwards even if the object turns out to exist, and when a region's lease expires the store fails every waiter of that region that is not yet acknowledged, so that a create still in flight at that moment cannot be acknowledged later. The metasrv seals the segment only after the lease deadline plus the upload deadline have both passed, so an object that appears after sealing belongs to a create the old writer had already reported as failed. Under the durable acknowledgement mode this means a sealed set never excludes an acknowledged write; under the proposed `enqueued` mode the entries in the old writer's unpersisted backlog at that moment are lost, which is that mode's stated recovery point objective. The late-completing create is part of the acceptance criteria for takeover, stated in terms of the sealing instant: an object that was absent at sealing, whether it fills a hole later or lies above the cutover, is excluded on every open of the region; an object that was visible at sealing replays on every open even if its write was never acknowledged, which is the same outcome as a crash between an object's creation and its acknowledgement; and in the durable mode every caller whose create had not completed when the lease expired must have been failed, so no acknowledged write is among the excluded objects. The generation in the key is an additional guarantee that such a late object lands in a directory the new writer never reads beyond the sealed set. Object-sequence-major entry ids are what make this cheap: `entry_id >> 20` locates the object under the right prefix without the source node's in-memory catalog.

**Entry id floor across prefixes.** A region that arrives under a new prefix, by migration or by failover, brings a watermark and replayed ids that were assigned under another prefix's sequence. Before the target assigns the region its first new id, the target store raises its next object sequence above `max_id >> 20`, where `max_id` is the largest of the region's inherited watermark and every id it replayed, so that every new id of that region is greater than every id it already has; sequence gaps are tolerated and the sequence space is wide enough for this. The rule is applied on every open from the region's persisted state, in the same place Mito already hands the store the manifest watermark, so it needs no persisted state of its own and survives a crash before the target's first flush: the restart re-derives the floor and the region's acknowledged-but-unflushed entries under the target prefix are above the inherited watermark and replay.

**Acknowledgement modes.** Both are defined here, one implemented and one *proposed*. Cluster mode is expected to run `enqueued` for latency-sensitive workloads; the object layout, the flush durability barrier and the recovery path are identical in both modes.

**Corruption.** *Proposed* segment-level skipping with region marking lets cluster mode route the WAL hole event to the metasrv so that a follower or a takeover knows the region needs a fresh replica rather than a replay.

**Garbage collection across nodes.** Within one prefix, garbage collection works as described under *Entry ids*: watermark from each region's manifest, per-segment footer comparison, the highest-sequence object always retained. A source prefix that still holds segments of regions which have moved away cannot be trimmed by the source alone, because it no longer learns those regions' watermarks. Either the target reports to the metasrv, after each flush, the source sequence below which it no longer needs the region, and the metasrv drives deletion, or a cluster-level janitor collects the watermarks; which of the two is the last of the *Unresolved questions*.

# Future work

1. Recovery reads: fetch only trailers and footers with bounded concurrency; verify segment CRCs on read; probe the next sequence with a GET instead of listing.
2. Object-sequence-major entry ids, the pipelined uploader with in-order acknowledgement, and the `enqueued` mode with its backlog bounds, in that order, each landing as its own change with the crash gate run in both modes.
3. Segment-level corruption skipping with region marking and metrics.
4. Garbage collection of objects whose every segment is at or below its region's flushed watermark, keeping the highest-sequence object, driven by the watermarks Mito re-establishes on open.
5. Metrics for flush latency, object count and size, replay duration; a fault matrix for network errors, unwritable buckets and missing objects.
6. After a transient create failure whose immediate read-back also failed, reconcile the sequence number before reusing it, so that a create that succeeded without a response is indexed instead of conflicting with the next batch.
7. A cost and latency comparison against Raft Engine with `sync_write = true`, to calibrate `flush_interval`, `max_batch_bytes` and the default acknowledgement mode.
8. Distributed mode, which needs metasrv-side allocation of node ids and generations, the takeover chain in `WalOptions`, read-only replay of a foreign prefix, and cross-node garbage collection, as sketched under *Alignment with cluster mode*.

# Unresolved questions

- Whether the writer instance id should be used to detect a second writer on the same prefix. Conditional creates already prevent overwrites, but a concurrent writer is only noticed when it wins a sequence number.
- Whether region-level `read` should fetch only the target segment with a range request rather than the whole object.
- Whether garbage collection of a source prefix after a failover is driven by the target reporting its watermark to the metasrv, or by a cluster-level janitor that collects watermarks; the same question covers regions that are closed on a node but still own objects under its prefix.
