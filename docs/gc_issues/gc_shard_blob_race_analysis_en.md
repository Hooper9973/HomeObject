# GC Issues Related to Shard/Blob

## Issue 1: `create_shard` boundary is unclear and races with GC, causing a stale `pchunk`

### related Issue

- [8/4/2025 Slack thread](https://ebay-eng.slack.com/archives/CTUCF5222/p1754299232462799)
- [Issue 99](https://docs.google.com/document/d/16DMqv9J-JuNs5c25IBuX01QXe5WbxPm4B4hAfz4JNZM/edit?tab=t.hvzi5073ibh4#heading=h.js3g2mi3agvw)
- [SDSTOR-21981: GC conflict with shard creating](https://jirap.corp.ebay.com/browse/SDSTOR-21981)

### Failure Chain Analysis

This issue mainly shows up on followers. On the leader side, the request order usually looks straightforward:

```text
C1, P1, S1, C2, P2, S2
```

Here `C` means `CREATE_SHARD`, `P` means `PUT_BLOB`, and `S` means `SEAL_SHARD`.

But follower localize order does not strictly follow leader request order. As long as dependencies are satisfied, a later `CREATE_SHARD` can localize first. The core dependencies are roughly:

1. `C1` only depends on the PG already existing
2. `P1` / `S1` depend on shard1 already existing
3. `C2` only depends on the PG already existing
4. `P2` / `S2` depend on shard2 already existing

So the reachable follower localize order can be simplified as:

```text
C1
then any ordering of {P1, S1, C2}
then any ordering of {P2, S2}
```

There are 12 core patterns here. The dangerous one is:

```text
C1, P1, C2, S1, P2, S2
```

The current `select_specific_chunk(...)` semantics are too broad. If the target chunk is in `GC`, the selector waits/retries internally. If the target chunk is `AVAILABLE`, it switches it to `INUSE`. But if the target chunk is already `INUSE`, it may still return success.

That behavior is necessary for repeated entry by the same shard, but it is wrong for another shard. Once a successor `CREATE_SHARD` localizes successfully before the predecessor has released the `vchunk`, and GC enters afterward, the system can end up persisting a stale `pchunk` into shard metadata.

The full failure chain is:

1. predecessor shard1 is using `vchunk=N`, currently mapped to physical `pchunk=A`
2. the follower receives successor shard2's `CREATE_SHARD`, targeting the same `vchunk=N`
3. `get_blk_alloc_hints(CREATE_SHARD_MSG)` resolves `vchunk=N`
4. `select_specific_chunk(pg_id, vchunk=N)` sees the chunk is `INUSE` but still returns localize success
5. during `async_alloc_write(...)`, the follower writes shard2 superblk to `pchunk=A`
6. shard2 has not committed yet, so the committed shard map still has no open-owner truth for shard2
7. shard1's `SEAL_SHARD` commits and releases `vchunk=N` to `AVAILABLE`
8. GC sees the chunk as eligible and remaps `vchunk=N` from `pchunk=A` to `pchunk=B`
9. shard2's `CREATE_SHARD` commit arrives, and `local_create_shard(...)` uses the earlier allocation result to persist `p_chunk_id=A` into shard2 metadata
10. later `put_blob` continues allocating and writing using shard2 metadata, which still points to `p_chunk_id=A`

This usually leads to two downstream risks.

The first risk is stale-chunk writes. Once shard metadata keeps the old `pchunk`, later `put_blob` requests continue writing to a chunk that is no longer the live physical location for that `vchunk`. One concrete example is clear: GC had already remapped `vchunk_id=18` from `pchunk=274` to `pchunk=282`, but `local_create_shard(...)` still recorded shard `0x18` with `p_chunk_id=274`. A later `put_blob` for blob `10132` again picked `chunk=274`, the data may be corrupted later.



The second risk is GC verification failure, and in practice this can turn into GC stuck behavior. Once later writes continue landing on a stale chunk, that stale chunk can eventually be overwritten or reused. When GC later reads those PBAs as part of `copy_valid_data(...)`, blob verification can fail because the payload on disk no longer matches the logical blob route. One observed failure looked like this:

```text
[03/07/26 01:11:51.753] ... [gc_task_id=112, pg_id=0, shard_id=0x19] blob verification fails for move_from_chunk=66, blob_id=10488, pba=[{blk#=52278 count=1025 chunk=55},]
[03/07/26 01:11:51.767] ... Invalid header found ...
...
[03/07/26 01:11:52.694] ... [gc_task_id=112, pg_id=0, shard_id=0x19] blob verification fails for move_from_chunk=66, blob_id=10699, pba=[{blk#=95328 count=1025 chunk=55},]
[03/07/26 01:11:52.694] ... Failed to copy all blobs from move_from_chunk=66 to move_to_chunk=51
[03/07/26 01:11:52.694] ... failed before persisting gc metablk
```

So the stale `pchunk` problem is not only a route-consistency issue. It can also evolve into data corruption symptoms on later reads, and once GC starts reading those corrupted PBAs, it can fail verification and get stuck retrying the same chunk.



### Root Cause Analysis

There are two root causes.

First, `CREATE_SHARD` localize only sees `INUSE`, not owner.

Today the system only knows that a `vchunk` is `INUSE`, but not who made it `INUSE`. If the same shard enters the same `vchunk` repeatedly, `INUSE` should succeed. If a different shard tries to take the same `vchunk`, `INUSE` should fail. The current code does not make that distinction, so a successor `CREATE_SHARD` on the follower can localize too early.

Second, later write paths trust the shard metadata `pchunk` too much.

`put_blob` does not re-resolve the current `pchunk` from `vchunk` every time. It directly uses the shard metadata `p_chunk_id` as the allocation hint. Once shard metadata stores a stale `pchunk`, later writes keep landing on the old chunk.

### solution

1. The main fix is to make the shard handover barrier explicit and record chunk ownership in runtime state, so that at any moment at most one open shard truly owns the target `vchunk`. See `owner_aware_vchunk_guard_design_en.md` for the detailed design.
2. `get_blk_alloc_hints(CREATE_SHARD_MSG)` should perform owner-aware acquire internally. On success it returns `chunk_id_hint`; on failure it returns `RESULT_NOT_EXIST_YET`, and the replication layer retries later.
3. `put_blob` should gradually stop trusting shard metadata `p_chunk_id` blindly. At least on key write paths, we should evaluate whether shard metadata `pchunk` matches the current `vchunk -> pchunk` mapping. Once the main lifecycle fix is in place, this mismatch should no longer happen in theory; if it still happens, it should be surfaced directly instead of continuing to write to the old `pchunk`.

## Issue 2: `seal_shard` / `put_blob` race with GC and write a stale route back into the index

### related Issue

- [seal shard Slack thread](https://ebay-eng.slack.com/archives/CTUCF5222/p1777392996162189?thread_ts=1776144378.613109&cid=CTUCF5222)

### Failure Chain Analysis

The `create_shard` issue is about multiple shards competing for the same `vchunk`. This one is different. Here the problem is that a single shard lifecycle does not fully close, and a stale route gets written back into the index.

The chain is:

1. the leader receives `seal_shard`
2. the leader also already has a `put_blob` that entered earlier but commits later
3. `put_blob` checks shard state on entry, and the shard is still not `SEALED`, so the request is accepted
4. `seal_shard pre_commit` changes in-memory shard state to `SEALED`
5. `seal_shard commit` persists the sealed state and releases the `vchunk`
6. GC sees no open shard, or sees the chunk as eligible, then starts migrating data and updating the route
7. the later `put_blob commit` still inserts the old `pba` from its earlier allocation result into the pg index table

The result is that GC has already moved the route forward, but the late `put_blob commit` registers the old `pba` back into pg index.

### Root Cause Analysis

The core issue here is that validation is incomplete.

Today the shard state is checked when the leader first accepts a `put_blob` request. At that moment the shard may still be `OPEN`, so the request is allowed to proceed. But that constraint is not enforced again in the later commit path.

Once `seal_shard` has moved the shard into `SEALED` and GC has already advanced the route, a late `put_blob commit` can still write the old `pba` back into pg index. In other words, the request is validated at admission time, but not validated again when it is finally about to publish its route into the committed state.

`put_blob` also uses shard metadata `p_chunk_id` directly during allocation. After GC introduced `vchunk -> pchunk` remap, that field can become stale. Without a commit-time check and without a consistency check against the current mapping, the old route can be reinserted into pg index.

### solution

This needs to be fixed in two layers.

The first layer is the `SEAL_SHARD` lifecycle boundary itself:

1. add `sealed_lsn` to shard metadata
2. in `put_blob on_commit`, validate `put_blob.lsn <= sealed_lsn`; if `put_blob.lsn > sealed_lsn`, the request must fail and must not write the old `pba` back into the index
3. this guarantees that once a shard is sealed, a late commit cannot reintroduce an old write into that chunk's route truth

The second layer is consistency between `put_blob` and GC route state:

1. before `put_blob` allocates, evaluate whether shard metadata `pchunk` matches the current `vchunk -> pchunk` mapping
2. once `create_shard` lifecycle is fixed, this mismatch should not happen in theory; if it still happens, it should be surfaced instead of continuing to write to the old `pchunk`
3. `put_blob commit` must avoid registering an old `pba` into pg index after the shard is sealed and the route has already been changed by GC

## Issue 3: BR / snapshot races with GC and the leader sends wrong data

### related Issue

- [SDSTOR-22342: BR read stale data while GC task executing](https://jirap.corp.ebay.com/browse/SDSTOR-22342)

### Failure Chain Analysis

This issue happens on the read path, and the entry point is leader-side `read_snapshot_obj()`.

The BR/snapshot leader caches routes. The typical chain is:

1. the leader generates snapshot objects in `read_snapshot_obj()`
2. when it enters a shard with `batch_id == 0`, `PGBlobIterator::generate_shard_blob_list(...)` queries the whole shard's `(blob_id -> pba)` list once
3. the query result is cached in leader-side `cur_blob_list_`
4. later blob batches and resends for the same shard continue to reuse that cached route
5. during this time GC changes the blob route from the old chunk to the new chunk
6. the leader still reads using the cached old `pba`
7. if the old chunk has already been reused by later GC, the leader may read data that does not belong to this blob at all
8. after `verify_blob(...)` fails, the current implementation may still mark the blob as `CORRUPTED` and continue sending it to the follower
9. the follower does not reject that `CORRUPTED` blob immediately; it still allocates, writes, commits, and updates the local pg index
10. the final follower-side symptom is read verify failure, typically as invalid shard header

### Root Cause Analysis

The root cause here is that reader state, GC state, and allocator state advance at different times.

Concretely:

1. leader-side `cur_blob_list_` caches the old route
2. GC has already advanced the pg index route to the new chunk
3. the old chunk may already have been reused by reserved-chunk / GC flow
4. after verify failure, the leader may still send `CORRUPTED` payload to the follower
5. the follower writes that `CORRUPTED` blob to disk and updates local index state

This is not the same layer as owner-aware `INUSE` for `CREATE_SHARD`. The owner-aware guard mainly protects write-path ownership boundaries. The BR problem is that the read path holds an old route view for too long.

### solution

1. after leader-side `verify_blob(...)` fails, add one retry path: re-query the latest route by `blob_id`, fetch the blob again, and do not send bad data directly to the follower
2. when the follower receives a `CORRUPTED` blob, do not continue write / commit / update local index
3. add a PG-level gate during BR, at least to block new normal GC from starting on the same PG

## Summary

The three main problems closed out by this document are:

1. the `create_shard` boundary is unclear, so a successor shard can localize too early and eventually persist a stale `pchunk` into shard metadata
2. the `seal_shard` lifecycle does not fully close, so a late `put_blob commit` can still write a stale route back into pg index
3. BR / snapshot holds old routes for too long, so after GC remaps the route the leader still reads by old `pba` and sends wrong data to the follower

The matching fix directions are also clear:

1. use owner-aware runtime `vchunk` guard to close the ownership boundary for `create_shard`
2. use `sealed_lsn` and owner-aware release to close the shard lifecycle boundary for `seal_shard`
3. add retry to the BR read path and tighten leader/follower handling for `CORRUPTED` blobs
4. gradually reduce direct trust in shard metadata `p_chunk_id` on write paths, and validate it against the current `vchunk -> pchunk` mapping when needed

There is also one possible GC hardening point worth keeping in mind:

5. add a runtime check before GC replace to make sure `move_from_chunk` has stayed static and has not received new writes after the GC task started

This is useful as a guardrail, but it is not the main fix. The tradeoff is that inflight data-channel writes may still arrive late even when the overall shard lifecycle is correct. In that case the GC task would fail and retry more often, so this check improves safety at the cost of more GC retries.
