## Current Problem

The concrete problem is simple: a `vchunk` that is already `INUSE` can still be selected again by another shard during `CREATE_SHARD`.

That makes the shard handover barrier unclear.

In the failure case, the sequence looks like this:

```text
1. predecessor shard still owns vchunk N
2. successor CREATE_SHARD arrives on the follower
3. follower localize succeeds too early, even though vchunk N is still INUSE
4. successor resolves the old pchunk for vchunk N
5. predecessor SEAL_SHARD commits and releases vchunk N
6. GC remaps vchunk N to a new pchunk
7. successor CREATE_SHARD commits later
8. local_create_shard(...) persists the earlier old pchunk into shard metadata
```

At that point, the stale `pchunk` has already been written into shard metadata.

That is the entry point of the `create_shard x GC` stale `pchunk` chain.

So the visible problem is not just that a chunk is already `INUSE`. The visible problem is that shard handover is not protected clearly enough, and another shard can still get through that boundary too early.

## Why This Happens

The reason this happens is that, once GC is part of the picture, a chunk is no longer just a stable object that happens to be busy. It may be released, remapped, or cleaned while shard handover is still in flight. Under that condition, an ambiguous `INUSE` state becomes fragile.

Today the chunk selector mainly exposes three states:

1. `AVAILABLE`
2. `INUSE`
3. `GC`

That was tolerable before GC became part of the handover window. After GC is introduced, the same ambiguity becomes much less stable, because an early success may resolve a `pchunk` that is about to be released or remapped.

The current `select_specific_chunk(...)` semantics are closer to “ensure target chunk is usable”:

1. if the target chunk is in `GC`, selector waits/retries internally
2. if the target chunk is `AVAILABLE`, it transitions to `INUSE`
3. if the target chunk is already `INUSE`, it still returns success

The problem is the third rule. Returning success on `INUSE` means the selector treats two very different situations as if they were the same:

1. the same shard is re-entering the same `vchunk`, which is expected
2. another shard is entering too early, while the old owner has not fully finished handover yet

Once GC can run between those steps, the second case becomes dangerous. The successor may localize successfully, resolve the old `pchunk`, and then race with predecessor release and GC remap. From that point on, the chunk it resolved is no longer stable.

The root issue is simple: the current state only says `INUSE`, but it does not say who owns that `INUSE`. So selector cannot tell safe same-shard re-entry from unsafe cross-shard entry.

The same `vchunk` is entered multiple times:

1. leader `_create_shard(...)` first selects a `vchunk` from available chunks
2. later `async_alloc_write(...)` localizes again based on `CREATE_SHARD_MSG`
3. commit path `local_create_shard(...)` enters again
4. replay `CREATE_SHARD` may enter through `local_create_shard(...)` again
5. `on_log_replay_done(...)` may reconcile final `OPEN` shards again

If we only look at `INUSE`, the system cannot distinguish:

1. repeated entry by the same shard, which should succeed idempotently
2. another shard trying to take the same `vchunk`, which should fail and retry later

This is why the boundary becomes blurry. The runtime has to allow re-entry, but it has no way to tell whether the re-entry belongs to the same shard or to a different shard.

That is also why `strict claim` and `ensure inuse` became confusing in earlier discussions: neither one really answers who owns this `INUSE` state.

## Design Direction

At a high level, this proposal solves the problem by making runtime ownership explicit.

The key observation is that the real problem is not simply that a chunk is `INUSE`. The real problem is that the system cannot tell whether this `INUSE` belongs to:

1. the same shard re-entering the same `vchunk`, which should succeed
2. a different shard trying to take the same `vchunk`, which should fail

So the goal of this design is not to introduce yet another split between `strict claim` and `ensure inuse`. The goal is to give the runtime enough information to separate those two cases cleanly.

The proposed direction is:

1. keep the current chunk state machine lightweight
2. add runtime-only owner information to each `vchunk`
3. make acquire/release decisions based on both state and owner
4. use the same owner-aware semantics across leader, follower, commit, replay, and recovery

With that in place, the system can do the right thing in both cases:

1. same-shard repeated entry stays idempotent
2. different-shard acquisition is rejected before it can localize to an old `pchunk`

Once that boundary is in place, the `create_shard -> stale pchunk -> later put_blob writes to old chunk` chain is cut at the source.

## Core Model

### Runtime Owner

Add runtime-only owner information to the `vchunk`.

For example, in `ExtendedVChunk`:

```cpp
std::optional<shard_id_t> m_owner_shard_id;
```

The semantics are:

1. `AVAILABLE` means no owner, `owner == nullopt`
2. `INUSE` means there is an owner, `owner == shard_id`
3. `GC` is a physical remap process and does not mean logical ownership of an open shard disappears

This owner exists only in memory:

1. it does not go into metablk
2. it does not change shard superblk schema
3. it does not change snapshot schema
4. it is rebuilt after recovery from shard map and replay results

With this, `INUSE` splits into two explicit branches:

1. `INUSE && owner == current shard_id`: same-shard repeated entry, success
2. `INUSE && owner != current shard_id`: different-shard conflict, failure

### Runtime Invariants

The recommended invariants are:

1. in `AVAILABLE`, `m_owner_shard_id == nullopt`
2. in `INUSE`, `m_owner_shard_id.has_value()` must be true
3. `INUSE(owner=X)` can only be repeatedly acquired or released by shard `X`
4. acquire by a different owner must not succeed
5. release by a different owner must not succeed
6. retain `owner_shard_id` during `Emergency GC`

One point needs to be explicit: retaining owner does not mean the business layer can bypass `GC` and continue allocating. `GC` is still an infrastructure state inside selector. When acquire sees `GC`, it keeps waiting/retrying. Retaining owner only keeps ownership semantics continuous across GC, so same-shard repeated entry and different-shard conflict are not mixed together.

## APIs

### Owner-Aware Acquire

Add or refactor an owner-aware acquire API:

```cpp
csharedChunk acquire_specific_chunk(pg_id_t pg_id,
                                    chunk_num_t v_chunk_id,
                                    shard_id_t owner_shard_id);
```

Unified semantics:

1. `GC`: selector waits/retries internally; the business layer does not branch on it directly
2. `AVAILABLE`: transition to `INUSE`, set `owner = owner_shard_id`, return success
3. `INUSE && owner == owner_shard_id`: idempotent success
4. `INUSE && owner != owner_shard_id`: return failure
5. pg or `vchunk` not found: return failure

The important part is the split between rule 3 and rule 4. They turn “already `INUSE`” into two different meanings.

This one API covers:

1. first claim
2. same-shard repeated entry
3. commit reconciliation
4. replay reconciliation

### Owner-Aware Release

Add or refactor an owner-aware release API:

```cpp
bool release_specific_chunk(pg_id_t pg_id,
                            chunk_num_t v_chunk_id,
                            shard_id_t owner_shard_id);
```

Unified semantics:

1. `INUSE && owner == owner_shard_id`: transition to `AVAILABLE`, clear owner, return success
2. `INUSE && owner != owner_shard_id`: return failure and record warning/error logs
3. `AVAILABLE`: duplicate-release no-op is acceptable, mainly for replay/recovery compatibility
4. pg or `vchunk` not found: return failure

The main goal of release is to prevent an old request from dropping a new owner.

For example, a late release from predecessor shard1 must not turn the same `vchunk` back to `AVAILABLE` after successor shard2 already owns it. Otherwise, even a strict acquire path can still be broken by an incorrect release.

### Boundary Of `get_blk_alloc_hints(CREATE_SHARD_MSG)`

This design does not extend the `blk_alloc_hints` format. It reads `shard_id` directly from the message.

```text
ReplicationStateMachine::get_blk_alloc_hints(CREATE_SHARD_MSG)
  -> parse pg_id, shard_id, v_chunk_id from message
  -> chunk_selector.acquire_specific_chunk(pg_id, v_chunk_id, shard_id)
  -> success: return chunk_id_hint = acquired_chunk->get_chunk_id()
  -> fail: return RESULT_NOT_EXIST_YET
```

The boundary is clean:

1. `get_blk_alloc_hints(...)` decides whether this shard may currently own this `vchunk` locally
2. allocator only allocates blocks from the already resolved `chunk_id_hint`
3. `blk_alloc_hints` needs no new field

`RESULT_NOT_EXIST_YET` does not mean the PG does not exist, and it does not mean the shard will never exist. It means:

```text
The local precondition for this shard to own this vchunk is not satisfied yet.
The replication layer should defer and retry.
```

That matches follower localize behavior. If the predecessor still owns the `vchunk`, successor `CREATE_SHARD` localize fails and waits for retry. Only after the predecessor really releases can the successor acquire it.

## Runtime Flows

### Leader Runtime Behavior

leader `_create_shard(...)` should still choose a suitable `vchunk` only from `AVAILABLE` chunks.

The change is that owner is set at the same time.

```text
_create_shard(...)
  -> get_most_available_blk_chunk(pg_id, shard_id)
     -> AVAILABLE -> INUSE(owner=shard_id)
  -> build CREATE_SHARD_MSG(pg_id, shard_id, v_chunk_id)
  -> async_alloc_write(...)
     -> get_blk_alloc_hints(CREATE_SHARD_MSG)
     -> acquire_specific_chunk(pg_id, v_chunk_id, same shard_id)
     -> same-owner success
  -> on_commit(...)
     -> local_create_shard(...)
     -> acquire_specific_chunk(pg_id, v_chunk_id, same shard_id)
     -> same-owner success
```

This solves the leader double-enter problem.

### Follower Runtime Behavior

After receiving `CREATE_SHARD_MSG`, the follower does not have the leader's earlier “choose from available chunks” entry point. Its first local decision happens in `get_blk_alloc_hints(CREATE_SHARD_MSG)`.

```text
receive CREATE_SHARD_MSG(pg_id, shard_id, v_chunk_id)
  -> get_blk_alloc_hints(CREATE_SHARD_MSG)
     -> acquire_specific_chunk(pg_id, v_chunk_id, shard_id)
     -> if AVAILABLE: claim success
     -> if INUSE(owner=same shard): idempotent success
     -> if INUSE(owner=another shard): fail with RESULT_NOT_EXIST_YET
  -> alloc/write shard superblk
  -> on_commit(...)
     -> local_create_shard(...)
     -> acquire_specific_chunk(pg_id, v_chunk_id, same shard_id)
     -> idempotent success
```

This directly cuts the first step of the stale `pchunk` chain:

```text
C2.localize_success while predecessor still INUSE
C1, P1, C2, S1, P2, S2
```

Under the new semantics, if the predecessor still owns the `vchunk`, successor `C2` fails localize because the owner is different. The replication layer retries later. Only after `S1.commit` finishes owner-aware release can `C2` acquire successfully.

### Commit Reconciliation

`local_create_shard(...)` uses the same owner-aware acquire.

Commit may see three cases:

1. `INUSE(owner=same shard)`: normal repeated entry, success
2. `AVAILABLE`: possible in replay/recovery reconcile, or on paths where owner is not established yet; acquire fills the owner
3. `INUSE(owner=another shard)`: inconsistent state, return failure, do not silently succeed

On normal runtime paths, `local_create_shard(...)` should usually see case 1, because leader or follower alloc/localize has already owned the `vchunk`. Case 2 mainly exists for replay/recovery reconcile and should not become the normal runtime path.

So commit no longer means “the chunk looks `INUSE`, so it is fine.” It explicitly verifies that the current owner of the `vchunk` matches the shard being committed.

### `SEAL_SHARD` Release

Current `seal_shard on_commit` is roughly:

```text
update_shard_in_map(shard_info)
get_shard_v_chunk_id(shard_id)
release_chunk(pg_id, v_chunk_id)
```

The proposal is:

```text
update_shard_in_map(shard_info)
get_shard_v_chunk_id(shard_id)
release_specific_chunk(pg_id, v_chunk_id, shard_id)
```

There are two key points.

First, release uses the `v_chunk_id` stored in the existing local shard sb, not `v_chunk_id=0/p_chunk_id=0` from `SEAL_SHARD_MSG`.

Second, release must validate owner. The predecessor may only release the owner it currently holds. If the `vchunk` is already owned by another shard, the old release must fail instead of turning the chunk back to `AVAILABLE`.

## Replay And Recovery

Owner is not persisted, so recovery rebuilds it in three steps.

### Step 1: Recover Metadata

The system first recovers durable metadata, including shard map, shard state, and the `pg_id + v_chunk_id` information that is already available after metablk recovery.

### Step 2: Replay Committed Logs

Replay `CREATE_SHARD`:

```text
on_commit(CREATE_SHARD_MSG)
  -> local_create_shard(...)
  -> acquire_specific_chunk(pg_id, v_chunk_id, shard_id)
```

Replay `SEAL_SHARD`:

```text
on_commit(SEAL_SHARD_MSG)
  -> get_shard_v_chunk_id(shard_id)
  -> release_specific_chunk(pg_id, v_chunk_id, shard_id)
```

### Step 3: `on_log_replay_done(...)` Final Reconcile

`on_log_replay_done(...)` performs one more owner-aware acquire for final `OPEN` shards as the last reconcile step.

After recovery completes, this must hold:

```text
every final OPEN shard must have vchunk state INUSE(owner = that shard)
```

If the system sees `INUSE(owner=another shard)`, that is not a normal retry case. It means recovered state and runtime owner disagree, and it should be surfaced directly as an invariant violation.

## Should `PUT_BLOB` Validate Runtime Owner?

Once chunk runtime state carries `owner_shard_id`, the next natural question is whether `PUT_BLOB` should also validate that owner.

The answer in this design is no.

The main reason is replay. Runtime owner is not durable state. During recovery, shard metadata, shard map, and other durable mappings can be recovered first, but runtime owner is rebuilt gradually through replay and final reconcile. That means replaying a `PUT_BLOB` message cannot safely rely on owner already being established at that moment.

In other words, `PUT_BLOB` and owner-aware guard do not solve the same problem:

1. owner-aware guard protects shard handover and shard lifecycle boundaries
2. `PUT_BLOB` must remain correct even when replay is rebuilding runtime state

Because of that, this design does not make `PUT_BLOB` validate runtime owner.

What `PUT_BLOB` should validate instead is the durable part of the route:

1. shard lifecycle checks such as `sealed_lsn`
2. whether the `vchunk -> pchunk` mapping is still the current valid mapping

So the split is intentional:

1. owner-aware guard ensures the `vchunk` is handed over by the right shard at the right time
2. `PUT_BLOB` only validates whether the write route is still valid for the current `vchunk -> pchunk` mapping

This keeps replay semantics stable. Replay only needs to guarantee that the final recovered state is correct. It does not require every replayed `PUT_BLOB` to observe a fully rebuilt runtime owner state at the moment it is applied.

## How This Solves The Problem

Once owner-aware runtime guard is in place, the current failure chain is blocked in the places that matter:

1. same-shard repeated entry still succeeds, so leader runtime, commit, replay, and recovery remain reentrant
2. different-shard acquisition fails before successor `CREATE_SHARD` can localize to the predecessor's old `pchunk`
3. `SEAL_SHARD` release can no longer silently drop a `vchunk` that is already owned by another shard
4. replay and recovery rebuild the same ownership model instead of using a separate localize rule

In other words, the design does not try to make `INUSE` stricter everywhere. It makes `INUSE` precise enough to preserve the reentrant cases we need, while rejecting the cross-shard cases that are currently slipping through.

## Scope

### Relationship With GC

The owner-aware guard mainly solves the ownership boundary where `CREATE_SHARD` / `SEAL_SHARD` intersects with GC.

It cuts this main chain:

```text
successor CREATE_SHARD localizes too early
-> resolves predecessor old pchunk
-> predecessor SEAL_SHARD releases
-> GC remaps
-> successor commit persists stale pchunk
```

It does not solve every GC problem by itself.

Out of scope for this design:

1. BR/snapshot holding cached old routes
2. leader sending `CORRUPTED` payload after verify failure
3. follower continuing write/commit/update-index for `CORRUPTED` blobs
4. normal `get_blob` stale-route retry
5. all windows inside GC copy/replace/update-meta
6. a broader lifecycle redesign such as handover pending / draining / `GC_READY`

### What It Does Not Solve

Those problems should continue as parallel hardening or later lifecycle redesign work.

In particular, owner-aware guard only reduces the chance of manufacturing a stale `pchunk`. It does not automatically make all existing read/write paths stop trusting shard metadata `p_chunk_id`. Key write paths still need to evaluate `vchunk -> pchunk` consistency checks or retry/fallback behavior.

## Conclusion

The core judgment in this design is:

```text
The problem is not INUSE itself. The problem is that the system does not know who made it INUSE.
```

Once runtime owner is added, the system can distinguish same-shard repeated entry from different-shard conflict.

Owner-aware runtime `vchunk` guard is the smallest clear low-intrusion fix for the `create_shard x GC` stale `pchunk` chain. It does not change durable formats, does not extend replication messages, does not extend `blk_alloc_hints`, and still unifies localize semantics across leader, follower, commit, replay, and recovery.
