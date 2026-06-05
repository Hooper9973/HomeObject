# SDSTOR-22421: Fix CREATE_SHARD × GC Stale-pchunk Race

**Date:** 2026-06-04
**Branch:** `SDSTOR-22421-fix-create-shard`
**Reference commit (log-only approach):** `c1d3e02f` (branch `review-pr`)

---

## 1. Problem Statement

When `chunks_per_pg == 1`, a successor `CREATE_SHARD` message can land on a vchunk that a
predecessor shard still owns. If GC concurrently remaps that vchunk to a new pchunk while the
successor is in-flight, the successor captures the predecessor's stale `p_chunk_id`. The recorded
shard/blob route then diverges from the live vchunk→pchunk mapping (root cause of PG 39 / PG 3409).

Two sub-problems must be solved together:

1. **Runtime ownership ambiguity** — the chunk selector cannot distinguish "same shard re-entering"
   from "different shard entering too early". A GC remap between the two is silently accepted.

2. **`r_cast` deserialisation** — the current code reads `shard_info_superblk` out of the raft
   journal header extension via a raw `r_cast`. This is fragile (no size/version checking) and
   couples all callers to the binary struct layout.

---

## 2. Scope

| In scope | Out of scope |
|---|---|
| Owner-aware vchunk guard (`heap_chunk_selector`) | SEAL_SHARD path changes |
| CREATE_SHARD log-only replication | Issue2 (late put into sealed shard data loss) |
| `shard_info_superblk` serialize/deserialize interface | Changes to `ReplicationMessageHeader` layout |
| Issue1 reproduction test | Snapshot/resync path changes |
| Shard serialization unit tests | |

---

## 3. Part 1 — Owner-Aware VChunk Guard

### 3.1 ChunkState extension

`ChunkState` is extended from three states to five:

```
AVAILABLE   — free, can be selected for a new shard
SELECTED    — mechanically reserved by homestore select_chunk (or by recovery for an OPEN shard),
              but owner not yet bound (create_shard commit / replay-done has not run)
INUSE       — owned by exactly one committed shard; owner is always present
GC          — chunk is being relocated by normal GC (no live shard owner)
EMERGENT_GC — chunk is being relocated by emergent (forced) GC while an open shard still owns it;
              owner is present
```

The split between `GC` and `EMERGENT_GC` lets `in_gc_state()` hide the distinction from external GC
bookkeeping while still preserving the owner through an emergent remap.

### 3.2 `ExtendedVChunk` ownership invariant

`ExtendedVChunk` gains a runtime-only field `m_owner_shard_id` (not persisted). The coupling between
state and owner is strict and asserted at every mutation site via `is_valid()`:

| State | Owner |
|---|---|
| `AVAILABLE` | must be absent |
| `SELECTED` | must be absent |
| `INUSE` | must be present |
| `GC` | must be absent |
| `EMERGENT_GC` | must be present |

### 3.3 New chunk selector primitives

Three owner-aware primitives replace `select_specific_chunk` / `release_chunk`:

**`check_virtual_chunk(pg_id, v_chunk_id, requester_shard_id)` → bool**
Non-mutating eligibility probe. Returns `false` (caller must defer) when:
- chunk is in `GC` or `EMERGENT_GC`
- chunk is `SELECTED` (in-flight create whose owner is not bound yet)
- chunk is `INUSE` and `owner != requester`

Returns `true` when:
- chunk is `AVAILABLE` (successor can proceed)
- chunk is `INUSE` and `owner == requester` (same-shard idempotent re-entry)

**`acquire_virtual_chunk(pg_id, v_chunk_id, owner_shard_id)` → csharedChunk**
Owner-aware promotion:
- no owner requested → `AVAILABLE` becomes `SELECTED`; `SELECTED`/`INUSE` left as-is
- owner requested, current owner none/same → marks `INUSE`, adopts/confirms owner, returns chunk

Used in `local_create_shard` (commit path and crash-recovery replay) to bind the owner and
transition `SELECTED → INUSE`.

**`release_virtual_chunk(pg_id, v_chunk_id, owner_shard_id)` → bool**
Strict release. Chunk must currently be `INUSE` (owned by the releasing shard) or `SELECTED`
(reserved by homestore but create rollback path). Transitions to `AVAILABLE`.

### 3.4 `get_most_available_blk_chunk` — up-front owner binding

`get_most_available_blk_chunk` records the new shard as the owner up-front when it selects a vchunk
(transitions `AVAILABLE → INUSE` immediately). This ensures that when `get_blk_alloc_hints` fires on
followers, `check_specific_chunk` correctly gates out concurrent successors.

### 3.5 Wiring through the create path

```
_create_shard (leader)
  └─ get_most_available_blk_chunk  →  vchunk: AVAILABLE → INUSE (owner = new_shard_id)

get_blk_alloc_hints (all replicas, CREATE_SHARD_MSG)
  └─ check_virtual_chunk(pg, v_chunk_id, shard_id)
       false  →  RESULT_NOT_EXIST_YET  (homestore retries)
       true   →  build hints with application_hint = (pg_id << 16 | v_chunk_id)

local_create_shard (all replicas, on_commit / log-replay)
  └─ acquire_virtual_chunk(pg, v_chunk_id, shard_id)  →  confirms INUSE + binds owner

SEAL_SHARD on_commit
  └─ release_virtual_chunk(pg, v_chunk_id, shard_id)  →  INUSE → AVAILABLE

CREATE_SHARD rollback / error path
  └─ release_virtual_chunk(pg, v_chunk_id, shard_id)  →  INUSE → AVAILABLE
```

### 3.6 GC interaction

`update_vchunk_info_after_gc` (run after `switch_chunks_for_pg`):
- **Emergent GC**: carries the owner onto the new pchunk (or leaves it `SELECTED` when owner is not
  yet rebound during crash recovery); old pchunk becomes a pg-less reserved chunk (`EMERGENT_GC →
  GC`, owner dropped).
- **Normal GC**: clears owner, old chunk transitions `GC → AVAILABLE`.

### 3.7 `put_blob` route validation

- **Write-time** (`blob_put_get_blk_alloc_hints`): resolves the live vchunk→pchunk mapping and
  allocates against it instead of the (possibly stale) cached `shard.p_chunk_id`.
- **Commit-time** (`on_blob_put_commit`): rejects the commit if the allocated pba's chunk no longer
  matches the live mapping, refusing to publish a stale route.

---

## 4. Part 2 — CREATE_SHARD Log-Only + Proper Serialisation

### 4.1 `shard_info_superblk` serialize/deserialize interface

The root cause of the fragile `r_cast` reads is that `shard_info_superblk` has no typed
deserialization boundary. The fix is to add `serialize` and `deserialize` member functions to the
struct itself, in `hs_homeobject.hpp`:

```cpp
struct shard_info_superblk : DataHeader {
    ShardInfo info;
    chunk_num_t p_chunk_id;
    chunk_num_t v_chunk_id;

    // Writes this struct into buf. buf must be at least sizeof(shard_info_superblk) bytes.
    void serialize(uint8_t* buf, size_t buf_size) const;

    // Validates size and returns a typed const pointer into data (zero-copy).
    // Returns nullptr on size mismatch.
    static const shard_info_superblk* deserialize(const uint8_t* data, size_t size);
};
```

- `serialize()` encapsulates the existing `memcpy(buf, this, sizeof(*this))` pattern.
- `deserialize()` checks `size >= sizeof(shard_info_superblk)` before returning the pointer.
- Binary format is unchanged — no on-wire compatibility impact.
- All callers that currently use `r_cast<shard_info_superblk const*>(header.cbytes() +
  sizeof(ReplicationMessageHeader))` for CREATE_SHARD are replaced with `deserialize()`.

**Callers to update (CREATE_SHARD path only):**

| Location | Change |
|---|---|
| `on_shard_message_rollback` — CREATE_SHARD case | `r_cast` → `deserialize()` |
| `resolve_v_chunk_id_from_msg` | `r_cast` → `deserialize()` |
| `release_chunk_based_on_create_shard_message` | `r_cast` → `deserialize()` |
| `on_shard_message_commit` — CREATE_SHARD case | `r_cast` → `deserialize()` |

SEAL_SHARD callers are left unchanged in this patch.

### 4.2 CREATE_SHARD becomes log-only

**Current flow:**
```
_create_shard
  ├─ build shard_info_superblk
  ├─ memcpy → header_extn          (journal)
  ├─ add_data_sg(sb_blob)          (data blk write)
  └─ async_alloc_write(..., data_sgs)
```

**New flow:**
```
_create_shard
  ├─ build shard_info_superblk
  ├─ sb.serialize() → header_extn  (journal only)
  └─ async_alloc_write(..., sg_list{})   ← empty data sgs
```

Specific changes in `_create_shard`:
- Remove `req->disable_push_data()` (no longer needed).
- Remove `req->add_data_sg(std::move(sb_blob))`.
- Pass `sisl::sg_list{}` instead of `req->data_sgs()` to `async_alloc_write`.
- `std::memcpy(req->header_extn(), sb_blob.cbytes(), sizeof(shard_info_superblk))` is kept as-is
  (or replaced by `sb->serialize(req->header_extn(), ...)`).
- `payload_size` and `payload_crc` remain as they are (covering the header_extn bytes).

### 4.3 `get_blk_alloc_hints` — CREATE_SHARD case is removed

When `async_alloc_write` is called with an empty `sg_list` (log-only), homestore does **not** call
`get_blk_alloc_hints`. The CREATE_SHARD case is therefore removed from `get_blk_alloc_hints`
entirely. Only SEAL_SHARD and PUT_BLOB remain.

Consequence: `check_specific_chunk` can no longer live in `get_blk_alloc_hints` for CREATE_SHARD.
Its gate responsibility moves into `on_shard_message_commit` (see §4.4 below).

`resolve_v_chunk_id_from_msg` (which was only called from the CREATE_SHARD case of
`get_blk_alloc_hints`) is removed.

### 4.4 `on_shard_message_commit` — CREATE_SHARD: guard + alloc + create

`on_shard_message_commit` signature stays as `(lsn, h, blkids, repl_dev, ctx)`. For CREATE_SHARD,
`blkids` is empty (homestore did not allocate); the commit handler performs its own allocation.

Steps in the CREATE_SHARD case:

1. **Deserialize** shard_info_superblk from header_extn via `deserialize()`. Extract `v_chunk_id`,
   `pg_id`, `shard_id`.

2. **Owner-aware guard** — replaces `check_virtual_chunk` from the old `get_blk_alloc_hints` path.
   Spin with a bounded retry until `check_virtual_chunk(pg_id, v_chunk_id, shard_id)` returns
   `true`. This is a blocking wait (capped at a small number of retries with a short sleep) in the
   commit thread. The wait is bounded: the predecessor's SEAL_SHARD commit must eventually run and
   call `release_virtual_chunk`, after which the guard unblocks.

3. **Allocate blk** — `data_service().alloc_blks(size, hints, blkids)` with
   `hints.application_hint = (pg_id << 16 | v_chunk_id)`. Retry loop (up to 5 times) on
   `SPACE_FULL` by triggering emergent GC, identical to the pattern in reference commit `c1d3e02f`.

4. **`local_create_shard`** — `acquire_virtual_chunk` transitions the vchunk `SELECTED → INUSE`
   and binds the owner. `add_new_shard_to_map` records the shard.

```cpp
// Pseudocode for on_commit CREATE_SHARD case
const auto* sb = shard_info_superblk::deserialize(
    h.cbytes() + sizeof(ReplicationMessageHeader),
    h.size() - sizeof(ReplicationMessageHeader));

// Owner-aware guard: wait for predecessor to release vchunk
for (int i = 0; i < MAX_GUARD_RETRIES; ++i) {
    if (chunk_selector()->check_virtual_chunk(pg_id, sb->v_chunk_id, shard_id)) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

// Allocate shard header blk (with emergent-GC retry on SPACE_FULL)
homestore::blk_alloc_hints hints;
hints.application_hint = (uint64_t)pg_id << 16 | sb->v_chunk_id;
hints.reserved_blks = get_reserved_blks();
for (int i = 0; i < 5; ++i) {
    alloc_status = data_service().alloc_blks(..., hints, blkids);
    if (alloc_status == SUCCESS) break;
    if (alloc_status == SPACE_FULL) { trigger emergent GC; continue; }
    RELEASE_ASSERT(false, "fatal alloc error");
}

auto shard_info = sb->info;
shard_info.lsn = lsn;
local_create_shard(shard_info, sb->v_chunk_id, blkids.chunk_num(), blkids.blk_count(), tid);
```

### 4.5 `on_fetch_data` and `on_no_space_left`

**`on_fetch_data`:** The CREATE_SHARD / SEAL_SHARD cases in `on_fetch_data` copy data from the
header extension to serve followers that need to fetch data. Since CREATE_SHARD no longer writes a
data block, the CREATE_SHARD case in `on_fetch_data` is removed. SEAL_SHARD case is not touched.

**`on_no_space_left`:** The CREATE_SHARD case in `on_no_space_left` handled the scenario where
writing the shard header blk on a follower ran out of space. Since the data blk write is moved into
`on_commit` (no push_data), the CREATE_SHARD case here is removed. SEAL_SHARD case is not touched.

---

## 5. Combined Flow — After Both Parts

```
Leader: _create_shard
  1. get_most_available_blk_chunk  →  vchunk: AVAILABLE → INUSE (owner = shard_id)
  2. build shard_info_superblk (info + v_chunk_id)
  3. sb.serialize() → header_extn
  4. async_alloc_write(header, key={}, data={})  ← log-only, no get_blk_alloc_hints

All replicas: on_commit (CREATE_SHARD_MSG)
  5. deserialize shard_info_superblk from header_extn  (via deserialize())
  6. check_virtual_chunk(pg, v_chunk_id, shard_id)  ← owner-aware guard
       → false: spin-wait until predecessor releases / GC finishes
       → true:  proceed
  7. data_service().alloc_blks(hints=(pg_id<<16|v_chunk_id))  →  blkids / p_chunk_id
       (retry loop on SPACE_FULL with emergent GC)
  8. local_create_shard(shard_info, v_chunk_id, p_chunk_id)
        └─ acquire_virtual_chunk  →  INUSE (owner bound / confirmed)
        └─ add_new_shard_to_map

SEAL_SHARD on_commit (unchanged)
  9. release_virtual_chunk  →  INUSE → AVAILABLE

CREATE_SHARD rollback / error
  10. release_virtual_chunk(pg, v_chunk_id, shard_id)  →  INUSE → AVAILABLE
```

---

## 6. Tests

### 6.1 `shard_info_superblk` serialization unit test

Location: `src/lib/homestore_backend/tests/hs_shard_tests.cpp` (or a dedicated misc test).

- Construct a `shard_info_superblk` with known field values.
- Call `serialize()` into a buffer.
- Call `deserialize()` on the buffer.
- Assert all fields (`info`, `v_chunk_id`, `p_chunk_id`) round-trip correctly.
- Assert `deserialize()` returns `nullptr` when size is too small.

### 6.2 Issue1 reproduction test — `Issue1StalePChunkRouteAfterGC`

Location: `src/lib/homestore_backend/tests/hs_gc_tests.cpp`

Scenario (3-replica setup, `chunks_per_pg == 1`):
1. shard1 is created on vchunk0 (pchunk0).
2. A follower pauses shard1's SEAL commit right before the vchunk release (flip
   `issue1_pause_seal_shard_release`).
3. Emergent GC runs and remaps vchunk0 → pchunk1.
4. shard1's SEAL is released; vchunk0 is now free (pchunk1).
5. A successor CREATE_SHARD targets the same vchunk0.
6. `check_virtual_chunk` deferred the successor until the predecessor released → successor now
   resolves pchunk1 (the live mapping), not pchunk0 (stale).
7. All replicas confirm pchunk1 in shard/blob routes.

Pass condition: no stale pchunk route on any replica. The test must fail on the unfixed code and
pass with the guard.

### 6.3 Existing shard tests

All tests in `hs_shard_tests.cpp` must continue to pass:
- `CreateMultiShards`
- `CreateMultiShardsOnMultiPG`
- `SealShard`
- `ShardManagerRecovery`
- `SealedShardRecovery`
- `SealShardWithRestart`
- `CreateShardOnDiskLostMember`
- `ShardVersionMigrationRecovery`

### 6.4 `test_heap_chunk_selector`

Unit tests for the state machine, `is_valid()` invariant, GC state split, and `SELECTED`
reservation. All 9 tests must pass.

---

## 7. Compatibility

- `ReplicationMessageHeader` struct layout is **not changed**. No binary compatibility concern for
  the replication wire format.
- `shard_info_superblk` binary layout is **not changed**. Existing persisted superblks and raft
  journal entries are readable without migration.
- `ChunkState` values `SELECTED` and `EMERGENT_GC` are runtime-only (not persisted). No on-disk
  format migration required.
- `m_owner_shard_id` in `ExtendedVChunk` is runtime-only (not persisted). It is rebuilt from the
  shard map during `on_log_replay_done`.
