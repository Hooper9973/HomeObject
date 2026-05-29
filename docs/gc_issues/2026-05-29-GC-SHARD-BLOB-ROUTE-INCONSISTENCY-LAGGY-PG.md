# Root Cause Analysis: Production GC Shard/Blob Issue Summary

**Date**: May 29, 2026

**Case Owner**: Hooper Lu

**Contributors**: Xiaoxi Chen, Jie Yao

**Issue**:

Production GC failures across multiple PGs show two main shard/blob route-divergence patterns, plus a small set of cases that remain under review.

The dominant pattern is that `CREATE_SHARD` can persist a stale `p_chunk_id` after GC has already moved the live `vchunk` to a new physical chunk. A second pattern is that shard lifecycle has already moved forward, but a late blob route still remains on the old side.

Operationally, impacted laggy PGs can be recovered through `PG move`.

**Severity**: High — causes GC inconsistency, stale blob routing, and laggy PG events.

---

## Executive Summary

The production cases fall into two confirmed buckets, plus a small set that remains under review:

1. `Issue 1`: `CREATE_SHARD` persists a stale `pchunk` after the live side already moved.
2. `Issue 2`: put blob into a sealed shard.
3. two cases remain under review; one of them may fit a stale-data pattern similar to `Issue 3`.

This RCA expands two representative cases in detail:

1. `PG 39` as the clearest `Issue 1` example
2. `PG 4616` as the compact late-tail example discussed here under `Issue 2`

The remaining cases are summarized in one compact mapping table.

---

## Symptom: Production Case Pattern

### Observed Production Shape

Across the production cases, the repeated symptom is that the captured failing GC round already uses a later chunk as `move_from_chunk`, while one blob group or blob tail still points back to an older physical chunk.

This produces two visible shapes:

1. **Old/new split**: an older blob group remains on old chunk `A`, while later blobs are already on chunk `B`
2. **Late tail**: the dominant shard image is already on the new side, but one small old-side residue still remains

### Representative Case 1: `Issue 1`

#### `PG 39`: Classic old/new split

**Failed shard**: `0x2700000000028b`

##### Raw Production Evidence

```text
Querying GC logs for PG 39 on SM nuobject2-animal-dog-fd3-94-sm684...
Analyzing 1000 log entries...

================================================================================
SUCCESSFUL SHARDS: 0
================================================================================

================================================================================
EMPTY SHARDS: 2
================================================================================
  Shard 0x2700000000015f: empty in chunk(s) 4564 (40 tasks)
  Shard 0x2700000000002f: empty in chunk(s) 4564 (31 tasks)

================================================================================
FAILED SHARDS: 1
================================================================================

Shard 0x2700000000028b
  Failed tasks (63):
    Task 1389980: chunk 4564 -> 1616
    Task 1389983: chunk 4564 -> 5388
    Task 1389986: chunk 4564 -> 7385
    ... and 60 more

  Blob-level analysis:
  blob_id    chunk      blk          count    result
  ------------------------------------------------------------
  88366      3152       274572       2049     FAIL
  88367      3152       276621       2049     FAIL
  88368      3152       278670       2049     FAIL
  88369      4564       0            2049     SUCCESS
  88370      4564       2049         2049     SUCCESS
  88371      4564       4098         2049     SUCCESS
  88372      4564       6147         2049     SUCCESS
  88373      4564       8196         2049     SUCCESS
```

##### Read This Case

Read this case with only two signals:

1. `Blob-level analysis` is the main signal: old blob group is on `3152`, later blob group is on `4564`
2. `Failed tasks` only tells us that the later failing GC round we captured already uses `move_from_chunk = 4564`

##### Likely Flow

1. `CREATE_SHARD` localized early and allocated its shard header on old physical chunk `3152`.
2. Before that create committed into shard metadata, GC remapped the live `vchunk` from `3152` to `4564` without seeing this shard in `chunks_to_shards_map[3152]`.
3. `CREATE_SHARD on_commit` resumed only after the GC state cleared, but it still persisted stale `p_chunk_id = 3152` from the earlier allocation result.
4. Later route state contains both the stale side `3152` and the current side `4564`; the captured failing GC round already uses `4564` as `move_from_chunk`.

##### Minimal Timeline Picture

```text
PG 39 / shard 0x2700000000028b

time --->

s1 (old owner)      c2 (new create_shard)              GC                         chunks_to_shards_map          route publication
--------------      ----------------------             -----------------------    ----------------------------    ---------------------------
owns vchunk
uses old side

                     enters early
                    select_specific_chunk
                    accepts INUSE ownerless
                    vchunk
                    allocation writes
                    header to 3152
                    but not committed yet                                           [3152] has old shards
                                                                                     c2 not inserted yet

commit first
release old side
----------------->                                             starts GC on 3152
                                                                 scans/copies blobs
                                                                 from old side 3152
                                                                 c2 shard is not yet
                                                                 visible for rewrite
                                                                 remaps vchunk
                                                                 3152 -> 4564
                                                                 finishes and clears
                                                                 GC state

                   ------

                     commit later
                    waits until target
                    vchunk is not GC,
                    then marks current
                    vchunk INUSE
                     inserts shard into
                     chunks_to_shards_map[3152]
                    using stale pchunk
                    from allocation result                                      c2 now appears in
                                                                                  old-side map

                                                                                                                 blob 88366-88368
                                                                                                                 publish route -> 3152
                                                                                                                 stale side


                                                                  later GC round
                                                                  now sees
                                                                 move_from_chunk=4564
                                                                 while old 3152 blobs remain

                                                                                                                 blob 88369-88373
                                                                                                                 route -> 4564
                                                                                                                 current side

observed later in blob analysis:

  old blob group on 3152: 88366 88367 88368 -> FAIL
  later blob group on 4564: 88369 88370 88371 88372 88373 -> SUCCESS
```

##### Why This Maps To `Issue 1`

The key signal is just the split:

1. older blob group is still on `3152`
2. later blob group is on `4564`
3. the failing GC round we captured already uses `4564` as `move_from_chunk`
4. the source-backed way to get that split is: early create allocation on `3152`, GC remap to `4564`, then create commit persisting stale `p_chunk_id = 3152`

That is enough to make this an `Issue 1` style case.

#### `PG 3409`: Fully-wrong old-side case

**Failed shard**: `0xd5100000000023f`

##### Raw Production Evidence

```text
Querying GC logs for PG 3409 on SM nuobject2-animal-dog-fd1-38-sm204...
Analyzing 1000 log entries...

================================================================================
SUCCESSFUL SHARDS: 0
================================================================================

================================================================================
EMPTY SHARDS: 1
================================================================================
  Shard 0xd5100000000007b: empty in chunk(s) 1732 (56 tasks)

================================================================================
FAILED SHARDS: 1
================================================================================

Shard 0xd5100000000023f
  Failed tasks (107):
    Task 3151881: chunk 1732 -> 3141
    Task 3151885: chunk 1732 -> 3812
    Task 3151887: chunk 1732 -> 2540
    ... and 104 more

  Blob-level analysis:
  blob_id    chunk      blk          count    result
  ------------------------------------------------------------
  83447      3034       243834       2049     FAIL
  83448      3034       245883       2049     FAIL
  83450      3034       249981       2049     FAIL
  83461      3034       272520       2049     FAIL
```

##### Read This Case

`PG 39` is the clearest two-era split. `PG 3409` shows the stronger version of the same family.

1. the later captured failing GC round already uses `1732` as `move_from_chunk`
2. but the visible failing blob group is still concentrated on stale side `3034`
3. the new side on `1732` is so weak that the summary mostly shows the old side still failing

##### Likely Flow

1. `CREATE_SHARD` likely allocated on old physical chunk `3034` before the live side advanced.
2. The captured later GC round already uses `1732` as `move_from_chunk`, but the visible failing blob group still points back to `3034`.
3. This is a stronger version of `PG 39` because the current side is barely visible in the blob picture.

##### Minimal Timeline Picture

```text
time --->

c2 CREATE_SHARD                 GC                             observed blob state
---------------------------     --------------------------     ---------------------------
enter early
allocate header on 3034
commit not visible yet

                                live side advances
                                3034 -> 1732

on_commit
insert shard using
stale p_chunk_id = 3034

                                                                   old blobs still dominate 3034
                                                                   captured later GC round uses
                                                                  move_from_chunk = 1732
```

##### Why This Maps To `Issue 1`

That makes `PG 3409` the "all wrong" `Issue 1` shape: unlike `PG 39`, where both old and new sides are still visible, here the new side is so weak that the blob picture is almost entirely stale-side only.

### Representative Case 2: `PG 4616` (`Issue 2` late-tail shape)

**Failed shard**: `0x12080000000001a5`

#### Raw Production Evidence

```text
Querying GC logs for PG 4616 on SM nuobject2-animal-dog-fd2-40-sm417...
Analyzing 1000 log entries...

================================================================================
SUCCESSFUL SHARDS: 0
================================================================================

================================================================================
EMPTY SHARDS: 2
================================================================================
  Shard 0x1208000000000013: empty in chunk(s) 2768 (17 tasks)
  Shard 0x120800000000018b: empty in chunk(s) 2768 (9 tasks)

================================================================================
FAILED SHARDS: 1
================================================================================

Shard 0x12080000000001a5
  Failed tasks (26):
    Task 1388617: chunk 2768 -> 5971
    Task 1388618: chunk 2768 -> 5961
    Task 1388621: chunk 2768 -> 2302
    ... and 23 more

  Blob-level analysis:
  blob_id    chunk      blk          count    result
  ------------------------------------------------------------
  85716      2768       16393        2049     SUCCESS
  85717      2768       18442        2049     SUCCESS
  85718      2768       20491        2049     SUCCESS
  85719      2768       22540        2049     SUCCESS
  85720      2768       24589        2049     SUCCESS
  85721      2768       26638        2049     SUCCESS
  85722      2768       28687        2049     SUCCESS
  85723      2768       30736        2049     SUCCESS
  87685      2768       95230        2049     SUCCESS
  87686      2768       91132        2049     SUCCESS
  87687      2768       93181        2049     SUCCESS
  87688      2768       97279        2049     SUCCESS
  87689      2768       128014       2049     SUCCESS
  87690      2768       134161       2049     SUCCESS
  87691      2768       138259       2049     SUCCESS
  87692      2768       132112       2049     SUCCESS
  87717      2768       177190       2049     SUCCESS
  87718      2768       146455       2049     SUCCESS
  87719      2768       62446        2049     SUCCESS
  87720      2768       154651       2049     SUCCESS
  87721      2768       148504       2049     SUCCESS
  87722      2768       136210       2049     SUCCESS
  87723      2768       181288       2049     SUCCESS
  87724      2768       164896       2049     SUCCESS
  87725      2768       168994       2049     SUCCESS
  87726      2768       171043       2049     SUCCESS
  87727      2768       173092       2049     SUCCESS
  87728      2768       150553       2049     SUCCESS
  87729      2768       64495        2049     SUCCESS
  87730      2768       56299        2049     SUCCESS
  87731      2768       152602       2049     SUCCESS
  87732      2768       58348        2049     SUCCESS
  87733      6330       504367       2049     FAIL
```

#### Read This Case

1. old blob side is `6330`
2. later blob side is `2768`
3. the captured failing GC round already uses `2768` as `move_from_chunk`


#### Likely Flow

1. the shard route had already advanced so that the live side was on `2768`
2. one earlier accepted blob write or late route publication still preserved old-side state on `6330`
3. blob `87733` is the visible stale-side residue on `6330`
4. the captured failing GC round already uses `2768` as `move_from_chunk`

#### Minimal Timeline Picture

```text
time --->

PUT_BLOB admitted earlier        SEAL_SHARD / GC route advance      late route publication
---------------------------      ------------------------------      ---------------------------
request accepted while
shard is still open

                                 shard lifecycle advances
                                 live side moves
                                 6330 -> 2768

                                                                    one old-side blob route
                                                                    still lands on 6330
                                                                    (87733)

                                                                    dominant later blob group
                                                                    stays on 2768

                                 later GC round sees
                                 move_from_chunk = 2768
```

#### Why This Maps To `Issue 2` In This Summary

This RCA uses `PG 4616` as the representative `Issue 2` shape because the operational picture is different from `PG 39`: almost all blobs are already on `2768`, and only one old-side residue remains on `6330`.

That is the symptom we want to highlight here for `Issue 2`: once shard lifecycle has already moved forward, this kind of old-side tail should not remain.

---

## Call Chain Analysis

### Issue 1: `CREATE_SHARD` Re-enters An `INUSE vchunk` And Persists A Stale `pchunk`

Short name:

`Issue 1 = CREATE_SHARD on reused INUSE vchunk`

What it means:

1. successor `CREATE_SHARD` can allocate on a `vchunk` that is still `INUSE`
2. the allocation result records the old physical `pchunk`
3. GC can then remap the `vchunk` before the `CREATE_SHARD` commit is installed in shard metadata
4. `CREATE_SHARD on_commit` does not complete while the target `vchunk` is still in `GC`; it waits and resumes after GC clears the state
5. after it resumes, it can still persist the old physical `pchunk` from the earlier allocation result
6. later `PUT_BLOB` trusts shard metadata `p_chunk_id`, so some routes can stay on the old side while the current `vchunk` view has already moved

Typical production shape:

1. stale blob side `A`
2. current or later blob side `B`
3. captured later GC round already uses `move_from_chunk = B`, but some blob routes still point back to `A`

This is still the dominant production pattern in the current data set.

### Issue 2: `PUT_BLOB` Writes Back To A Sealed / Already-Moved Shard

Short name:

`Issue 2 = PUT_BLOB to sealed or already-moved shard`

What it means:

1. a `PUT_BLOB` was accepted earlier
2. `SEAL_SHARD` and/or GC moved the shard lifecycle forward later
3. a late write or commit still publishes a stale route back to the old side
4. the main shard image is already on the new side, but a few tail blobs still point back to the old side

Typical production shape:

1. most blobs already look correct on the new side
2. only a small tail still points back to the old side

This is the smaller tail pattern seen in the compact representative case.

### Possible Issue 3: BR Writes Or Preserves A Stale Blob

Short name:

`Issue 3 = BR writes stale blob`

What it means:

1. BR / snapshot or a similar stale-data path reads old data
2. wrong data is written or preserved into the chunk
3. later GC reads the same chunk and sees mixed good/bad data inside that chunk

Typical production shape:

1. no clear old/new chunk split
2. the same source chunk already contains mixed good/bad data

This document is still mostly based on GC-failure evidence. So `Issue 3` here should be read as a candidate explanation for chunk-local wrong-data symptoms, not as a direct BR trace.

---

## Impact Analysis

### 1. Production Distribution

From the current production mapping:

1. `Issue 1` explains most reviewed PGs (10/16) in the data set
2. `Issue 2` explains the reviewed late-tail cases (4/16)
3. `PG 759` and `PG 1962` remain under review

### 2. Compact Production Mapping Table


| Review | Case | PG | Failed shard(s) | Mapping | Short note |
| --- | --- | --- | --- | --- | --- |
| ✅ | 01 | 39 | `0x2700000000028b` | `Issue 1` | clean two-era split: `3152 -> 4564` |
| ✅ | 02 | 522 | `0x20a0000000002ec` | `Issue 1` | small but clean split: `6864 -> 6199` |
| 🟡 | 03 | 759 | `0x2f7000000000280` | `Unclear` | mixed three-chunk layout, not fully clear |
| ✅ | 04 | 1721 | `0x6b900000000020d` | `Issue 1` | dominant split on shard `0x...20d` |
| 🟡 | 05 | 1962 | `0x7aa000000000401`, `0x7aa000000000408` | `Unclear` | same chunk shows mixed good/bad data; best-fit is stale data already inside that chunk |
| ✅ | 06 | 2204 | `0x89c0000000002fa` | `Issue 1` | clean split: `1371 -> 2367` |
| ✅ | 07 | 2436 | `0x984000000000342`, `0x984000000000340` | `Issue 2` | main shard image is on new side, but a few tail blobs return to old side |
| ✅ | 08 | 2678 | `0xa760000000002f2` | `Issue 2` | clean split: `5213 -> 246` |
| ✅ | 09 | 3073 | `0xc0100000000015e` | `Issue 2` | clean split: `7778 -> 3893` |
| ✅ | 10 | 3409 | `0xd5100000000023f` | `Issue 1` | old side dominates, new side mostly absent |
| ✅ | 11 | 3969 | `0xf81000000000250` | `Issue 1` | old side dominates, new side mostly absent |
| ✅ | 12 | 4616 | `0x12080000000001a5` | `Issue 2` | one old-side blob remains: `6330 -> 2768` |
| ✅ | 13 | 5155 | `0x1423000000000224` | `Issue 1` | old side dominates, new side mostly absent |
| ✅ | 14 | 1349 | `0x545000000000278` | `Issue 1` | old side dominates, later GC already uses `6644` |
| ✅ | 15 | 2962 | `0xb9200000000019c` | `Issue 1` | clean split: `7010 -> 3237` |
| ✅ | 16 | 3907 | `0xf430000000001c0` | `Issue 1` | old side dominates, later GC already uses `7121` |


### 3. Operational Impact

The visible production impact is:

1. GC sees inconsistent route truth across old and new chunk sides
2. some PGs become laggy and require operational recovery
3. `PG move` can recover those laggy PGs, but does not remove the root cause

---

## Root Cause Summary

### Primary Root Cause

**Shard/blob lifecycle boundaries are not closed tightly enough across `CREATE_SHARD`, `SEAL_SHARD`, and later route publication.**

For `Issue 1`, the system can persist a stale physical chunk after the live `vchunk` already moved.

For `Issue 2`, the system can still leave a residual old-side blob route after shard lifecycle has already advanced.

### Contributing Factors

1. `CREATE_SHARD` can carry an earlier allocation result forward too long
2. runtime ownership on the target chunk is not strict enough during handoff
3. shard seal does not fully block later old-side blob publication
4. later GC then exposes the old/new inconsistency clearly

---

## Solutions

### Solution 1: Enforce Exclusive Chunk Ownership For `Issue 1`

The main fix direction is to make shard ownership explicit so that a shard exclusively owns the target chunk while it is active.

Concretely:

1. ensure shard ownership on the target `vchunk` is exclusive
2. prevent another shard from re-entering the same chunk before the previous owner fully releases it
3. avoid persisting a stale `p_chunk_id` from an earlier allocation result after GC has already moved the live side

### Solution 2: Add `sealed_lsn` Boundary For `Issue 2`

The second fix direction is to add `sealed_lsn` into shard metadata and use it to close the shard lifecycle boundary.

Concretely:

1. add `sealed_lsn` to shard metadata
2. after shard seal, reject any later blob commit whose LSN is newer than the sealed boundary
3. guarantee that once the shard is sealed, no blob can still enter or publish back to the old side

### Solution 3: Keep `PG move` As Operational Recovery

For already impacted production PGs:

1. identify the laggy PG
2. use `PG move` to recover it
3. rebuild onto a clean route view

This is an operational mitigation, not the root fix.

---

## Action Plan

### Phase 1: Immediate

1. Using `PG move` as the production recovery path for laggy PGs
2. keep collecting failed/laggy PG evidence into the same mapping table

### Phase 2: Short-term

1. prepare a new SM version with the ability to check shard/blob state through the meta service and evaluate the extent of cluster impact

### Phase 3: Longer-term

1. implement exclusive runtime ownership for shard/chunk handoff
2. implement `sealed_lsn` enforcement after shard seal
3. harden the stale-data path represented here as `Issue 3`

---

## References

### Related Tickets

- **SDSTOR-331296**: animal laggy PG RCA; the Jira ticket contains the original issue logs.


---

**End of RCA**
