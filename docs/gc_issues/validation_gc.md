This note only records a few validation points that may help intercept wrong state earlier.

These are hardening ideas, not a new main design.

## Owner Check On `acquire_specific_chunk(...)`

This is already covered by the owner-aware `vchunk` guard.

When a shard tries to acquire a `vchunk`:

1. `AVAILABLE` -> success, set owner
2. `INUSE(owner=same shard)` -> success
3. `INUSE(owner=another shard)` -> fail
4. `GC` -> wait/retry

This protects the shard lifecycle boundary.

## `PUT_BLOB` Write-Time `vchunk -> pchunk` Check

Before `PUT_BLOB` uses a `pchunk`, it can validate the current route again.

The idea is simple:

1. get the shard's `vchunk`
2. resolve the current `pchunk` from that `vchunk`
3. if that `pchunk` does not match the write target, fail or retry

The point here is to stop blindly trusting shard metadata `p_chunk_id`.

## `PUT_BLOB` Commit-Time `pba` Check

At `PUT_BLOB on_commit`, the system already has:

1. message identity like `(shard_id, blob_id)`
2. alloc result `pbas`, which already points to a concrete `pchunk`

At that point, it can recheck whether the `pchunk` in `pbas` still matches the shard's current `pchunk`.

If not, the commit should fail instead of writing an old route back into pg index.

Expected rule:
between PUT_BLOB allocation and PUT_BLOB commit, the shard route should not change underneath this write
