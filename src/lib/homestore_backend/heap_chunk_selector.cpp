#include "heap_chunk_selector.h"

#include <execution>
#include <algorithm>
#include <utility>

#include <sisl/logging/logging.h>

namespace homeobject {
// Aborts if a chunk's (state, owner) invariant is broken. See ExtendedVChunk::is_valid() for the rules.
// Every site that mutates m_state / m_owner_shard_id should call this right after the mutation so the
// inconsistency is caught at its source rather than later when the chunk is mis-handed to another shard.
static void assert_chunk_invariant(const std::shared_ptr< HeapChunkSelector::ExtendedVChunk >& chunk,
                                   const char* where) {
    RELEASE_ASSERT(chunk->is_valid(), "{}: vchunk (pchunk={}) invariant violated: state={} but owner=0x{:x}", where,
                   chunk->get_chunk_id(), chunk->m_state,
                   chunk->m_owner_shard_id.has_value() ? chunk->m_owner_shard_id.value() : 0);
}

// https://github.com/eBay/HomeObject/pull/30#discussion_r1331112743
// we make the following assumptions
// 1 homestore will initialize HeapChunkSelector by adding all the chunks single threaded
// 2 we do not need dynamic chunk requirements

// it means after the single thread initialization,
// 1 the key collection of m_per_dev_heap will never change.
// 2 the key collection of m_chunks will never change

// this should only be called when initializing HeapChunkSelector in Homestore
void HeapChunkSelector::add_chunk(csharedChunk& chunk) {
    m_chunks.emplace(VChunk(chunk).get_chunk_id(), std::make_shared< ExtendedVChunk >(chunk));
}

void HeapChunkSelector::add_chunk_internal(const chunk_num_t p_chunk_id, bool add_to_heap) {
    // private function p_chunk_id must belong to m_chunks

    auto chunk = m_chunks[p_chunk_id];
    auto pdevID = chunk->get_pdev_id();
    // add this find here, since we don`t want to call make_shared in try_emplace every time.
    auto it = m_per_dev_heap.find(pdevID);
    if (it == m_per_dev_heap.end()) {
        it = m_per_dev_heap.emplace(pdevID, std::make_shared< ChunkHeap >()).first;
        it->second->pdev_name = chunk->get_pdev_name();
    }

    // build total blks for every chunk on this device;
    it->second->m_total_blks += chunk->get_total_blks();

    if (add_to_heap) {
        std::lock_guard< std::mutex > l(it->second->mtx);
        auto& heap = it->second->m_heap;
        heap.emplace(chunk);
        it->second->available_blk_count += chunk->available_blks();
    }
}

// select_chunk is only invoked by homestore when creating a shard. By the time homestore reaches here, the
// CREATE_SHARD localize has already passed the owner-aware eligibility check in
// ReplicationStateMachine::get_blk_alloc_hints (check_specific_chunk). So this is purely a mechanical
// allocation of the requested vchunk: decode (pg_id, v_chunk_id) from the application_hint exactly as before
// and delegate to acquire_specific_chunk with NO owner (the runtime owner is established authoritatively
// elsewhere: get_most_available_blk_chunk on the leader, and acquire_specific_chunk at local_create_shard
// commit / recovery).
csharedChunk HeapChunkSelector::select_chunk(homestore::blk_count_t count, const homestore::blk_alloc_hints& hint) {
    auto& chunkIdHint = hint.chunk_id_hint;
    if (chunkIdHint.has_value()) {
        LOGWARNMOD(homeobject, "should not allocated a chunk with exiting chunkIdHint={} in hint!",
                   chunkIdHint.value());
        return nullptr;
    }

    if (!hint.application_hint.has_value()) {
        LOGWARNMOD(homeobject, "should not allocated a chunk without exiting application_hint in hint!");
        return nullptr;
    }

    // Both chunk_num_t and pg_id_t are of type uint16_t.
    static_assert(std::is_same< pg_id_t, uint16_t >::value, "pg_id_t is not uint16_t");
    static_assert(std::is_same< homestore::chunk_num_t, uint16_t >::value, "chunk_num_t is not uint16_t");
    auto application_hint = hint.application_hint.value();
    pg_id_t pg_id = (uint16_t)(application_hint >> 16 & 0xFFFF);
    homestore::chunk_num_t v_chunk_id = (uint16_t)(application_hint & 0xFFFF);

    return acquire_specific_chunk(pg_id, v_chunk_id, std::nullopt);
}

bool HeapChunkSelector::try_mark_chunk_to_gc_state(const chunk_num_t chunk_id, bool force) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto chunk_it = m_chunks.find(chunk_id);
    if (chunk_it == m_chunks.end()) {
        LOGWARNMOD(homeobject, "No chunk found for chunk_id={}", chunk_id);
        return false;
    }

    auto& chunk_state = chunk_it->second->m_state;

    if (chunk_it->second->in_gc_state()) {
        LOGWARNMOD(homeobject, "gc: chunk is already in gc state, chunk_id={}", chunk_id);
        return false; // already in gc state, no need to change
    }

    if (chunk_it->second->is_reserved_by_shard() && !force) {
        LOGWARNMOD(homeobject, "gc: chunk is reserved by a shard (state={}), chunk_id={}", chunk_state, chunk_id);
        return false;
    }

    // Pick the GC sub-state from the runtime owner so the (state, owner) invariant holds by construction: a chunk
    // an open shard still owns (owner present) enters EMERGENT_GC and keeps its owner; a free / reserved chunk
    // (no owner) enters a normal GC. The persisted task priority - not this sub-state - still drives the actual
    // GC behavior, so an owner-less chunk recovered for an emergent task is harmlessly classified as normal GC.
    chunk_state = chunk_it->second->m_owner_shard_id.has_value() ? ChunkState::EMERGENT_GC : ChunkState::GC;
    assert_chunk_invariant(chunk_it->second, "try_mark_chunk_to_gc_state");
    return true;
}

void HeapChunkSelector::mark_chunk_out_of_gc_state(const chunk_num_t chunk_id, const ChunkState final_state,
                                                   const uint64_t task_id) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto chunk_it = m_chunks.find(chunk_id);
    RELEASE_ASSERT(chunk_it != m_chunks.end(), "chunk_id={} should be in m_chunks, but not found", chunk_id);

    auto& chunk = chunk_it->second;
    RELEASE_ASSERT(chunk->in_gc_state(), "chunk_id={} should be in gc state, but in {} state", chunk_id,
                   chunk->m_state);

    chunk->m_state = final_state;
    // A chunk that returns to AVAILABLE must shed any owner it carried (e.g. a normal GC that completes back to a
    // free chunk); an EMERGENT_GC that completes back to INUSE keeps its owner.
    if (final_state == ChunkState::AVAILABLE) { chunk->m_owner_shard_id.reset(); }
    assert_chunk_invariant(chunk, "mark_chunk_out_of_gc_state");
    LOGDEBUGMOD(homeobject, "gc task_id={}, chunk_id={} is marked out of gc state, final_state={}", task_id, chunk_id,
                final_state);
}

bool HeapChunkSelector::check_specific_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id,
                                             const shard_id_t owner_shard_id) {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "check: No pg found for pg={}", pg_id);
        return false;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pg_chunk_collection->mtx);
    if (v_chunk_id >= pg_chunks.size()) {
        LOGWARNMOD(homeobject, "check: No chunk found for v_chunk_id={}", v_chunk_id);
        return false;
    }

    auto chunk = pg_chunks[v_chunk_id];
    // Read-side sanity: the (state, owner) pair the decision below relies on must be internally consistent.
    assert_chunk_invariant(chunk, "check_specific_chunk");

    if (chunk->in_gc_state()) {
        // Being remapped by GC; defer so the successor doesn't localize onto the soon-to-be-stale pchunk.
        LOGDEBUGMOD(homeobject, "check: v_chunk_id={} for pg={} (pchunk={}) is in GC, defer shard=0x{:x}", v_chunk_id,
                    pg_id, chunk->get_chunk_id(), owner_shard_id);
        return false;
    }

    if (chunk->m_state == ChunkState::SELECTED) {
        // Mechanically reserved by another in-flight create whose owner is not bound yet; we cannot prove it is
        // ours (a shard never checks its own chunk after selecting it), so defer until it commits or rolls back.
        LOGDEBUGMOD(homeobject, "check: v_chunk_id={} for pg={} (pchunk={}) is reserved (SELECTED), defer shard=0x{:x}",
                    v_chunk_id, pg_id, chunk->get_chunk_id(), owner_shard_id);
        return false;
    }

    if (chunk->m_state == ChunkState::INUSE && chunk->m_owner_shard_id.value() != owner_shard_id) {
        // Predecessor shard still owns this vchunk; the successor must wait until SEAL releases it.
        LOGDEBUGMOD(homeobject,
                    "check: v_chunk_id={} for pg={} (pchunk={}) owned by shard=0x{:x}, defer shard=0x{:x}", v_chunk_id,
                    pg_id, chunk->get_chunk_id(), chunk->m_owner_shard_id.value(), owner_shard_id);
        return false;
    }

    // AVAILABLE, or INUSE owned by the same shard (idempotent re-entry) -> eligible to proceed.
    return true;
}

void HeapChunkSelector::foreach_chunks(std::function< void(csharedChunk&) >&& cb) {
    // we should call `cb` on all the chunks, selected or not
    std::for_each(std::execution::par_unseq, m_chunks.begin(), m_chunks.end(),
                  [cb = std::move(cb)](auto& p) { cb(p.second->get_internal_chunk()); });
}

csharedChunk HeapChunkSelector::acquire_specific_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id,
                                                       const std::optional< shard_id_t > owner_shard_id) {
    while (true) {
        {
            std::unique_lock lock_guard(m_chunk_selector_mtx);
            auto pg_it = m_per_pg_chunks.find(pg_id);
            if (pg_it == m_per_pg_chunks.end()) {
                LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
                return nullptr;
            }

            auto pg_chunk_collection = pg_it->second;
            auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
            std::scoped_lock lock(pg_chunk_collection->mtx);
            if (v_chunk_id >= pg_chunks.size()) {
                LOGWARNMOD(homeobject, "No chunk found for v_chunk_id={}", v_chunk_id);
                return nullptr;
            }

            auto chunk = pg_chunks[v_chunk_id];
            assert_chunk_invariant(chunk, "acquire_specific_chunk(pre)");

            if (chunk->in_gc_state()) {
                // Being remapped by GC; wait it out and retry (validation_gc.md: GC -> wait/retry).
                LOGDEBUGMOD(homeobject, "acquire: v_chunk_id={} for pg={} (pchunk={}) is in GC, wait and retry",
                            v_chunk_id, pg_id, chunk->get_chunk_id());
            } else {
                // Leaving AVAILABLE (to SELECTED or INUSE) is the single point where the chunk stops counting
                // towards the pg's free capacity; SELECTED->INUSE and idempotent re-entries must not double count.
                const bool was_available = chunk->m_state == ChunkState::AVAILABLE;

                if (owner_shard_id.has_value()) {
                    // Owner-aware acquire (create_shard commit / recovery rebind / same-shard re-entry): the chunk
                    // becomes owned (INUSE). Reject only if a *different* shard already owns it.
                    if (chunk->m_state == ChunkState::INUSE &&
                        chunk->m_owner_shard_id.value() != owner_shard_id.value()) {
                        LOGWARNMOD(homeobject,
                                   "acquire: v_chunk_id={} for pg={} (pchunk={}) already owned by shard=0x{:x}, "
                                   "reject shard=0x{:x}",
                                   v_chunk_id, pg_id, chunk->get_chunk_id(), chunk->m_owner_shard_id.value(),
                                   owner_shard_id.value());
                        return nullptr;
                    }
                    chunk->m_state = ChunkState::INUSE;
                    chunk->m_owner_shard_id = owner_shard_id;
                    LOGDEBUGMOD(homeobject, "acquire: v_chunk_id={} for pg={} (pchunk={}) owned by shard=0x{:x}",
                                v_chunk_id, pg_id, chunk->get_chunk_id(), owner_shard_id.value());
                } else {
                    // Owner-agnostic acquire (homestore's select_chunk): only mechanically reserve a free chunk.
                    // The owner is established authoritatively later (get_most_available_blk_chunk on the leader,
                    // acquire_specific_chunk at local_create_shard commit); an already SELECTED/INUSE chunk is left
                    // untouched (idempotent re-localize).
                    if (chunk->m_state == ChunkState::AVAILABLE) { chunk->m_state = ChunkState::SELECTED; }
                    LOGDEBUGMOD(homeobject, "acquire: v_chunk_id={} for pg={} (pchunk={}) selected (owner-agnostic)",
                                v_chunk_id, pg_id, chunk->get_chunk_id());
                }

                if (was_available) {
                    --pg_chunk_collection->available_num_chunks;
                    pg_chunk_collection->available_blk_count -= chunk->available_blks();
                }
                assert_chunk_invariant(chunk, "acquire_specific_chunk(post)");
                return chunk->get_internal_chunk();
            }
        }

        // chunk is in GC, wait for a while and retry
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

bool HeapChunkSelector::release_specific_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id,
                                               const shard_id_t owner_shard_id) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return false;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    if (v_chunk_id >= pg_chunks.size()) {
        LOGWARNMOD(homeobject, "No chunk found for v_chunk_id={}", v_chunk_id);
        return false;
    }
    std::scoped_lock lock(pg_chunk_collection->mtx);
    auto chunk = pg_chunks[v_chunk_id];
    assert_chunk_invariant(chunk, "release_specific_chunk(pre)");

    // A release always targets a chunk that is currently reserved by a shard - either INUSE (owned, the normal
    // SEAL / committed-shard path) or SELECTED (homestore mechanically selected it but the create_shard is being
    // rolled back before its owner was bound). Releasing an AVAILABLE (or GC) chunk means the shard lifecycle
    // bookkeeping is corrupted - fail loudly rather than silently double-free a vchunk (which could later be
    // handed to two shards at once). The chunk is guaranteed to be released at most once: SEAL commit flushes the
    // durable commit lsn before persisting the SEALED state, so SEAL is never replay-committed twice; and the
    // create_shard rollback path only fires for an un-committed shard.
    RELEASE_ASSERT(chunk->is_reserved_by_shard(),
                   "release: v_chunk_id={} for pg={} (pchunk={}) is not reserved by a shard (state={}) when released "
                   "by shard=0x{:x}",
                   v_chunk_id, pg_id, chunk->get_chunk_id(), chunk->m_state, owner_shard_id);
    // The owner is established at create_shard commit (acquire_specific_chunk with a concrete shard id). A
    // release must therefore target either this shard's own INUSE chunk, or a still owner-less SELECTED chunk that
    // was only mechanically selected by homestore (select_chunk) and is now being rolled back before commit.
    // Releasing a chunk owned by a *different* shard means two shards believe they own the same vchunk - a bug.
    RELEASE_ASSERT(!chunk->m_owner_shard_id.has_value() || chunk->m_owner_shard_id.value() == owner_shard_id,
                   "release: v_chunk_id={} for pg={} (pchunk={}) owned by shard=0x{:x} but released by shard=0x{:x}",
                   v_chunk_id, pg_id, chunk->get_chunk_id(),
                   chunk->m_owner_shard_id.has_value() ? chunk->m_owner_shard_id.value() : 0, owner_shard_id);

    chunk->m_state = ChunkState::AVAILABLE;
    chunk->m_owner_shard_id.reset();
    ++pg_chunk_collection->available_num_chunks;
    pg_chunk_collection->available_blk_count += chunk->available_blks();
    assert_chunk_invariant(chunk, "release_specific_chunk(post)");
    LOGDEBUGMOD(homeobject, "release: v_chunk_id={} for pg={} (pchunk={}) released by shard=0x{:x}", v_chunk_id, pg_id,
                chunk->get_chunk_id(), owner_shard_id);
    return true;
}

bool HeapChunkSelector::reset_pg_chunks(pg_id_t pg_id) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return false;
    }
    {
        auto pg_chunk_collection = pg_it->second;
        std::scoped_lock lock(pg_chunk_collection->mtx);
        for (auto& chunk : pg_chunk_collection->m_pg_chunks) {
            LOGDEBUGMOD(homeobject, "reset chunk={} in pg={} for destruction", chunk->get_chunk_id(), pg_id);
            chunk->reset();
        }
    }
    return true;
}

bool HeapChunkSelector::return_pg_chunks_to_dev_heap(const pg_id_t pg_id) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return false;
    }

    auto pg_chunk_collection = pg_it->second;
    auto pdev_id = pg_chunk_collection->m_pg_chunks[0]->get_pdev_id();
    auto pdev_it = m_per_dev_heap.find(pdev_id);
    RELEASE_ASSERT(pdev_it != m_per_dev_heap.end(), "pdev_id={} should in per dev heap", pdev_id);
    auto pdev_heap = pdev_it->second;

    {
        std::scoped_lock lock(pdev_heap->mtx, pg_chunk_collection->mtx);
        for (auto& chunk : pg_chunk_collection->m_pg_chunks) {
            if (chunk->m_state == ChunkState::INUSE) {
                chunk->m_state = ChunkState::AVAILABLE;
            } // with shard which should be first
            chunk->m_pg_id = std::nullopt;
            chunk->m_v_chunk_id = std::nullopt;

            pdev_heap->m_heap.emplace(chunk);
            pdev_heap->available_blk_count += chunk->available_blks();
        }
    }
    m_per_pg_chunks.erase(pg_it);
    return true;
}

uint32_t HeapChunkSelector::get_chunk_size() const {
    const auto chunk = m_chunks.begin()->second;
    return chunk->size();
}

homestore::cshared< HeapChunkSelector::ExtendedVChunk >
HeapChunkSelector::get_pg_vchunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return nullptr;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    if (v_chunk_id >= pg_chunks.size()) {
        LOGWARNMOD(homeobject, "No chunk found for v_chunk_id={}", v_chunk_id);
        return nullptr;
    }
    std::scoped_lock lock(pg_chunk_collection->mtx);
    return pg_chunks[v_chunk_id];
}

bool HeapChunkSelector::is_chunk_available(const pg_id_t pg_id, const chunk_num_t v_chunk_id) const {
    auto Exvchunk = get_pg_vchunk(pg_id, v_chunk_id);
    if (Exvchunk) return Exvchunk->available();
    return false;
}

std::optional< uint32_t > HeapChunkSelector::select_chunks_for_pg(pg_id_t pg_id, uint64_t pg_size) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    const auto chunk_size = get_chunk_size();
    if (pg_size < chunk_size) {
        LOGWARNMOD(homeobject, "pg_size={} is less than chunk_size={}", pg_size, chunk_size);
        return std::nullopt;
    }
    const uint32_t num_chunk = sisl::round_down(pg_size, chunk_size) / chunk_size;

    if (m_per_pg_chunks.find(pg_id) != m_per_pg_chunks.end()) {
        // leader may call select_chunks_for_pg multiple times
        RELEASE_ASSERT(num_chunk == m_per_pg_chunks[pg_id]->m_pg_chunks.size(), "num_chunk should be same");
        LOGWARNMOD(homeobject, "PG had already created, pg={}", pg_id);
        return num_chunk;
    }

    // Select a pdev with the most available num chunk
    auto most_avail_dev_it = std::max_element(m_per_dev_heap.begin(), m_per_dev_heap.end(),
                                              [](const std::pair< const uint32_t, std::shared_ptr< ChunkHeap > >& lhs,
                                                 const std::pair< const uint32_t, std::shared_ptr< ChunkHeap > >& rhs) {
                                                  return lhs.second->size() < rhs.second->size();
                                              });
    auto& pdev_heap = most_avail_dev_it->second;
    if (num_chunk > pdev_heap->size()) {
        LOGWARNMOD(homeobject, "Pdev has no enough space to create pg={} with num_chunk={}, available_num_chunk={}",
                   pg_id, num_chunk, pdev_heap->size());
        return std::nullopt;
    }
    LOGINFOMOD(homeobject, "select pdev[id={}, name={}] for pg_id={}, num_chunk={}", most_avail_dev_it->first,
               most_avail_dev_it->second->pdev_name, pg_id, num_chunk);
    auto pg_it = m_per_pg_chunks.emplace(pg_id, std::make_shared< PGChunkCollection >()).first;
    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pdev_heap->mtx, pg_chunk_collection->mtx);
    pg_chunks.reserve(num_chunk);

    // v_chunk_id start from 0.
    for (chunk_num_t v_chunk_id = 0; v_chunk_id < num_chunk; ++v_chunk_id) {
        auto chunk = pdev_heap->m_heap.top();
        // sanity check
        RELEASE_ASSERT(chunk->get_total_blks() == chunk->available_blks(), "chunk should be empty");
        RELEASE_ASSERT(chunk->available(), "chunk state should be available");
        pdev_heap->m_heap.pop();
        pdev_heap->available_blk_count -= chunk->available_blks();

        chunk->m_pg_id = pg_id;
        chunk->m_v_chunk_id = v_chunk_id;
        pg_chunks.emplace_back(chunk);
        ++pg_chunk_collection->available_num_chunks;
        pg_chunk_collection->m_total_blks += chunk->get_total_blks();
        pg_chunk_collection->available_blk_count += chunk->available_blks();
    }

    return num_chunk;
}

void HeapChunkSelector::update_vchunk_info_after_gc(const chunk_num_t move_from_chunk, const chunk_num_t move_to_chunk,
                                                    const ChunkState final_state, const pg_id_t pg_id,
                                                    const chunk_num_t vchunk_id, const uint64_t task_id) {

    std::unique_lock lock_guard(m_chunk_selector_mtx);

    // if the state of move_to_chunk is updated to inuse or available, then it might be selected for gc immediately. if
    // we change the state of move_to_chunk before gc_task_sb is destroyed, when crash recovery and redo the gc task,
    // the move_to_chunk will probably has new data written into it, which is different from the data we copied from
    // move_from_chunk. so, we need to switch the chunk state after gc_task_sb is destroyed.

    auto move_to_vchunk = get_extend_vchunk(move_to_chunk);
    auto move_from_vchunk = get_extend_vchunk(move_from_chunk);

    RELEASE_ASSERT(move_from_vchunk->in_gc_state(), "move_from_chunk={} should be in gc state", move_from_chunk);
    RELEASE_ASSERT(move_to_vchunk->in_gc_state(), "move_to_chunk={} should be in gc state", move_to_chunk);

    RELEASE_ASSERT(!(move_to_vchunk->m_pg_id.has_value()), "move_to_chunk={} should not belongs to a pg",
                   move_to_chunk);

    // 1 change the pg_id and vchunk_id of the move_to_chunk according to metablk
    move_to_vchunk->m_pg_id = pg_id;
    move_to_vchunk->m_v_chunk_id = vchunk_id;

    // 2 update the state of move_to_chunk, so that it can be used for creating shard or putting blob. we need to do
    // this after reserved_chunk meta blk is updated, so that if crash happens, we recovery the move_to_chunk is the
    // same as that before crash. here, the same means not new put_blob or create_shard happens to it, the data on the
    // chunk is the same as before.
    //
    // This is the authoritative point where the remapped vchunk takes on its post-gc state, and it runs AFTER
    // switch_chunks_for_pg has installed move_to_chunk into pg_chunks. Carry the runtime owner across the remap
    // here so the (state, owner) invariant holds:
    //  - normal GC (final_state == AVAILABLE): the vchunk is free, the new pchunk carries no owner.
    //  - emergent GC (final_state == INUSE): an active shard still owns the vchunk. If move_from_chunk carries
    //    that runtime owner (the common, non-recovery path) the new pchunk inherits it and stays INUSE. During
    //    crash recovery the GC task is replayed before on_log_replay_done has rebound the open shard's owner, so
    //    move_from_chunk has no owner yet; in that case we leave the new pchunk in the owner-pending SELECTED
    //    state and let on_log_replay_done's acquire bind the owner (keeping INUSE strictly owner-backed).
    if (final_state == ChunkState::INUSE) {
        if (move_from_vchunk->m_owner_shard_id.has_value()) {
            move_to_vchunk->m_owner_shard_id = move_from_vchunk->m_owner_shard_id;
            move_to_vchunk->m_state = ChunkState::INUSE;
        } else {
            move_to_vchunk->m_owner_shard_id = std::nullopt;
            move_to_vchunk->m_state = ChunkState::SELECTED;
        }
    } else {
        move_to_vchunk->m_owner_shard_id = std::nullopt;
        move_to_vchunk->m_state = final_state;
    }

    // 3 the move_from_chunk is now an evacuated, pg-less reserved chunk: drop its owner and, if it was relocated by
    // an emergent GC (EMERGENT_GC), downgrade it to a plain GC so the (state, owner) invariant holds for the
    // reserved chunk it has become (a reserved chunk has no owner -> normal GC).
    move_from_vchunk->m_pg_id = std::nullopt;
    move_from_vchunk->m_v_chunk_id = std::nullopt;
    move_from_vchunk->m_owner_shard_id = std::nullopt;
    if (move_from_vchunk->m_state == ChunkState::EMERGENT_GC) { move_from_vchunk->m_state = ChunkState::GC; }

    assert_chunk_invariant(move_to_vchunk, "update_vchunk_info_after_gc(move_to)");
    assert_chunk_invariant(move_from_vchunk, "update_vchunk_info_after_gc(move_from)");

    LOGDEBUGMOD(homeobject,
                "gc task_id={}, update vchunk info after gc, move_to_chunk={} now in pg={}, vchunk={}, state={}",
                task_id, move_to_chunk, pg_id, vchunk_id, final_state);
}

void HeapChunkSelector::switch_chunks_for_pg(const pg_id_t pg_id, const chunk_num_t old_chunk_id,
                                             const chunk_num_t new_chunk_id, const uint64_t task_id) {
    LOGDEBUGMOD(homeobject, "gc task_id={}, switch chunks for pg_id={}, old_chunk={}, new_chunk={}", task_id, pg_id,
                old_chunk_id, new_chunk_id);

    auto EXVchunk_old = get_extend_vchunk(old_chunk_id);
    auto EXVchunk_new = get_extend_vchunk(new_chunk_id);

    auto old_available_blks = EXVchunk_old->available_blks();
    auto new_available_blks = EXVchunk_new->available_blks();

    RELEASE_ASSERT(EXVchunk_old->m_v_chunk_id.has_value(), "old_chunk_id={} should has a vchunk_id", old_chunk_id);
    RELEASE_ASSERT(EXVchunk_old->m_pg_id.has_value(), "old_chunk_id={} should belongs to a pg", old_chunk_id);
    RELEASE_ASSERT(EXVchunk_old->m_pg_id.value() == pg_id, "old_chunk_id={} should belongs to pg={}", old_chunk_id,
                   pg_id);

    auto v_chunk_id = EXVchunk_old->m_v_chunk_id.value();

    std::unique_lock lock(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    RELEASE_ASSERT(pg_it != m_per_pg_chunks.end(), "No pg_chunk_collection found for pg={}", pg_id);
    auto& pg_chunk_collection = pg_it->second;

    std::unique_lock lk(pg_chunk_collection->mtx);
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;

    // LOGDEBUGMOD(homeobject, "gc: before switch chunks for pg_id={}, pg_chunks={}", pg_chunks);

    if (sisl_unlikely(pg_chunks[v_chunk_id]->get_chunk_id() == new_chunk_id)) {
        // this might happens when crash recovery. the crash happens after pg metablk is updated but before gc task
        // metablk is destroyed.
        LOGDEBUGMOD(homeobject,
                    "gc task_id={}, the pchunk_id for vchunk={} in chunkselector for pg_id={} is already {},  skip "
                    "switching chunks!",
                    task_id, v_chunk_id, pg_id, new_chunk_id);
        return;
    } else {
        RELEASE_ASSERT(
            pg_chunks[v_chunk_id]->get_chunk_id() == old_chunk_id,
            "gc task_id={}, vchunk={} for pg={} in chunkselector should have a pchunk={} , but have a pchunk={}",
            task_id, v_chunk_id, pg_id, old_chunk_id, pg_chunks[v_chunk_id]->get_chunk_id());

        pg_chunks[v_chunk_id] = EXVchunk_new;

        // NOTE: the runtime owner and gc sub-state of the old/new pchunks are NOT touched here. This function runs
        // during replace_blob_index, before move_to_chunk has been transitioned to its final state. The owner is
        // carried across the remap later, in update_vchunk_info_after_gc, which is the authoritative state
        // transition point and runs after this swap.

        LOGDEBUGMOD(homeobject,
                    "gc task_id={}, vchunk={} in pg_chunk_collection for pg_id={} has been update from pchunk_id={} to "
                    "pchunk_id={}",
                    task_id, v_chunk_id, pg_id, old_chunk_id, new_chunk_id);
    }
    pg_chunk_collection->available_blk_count += new_available_blks - old_available_blks;
}

bool HeapChunkSelector::recover_pg_chunks(pg_id_t pg_id, std::vector< chunk_num_t >&& p_chunk_ids) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    // check pg exist
    if (m_per_pg_chunks.find(pg_id) != m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "pg={} had been recovered", pg_id);
        return false;
    }
    if (p_chunk_ids.size() == 0) {
        LOGWARNMOD(homeobject, "Unexpected empty pg={}", pg_id);
        return false;
    }

    // check chunks valid, must belong to m_chunks and have same pdev_id
    std::optional< uint32_t > last_pdev_id;
    for (auto p_chunk_id : p_chunk_ids) {
        auto it = m_chunks.find(p_chunk_id);
        if (it == m_chunks.end()) {
            LOGWARNMOD(homeobject, "No chunk found for p_chunk_id={}", p_chunk_id);
            return false;
        }
        auto chunk = it->second;
        if (last_pdev_id.has_value() && last_pdev_id.value() != chunk->get_pdev_id()) {
            LOGWARNMOD(homeobject, "The pdev value is different, last_pdev_id={}, pdev_id={}", last_pdev_id.value(),
                       chunk->get_pdev_id());
            return false;
        } else {
            last_pdev_id = chunk->get_pdev_id();
        }
    }

    auto pg_it = m_per_pg_chunks.emplace(pg_id, std::make_shared< PGChunkCollection >()).first;
    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pg_chunk_collection->mtx);
    pg_chunks.reserve(p_chunk_ids.size());

    // v_chunk_id start from 0.
    for (chunk_num_t v_chunk_id = 0; v_chunk_id < p_chunk_ids.size(); ++v_chunk_id) {
        chunk_num_t p_chunk_id = p_chunk_ids[v_chunk_id];
        auto chunk = m_chunks[p_chunk_id];
        chunk->m_pg_id = pg_id;
        chunk->m_v_chunk_id = v_chunk_id;
        pg_chunks.emplace_back(chunk);
    }
    return true;
}

void HeapChunkSelector::build_pdev_available_chunk_heap() {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    for (auto [p_chunk_id, chunk] : m_chunks) {
        // if selected for pg, or it is marked as GC state(reserved chunk), not add to pdev.
        bool add_to_heap = !chunk->m_pg_id.has_value() && !chunk->in_gc_state();
        add_chunk_internal(p_chunk_id, add_to_heap);
    }
}

bool HeapChunkSelector::recover_pg_chunks_states(pg_id_t pg_id,
                                                 const std::unordered_set< chunk_num_t >& excluding_v_chunk_ids) {
    std::unique_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "PG chunks should be recovered beforhand, pg={}", pg_id);
        return false;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pg_chunk_collection->mtx);

    for (size_t v_chunk_id = 0; v_chunk_id < pg_chunks.size(); ++v_chunk_id) {
        auto chunk = pg_chunks[v_chunk_id];
        pg_chunk_collection->m_total_blks += chunk->get_total_blks();
        if (excluding_v_chunk_ids.find(v_chunk_id) == excluding_v_chunk_ids.end()) {
            chunk->m_state = ChunkState::AVAILABLE;
            chunk->m_owner_shard_id.reset();
            ++pg_chunk_collection->available_num_chunks;
            pg_chunk_collection->available_blk_count += chunk->available_blks();

        } else {
            // OPEN shard: mechanically reserve the vchunk now (owner-less); its runtime owner is rebound later in
            // on_log_replay_done via acquire_specific_chunk, which promotes it from SELECTED to INUSE.
            chunk->m_state = ChunkState::SELECTED;
        }
        assert_chunk_invariant(chunk, "recover_pg_chunks_states");
    }
    return true;
}

std::shared_ptr< const std::vector< homestore::chunk_num_t > > HeapChunkSelector::get_pg_chunks(pg_id_t pg_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "pg={} had never been created", pg_id);
        return nullptr;
    }

    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    std::scoped_lock lock(pg_chunk_collection->mtx);
    auto p_chunk_ids = std::make_shared< std::vector< homestore::chunk_num_t > >();
    p_chunk_ids->reserve(pg_chunks.size());
    for (auto chunk : pg_chunks) {
        p_chunk_ids->emplace_back(chunk->get_chunk_id());
    }
    return p_chunk_ids;
}

std::optional< homestore::chunk_num_t > HeapChunkSelector::get_most_available_blk_chunk(shard_id_t shard_id,
                                                                                       pg_id_t pg_id) {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return std::nullopt;
    }
    std::scoped_lock lock(pg_it->second->mtx);
    auto pg_chunk_collection = pg_it->second;
    auto& pg_chunks = pg_chunk_collection->m_pg_chunks;
    auto max_it =
        std::max_element(pg_chunks.begin(), pg_chunks.end(),
                         [](const std::shared_ptr< ExtendedVChunk >& a, const std::shared_ptr< ExtendedVChunk >& b) {
                             return !a->available() || (b->available() && a->available_blks() < b->available_blks());
                         });
    if (!(*max_it)->available()) {
        LOGWARNMOD(homeobject, "No available chunk for pg={}, shard=0x{:x}", pg_id, shard_id);
        return std::nullopt;
    }
    auto v_chunk_id = std::distance(pg_chunks.begin(), max_it);
    LOGDEBUGMOD(homeobject, "Picked v_chunk_id={} : [p_chunk_id={}, avail={}], shard=0x{:x}", v_chunk_id,
                pg_chunks[v_chunk_id]->get_chunk_id(), pg_chunks[v_chunk_id]->available_blks(), shard_id);
    // The leader binds the new shard as the runtime owner up-front (it knows the shard id here), so the chunk goes
    // straight to INUSE: any concurrent cross-shard acquisition of the same vchunk can then be rejected.
    pg_chunks[v_chunk_id]->m_state = ChunkState::INUSE;
    pg_chunks[v_chunk_id]->m_owner_shard_id = shard_id;
    --pg_chunk_collection->available_num_chunks;
    pg_chunk_collection->available_blk_count -= pg_chunks[v_chunk_id]->available_blks();
    assert_chunk_invariant(pg_chunks[v_chunk_id], "get_most_available_blk_chunk");
    return v_chunk_id;
}

// return the maximum number of chunks that can be allocated on pdev
uint32_t HeapChunkSelector::most_avail_num_chunks() const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    uint32_t max_avail_num_chunks = 0ul;
    for (auto const& [_, pdev_heap] : m_per_dev_heap) {
        max_avail_num_chunks = std::max(max_avail_num_chunks, pdev_heap->size());
    }

    return max_avail_num_chunks;
}

uint32_t HeapChunkSelector::avail_num_chunks(pg_id_t pg_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return 0;
    }
    return pg_it->second->available_num_chunks.load();
}

uint32_t HeapChunkSelector::total_chunks() const { return m_chunks.size(); }

uint64_t HeapChunkSelector::avail_blks(pg_id_t pg_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg={}", pg_id);
        return 0;
    }
    return pg_it->second->available_blk_count.load();
}

uint64_t HeapChunkSelector::total_blks(uint32_t dev_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto it = m_per_dev_heap.find(dev_id);
    if (it == m_per_dev_heap.end()) {
        LOGWARNMOD(homeobject, "No pdev found for pdev {}", dev_id);
        return 0;
    }

    return it->second->m_total_blks;
}

uint64_t HeapChunkSelector::get_used_blks() const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    uint64_t used_blks = 0;
    for (const auto& [_, chunk] : m_chunks) {
        if (!chunk->in_gc_state()) { used_blks += chunk->get_used_blks(); }
    }
    return used_blks;
}

std::unordered_map< uint32_t, std::vector< homestore::chunk_num_t > > HeapChunkSelector::get_pdev_chunks() const {
    std::unordered_map< uint32_t, std::vector< homestore::chunk_num_t > > pdev_chunks;
    for (const auto& [_, EXvchunk] : m_chunks) {
        auto pdev_id = EXvchunk->get_pdev_id();
        pdev_chunks.try_emplace(pdev_id, std::vector< homestore::chunk_num_t >());
        pdev_chunks[pdev_id].emplace_back(EXvchunk->get_chunk_id());
    }
    return pdev_chunks;
}

homestore::cshared< HeapChunkSelector::ExtendedVChunk >
HeapChunkSelector::get_extend_vchunk(const homestore::chunk_num_t chunk_id) const {
    auto it = m_chunks.find(chunk_id);
    if (it != m_chunks.end()) { return it->second; }
    return nullptr;
}
// dump chunks info for given pg_id, return json format
nlohmann::json HeapChunkSelector::dump_chunks_info(pg_id_t pg_id) const {
    std::shared_lock lock_guard(m_chunk_selector_mtx);
    auto pg_it = m_per_pg_chunks.find(pg_id);
    if (pg_it == m_per_pg_chunks.end()) {
        LOGWARNMOD(homeobject, "No pg found for pg_id {}", pg_id);
        return nlohmann::json::object(); // Return an empty JSON object if pg_id is not found
    }

    nlohmann::json pg_chunk_info;
    pg_chunk_info["pg"]["id"] = pg_id;
    nlohmann::json chunks_array = nlohmann::json::array();
    for (const auto& chunk : pg_it->second->m_pg_chunks) {
        nlohmann::json chunk_json;
        chunk_json["vchunk_id"] = chunk->m_v_chunk_id.value();
        chunk_json["available_blk_count"] = chunk->available_blks();
        chunk_json["state"] = chunk->m_state;
        chunk_json["p_chunk_id"] = chunk->get_chunk_id();
        chunks_array.push_back(chunk_json);
    }
    pg_chunk_info["pg"]["chunk_num"] = pg_it->second->m_pg_chunks.size();
    pg_chunk_info["pg"]["chunks"] = chunks_array;
    return pg_chunk_info;
}

} // namespace homeobject
