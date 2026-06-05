#pragma once

#include "homeobject/common.hpp"

#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>
#include <homestore/blk.h>
#include <sisl/utility/enum.hpp>

#include <queue>
#include <vector>
#include <unordered_set>
#include <mutex>
#include <functional>
#include <atomic>

namespace homeobject {

// Lifecycle state of a pg vchunk inside the selector. The state is paired 1:1 with the runtime owner
// (ExtendedVChunk::m_owner_shard_id) - see ExtendedVChunk::is_valid() for the exact coupling. The state is
// runtime-only: it is never persisted and is rebuilt from the shard map / replay during recovery.
//   - AVAILABLE    : free, no owner; eligible to be picked for a new shard.
//   - SELECTED     : mechanically reserved by homestore's select_chunk (or by recovery for an OPEN shard) but
//                    its owning shard is not bound yet; no owner. It is off-limits to GC and to other shards,
//                    and is promoted to INUSE (owner bound) at create_shard commit / replay-done, or returned to
//                    AVAILABLE if the create is rolled back.
//   - INUSE        : owned by exactly one committed shard; the owner is always present.
//   - GC           : being relocated by a normal GC of a free / reserved chunk; no owner.
//   - EMERGENT_GC  : being relocated by an emergent (forced) GC of a chunk an open shard still owns; owner present.
ENUM(ChunkState, uint8_t, AVAILABLE = 0, SELECTED, INUSE, GC, EMERGENT_GC);

using csharedChunk = homestore::cshared< homestore::Chunk >;

class HeapChunkSelector : public homestore::ChunkSelector {
public:
    HeapChunkSelector() = default;
    ~HeapChunkSelector() = default;

    using VChunk = homestore::VChunk;
    using chunk_num_t = homestore::chunk_num_t;

    class ExtendedVChunk : public VChunk {
    public:
        ExtendedVChunk(csharedChunk const& chunk) :
                VChunk(chunk), m_state(ChunkState::AVAILABLE), m_pg_id(), m_v_chunk_id(), m_owner_shard_id() {}
        ~ExtendedVChunk() = default;
        ChunkState m_state;
        std::optional< pg_id_t > m_pg_id;
        std::optional< chunk_num_t > m_v_chunk_id;
        // Runtime-only owner of an INUSE vchunk. It is NOT persisted (not in metablk / superblk / snapshot);
        // it is rebuilt after recovery from the shard map and replay results. It lets the selector tell a
        // safe same-shard re-entry (owner == shard) apart from an unsafe cross-shard acquisition
        // (owner != shard), which is the root of the create_shard x GC stale-pchunk chain.
        // See docs/gc_issues/owner_aware_vchunk_guard_design_en.md.
        std::optional< shard_id_t > m_owner_shard_id;
        bool available() const { return m_state == ChunkState::AVAILABLE; }

        // True while the chunk is reserved by a shard - either mechanically selected by homestore / recovery
        // (SELECTED, owner pending) or owned by a committed shard (INUSE). Such a chunk must not be picked for a
        // different shard and must not be normally GC'd. Callers that only care "is some shard sitting on this
        // chunk" should use this wrapper rather than testing the individual states.
        bool is_reserved_by_shard() const {
            return m_state == ChunkState::SELECTED || m_state == ChunkState::INUSE;
        }

        // True while the chunk is being relocated by GC, regardless of whether it is a normal GC (of a free
        // chunk / reserved chunk) or an emergent GC (forced GC of a chunk an open shard still owns). External GC
        // bookkeeping that only cares "is this chunk currently off-limits because GC is touching it" should use
        // this wrapper instead of comparing against a specific GC state, so adding GC sub-states does not change
        // their behavior.
        bool in_gc_state() const { return m_state == ChunkState::GC || m_state == ChunkState::EMERGENT_GC; }

        // Validates the coupling between the lifecycle state (m_state) and the runtime owner (m_owner_shard_id).
        // These two fields are mutated together and must always stay consistent. With SELECTED carrying the
        // owner-pending reservation, every state has an unambiguous owner expectation:
        //   - AVAILABLE   : free -> MUST NOT carry an owner.
        //   - SELECTED    : reserved but not yet committed -> owner is bound later, so MUST NOT carry one yet.
        //   - INUSE       : owned by a committed shard -> the owner MUST be present.
        //   - GC          : normal GC of a free / reserved chunk -> no owner.
        //   - EMERGENT_GC : forced GC of a chunk an open shard still owns -> the owner MUST be present.
        // Callers should assert is_valid() right after mutating state/owner, and may use it as a read-side check.
        bool is_valid() const {
            switch (m_state) {
            case ChunkState::AVAILABLE:
                return !m_owner_shard_id.has_value();
            case ChunkState::SELECTED:
                return !m_owner_shard_id.has_value();
            case ChunkState::INUSE:
                return m_owner_shard_id.has_value();
            case ChunkState::GC:
                return !m_owner_shard_id.has_value();
            case ChunkState::EMERGENT_GC:
                return m_owner_shard_id.has_value();
            default:
                return false;
            }
        }
    };

    class ExtendedVChunkComparator {
    public:
        bool operator()(std::shared_ptr< ExtendedVChunk >& lhs, std::shared_ptr< ExtendedVChunk >& rhs) {
            return lhs->available_blks() < rhs->available_blks();
        }
    };
    using ExtendedVChunkHeap =
        std::priority_queue< std::shared_ptr< ExtendedVChunk >, std::vector< std::shared_ptr< ExtendedVChunk > >,
                             ExtendedVChunkComparator >;

    struct ChunkHeap {
        std::mutex mtx;
        ExtendedVChunkHeap m_heap;
        std::atomic_size_t available_blk_count;
        uint64_t m_total_blks{0}; // initlized during boot, and will not change during runtime;
        std::string pdev_name;
        uint32_t size() const { return m_heap.size(); }
    };

    struct PGChunkCollection {
        std::mutex mtx;
        std::vector< std::shared_ptr< ExtendedVChunk > > m_pg_chunks;
        std::atomic_size_t available_num_chunks;
        std::atomic_size_t available_blk_count;
        uint64_t m_total_blks{0}; // initlized during boot, and will not change during runtime;
    };

    void add_chunk(csharedChunk&) override;

    void foreach_chunks(std::function< void(csharedChunk&) >&& cb) override;

    csharedChunk select_chunk([[maybe_unused]] homestore::blk_count_t nblks, const homestore::blk_alloc_hints& hints);

    /**
     * Owner-aware *eligibility check* for a specific vchunk. This is a NON-mutating probe used at CREATE_SHARD
     * localize time (ReplicationStateMachine::get_blk_alloc_hints) to decide whether the asking shard may currently
     * own this vchunk locally, before any block is allocated. It is the gate that turns the create_shard x GC
     * stale-pchunk race into a clean defer-and-retry: if the predecessor shard still owns the vchunk, the successor
     * is told to wait instead of localizing onto the predecessor's (about-to-be-remapped) pchunk.
     * See docs/gc_issues/owner_aware_vchunk_guard_design_en.md and validation_gc.md.
     *
     * Rules (no state change):
     *   - pg / vchunk not found        -> false (defer).
     *   - GC / EMERGENT_GC             -> false (defer; the vchunk is being remapped).
     *   - SELECTED                     -> false (defer; reserved by an in-flight create whose owner isn't bound).
     *   - INUSE && owner != requester  -> false (defer; predecessor still owns it).
     *   - AVAILABLE                    -> true.
     *   - INUSE && owner == requester  -> true (same-shard re-entry).
     *
     * @param pg_id          The pg owning the vchunk.
     * @param v_chunk_id     The pg-relative vchunk id.
     * @param owner_shard_id The shard that wants to own the chunk.
     * @return true if the requester may proceed to localize/allocate, false if it should defer and retry.
     */
    bool check_virtual_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id, const shard_id_t owner_shard_id);

    /**
     * Acquire a specific vchunk, waiting out any in-flight GC. This is the single mechanical acquisition
     * primitive used by every "land on this exact vchunk" path:
     *   - homestore's select_chunk callback (owner_shard_id = std::nullopt): a pure mechanical reservation that
     *     marks the chunk SELECTED (owner bound later); the owner-aware admission already happened earlier in
     *     get_blk_alloc_hints via check_virtual_chunk.
     *   - create_shard commit / recovery (owner_shard_id set): promotes the chunk to INUSE and binds/confirms the
     *     runtime owner, rejecting a cross-shard acquisition of an already owned vchunk (the create_shard x GC
     *     stale-pchunk race) while allowing the same shard to re-enter idempotently (replay / retry).
     * See docs/gc_issues/owner_aware_vchunk_guard_design_en.md.
     *
     * Rules:
     *   - GC                                       -> wait/retry internally until the chunk leaves GC.
     *   - owner-agnostic (no owner requested)      -> AVAILABLE becomes SELECTED; SELECTED/INUSE are left as-is;
     *                                                 the owner is never touched. Returns the chunk.
     *   - owner requested, current owner none/same -> mark INUSE, adopt/confirm the owner, return the chunk.
     *   - owner requested, current owner different -> conflict, return nullptr.
     *
     * @param pg_id          The pg owning the vchunk.
     * @param v_chunk_id     The pg-relative vchunk id.
     * @param owner_shard_id The shard that wants to own the chunk, or std::nullopt for an owner-agnostic acquire.
     * @return the underlying chunk on success, nullptr on conflict.
     */
    csharedChunk acquire_virtual_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id,
                                       const std::optional< shard_id_t > owner_shard_id);

    /**
     * Owner-aware release. The chunk MUST currently be reserved by a shard - either INUSE (owned by the releasing
     * shard) or SELECTED (mechanically reserved by homestore's select_chunk but whose create_shard is being
     * rolled back before its owner was bound). Releasing an AVAILABLE / GC chunk is a programming error and aborts
     * via RELEASE_ASSERT. Releasing a chunk owned by a *different* shard likewise aborts via RELEASE_ASSERT.
     *
     * @param pg_id          The pg owning the vchunk.
     * @param v_chunk_id     The pg-relative vchunk id.
     * @param owner_shard_id The shard requesting the release.
     * @return true on success; false only when the pg/vchunk cannot be found.
     */
    bool release_virtual_chunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id, const shard_id_t owner_shard_id);

    /**
     * try to mark a chunk as gc state, so that it will not be selected by any creating shard.
     *
     * @param chunk_id
     * @param force if the current state is reserved by a shard (SELECTED/INUSE), should we force it into gc. this
     * is used for the emergent gc case.
     * @return true if success, false if the chunk is reserved by a shard (and not forced) or not found.
     */
    bool try_mark_chunk_to_gc_state(const chunk_num_t chunk_id, bool force = false);

    void mark_chunk_out_of_gc_state(const chunk_num_t chunk_id, const ChunkState final_state, const uint64_t task_id);

    bool reset_pg_chunks(pg_id_t pg_id);

    /**
     * Releases all chunks associated with the specified pg_id.
     *
     * This function is used to return all chunks that are currently associated with a particular
     * pg identified by the given pg_id. It is typically used in scenarios where
     * all chunks associated with a pg need to be freed, such as pg move out.
     *
     * @param pg_id The ID of the protection group whose chunks are to be released.
     * @return A boolean value indicating whether the operation was successful.
     */
    bool return_pg_chunks_to_dev_heap(pg_id_t pg_id);

    /**
     * select chunks for pg, chunks need to be in same pdev.
     *
     * @param pg_id The ID of the pg.
     * @param pg_size The fix pg size.
     * @return An optional uint32_t value representing num_chunk, or std::nullopt if no space left.
     */
    std::optional< uint32_t > select_chunks_for_pg(pg_id_t pg_id, uint64_t pg_size);

    // this function is used for pg info superblk persist v_chunk_id <-> p_chunk_id
    std::shared_ptr< const std::vector< chunk_num_t > > get_pg_chunks(pg_id_t pg_id) const;

    /**
     * pop pg top chunk
     *
     * @param shard_id  the new shard that will own the picked chunk; recorded as the runtime owner.
     * @param pg_id The ID of the pg.
     * @return An optional chunk_num_t value representing v_chunk_id, or std::nullopt if no space left.
     */
    std::optional< chunk_num_t > get_most_available_blk_chunk(shard_id_t shard_id, pg_id_t pg_id);

    // this should be called on each pg meta blk found
    bool recover_pg_chunks(pg_id_t pg_id, std::vector< chunk_num_t >&& p_chunk_ids);

    // this should be called after all pg meta blk and gc reserved chunk recovered
    void build_pdev_available_chunk_heap();

    // this should be called after ShardManager is initialized and get all the open shards
    bool recover_pg_chunks_states(pg_id_t pg_id, const std::unordered_set< chunk_num_t >& excluding_v_chunk_ids);

    /**
     * Returns the number of available blocks of the given device id.
     *
     * @param dev_id (optional) The device ID. if nullopt, it returns the maximum available blocks among all devices.
     * @return The number of available blocks.
     */
    uint64_t avail_blks(pg_id_t pg_id) const;

    /**
     * Returns the total number of blocks of the given device;
     *
     * @param dev_id The device ID.
     * @return The total number of blocks.
     */
    uint64_t total_blks(uint32_t dev_id) const;

    /**
     * Returns the maximum number of chunks on pdev that are currently available for allocation.
     * Caller is not interested with the pdev id;
     *
     * @return The number of available chunks.
     */
    uint32_t most_avail_num_chunks() const;

    /**
     * Returns the number of available chunks for a given pg id.
     *
     * @param pg_id The pg id.
     * @return The number of available chunks.
     */
    uint32_t avail_num_chunks(pg_id_t pg_id) const;

    /**
     * @brief Returns the total number of chunks.
     *
     * This function returns the total number of chunks in the heap chunk selector.
     *
     * @return The total number of chunks.
     */
    uint32_t total_chunks() const;

    uint64_t get_used_blks() const;

    uint32_t get_chunk_size() const;

    /**
     * @brief Returns the number of disks we seen
     *
     * Warning : calling this before HS fully start might getting wrong result.
     *
     * This function returns the number of disks the chunk selector seen.
     * It should be the accurate source that how many disks in the system for data.
     * If a disk is down in degraded mode, it won't be load and no chunk will be
     * added into selector.
     */
    uint32_t total_disks() const { return m_per_dev_heap.size(); }

    bool is_chunk_available(const pg_id_t pg_id, const chunk_num_t v_chunk_id) const;

    homestore::cshared< ExtendedVChunk > get_pg_vchunk(const pg_id_t pg_id, const chunk_num_t v_chunk_id) const;

    /**
     * @brief Returns all the pdev ids that managed by this chunk selector.
     */
    std::unordered_map< uint32_t, std::vector< chunk_num_t > > get_pdev_chunks() const;

    homestore::cshared< ExtendedVChunk > get_extend_vchunk(const chunk_num_t chunk_id) const;

    // swith pg chunks pg, which will move old chunk out of pg and move new chunk into pg.
    void switch_chunks_for_pg(const pg_id_t pg_id, const chunk_num_t old_chunk_id, const chunk_num_t new_chunk_id,
                              const uint64_t task_id);

    // switch vchunk info after gc, including pg and state.
    void update_vchunk_info_after_gc(const chunk_num_t move_from_chunk, const chunk_num_t move_to_chunk,
                                     const ChunkState final_state, const pg_id_t pg_id, const chunk_num_t vchunk_id,
                                     const uint64_t task_id);

    nlohmann::json dump_chunks_info(pg_id_t pg_id) const;

private:
    void add_chunk_internal(const chunk_num_t, bool add_to_heap = true);

private:
    std::unordered_map< uint32_t, std::shared_ptr< ChunkHeap > > m_per_dev_heap;

    std::unordered_map< pg_id_t, std::shared_ptr< PGChunkCollection > > m_per_pg_chunks;
    // hold all the chunks , selected or not
    std::unordered_map< chunk_num_t, homestore::cshared< ExtendedVChunk > > m_chunks;

    mutable std::shared_mutex m_chunk_selector_mtx;
};
} // namespace homeobject
