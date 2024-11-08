#pragma once

#include "homeobject/common.hpp"

#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>
#include <homestore/blk.h>

#include <queue>
#include <vector>
#include <mutex>
#include <functional>
#include <atomic>

namespace homeobject {

using csharedChunk = homestore::cshared< homestore::Chunk >;

class HeapChunkSelector : public homestore::ChunkSelector {
public:
    HeapChunkSelector() = default;
    ~HeapChunkSelector() = default;

    using VChunk = homestore::VChunk;
    class VChunkComparator {
    public:
        bool operator()(VChunk& lhs, VChunk& rhs) { return lhs.available_blks() < rhs.available_blks(); }
    };

    class VChunkDefragComparator {
    public:
        bool operator()(VChunk& lhs, VChunk& rhs) { return lhs.get_defrag_nblks() < rhs.get_defrag_nblks(); }
    };

    using VChunkHeap = std::priority_queue< VChunk, std::vector< VChunk >, VChunkComparator >;
    using VChunkDefragHeap = std::priority_queue< VChunk, std::vector< VChunk >, VChunkDefragComparator >;
    using ChunkIdMap = std::unordered_map < homestore::chunk_num_t, homestore::chunk_num_t >; // used for real chunk id -> virtual chunk id map
    using chunk_num_t = homestore::chunk_num_t;

    struct ChunkHeap {
        std::mutex mtx;
        VChunkHeap m_heap;
        std::atomic_size_t available_blk_count;
        uint64_t m_total_blks{0}; // initlized during boot, and will not change during runtime;
        uint32_t size() const { return m_heap.size(); }
    };

    void add_chunk(csharedChunk&) override;

    void foreach_chunks(std::function< void(csharedChunk&) >&& cb) override;

    csharedChunk select_chunk([[maybe_unused]] homestore::blk_count_t nblks, const homestore::blk_alloc_hints& hints);

    // this function will be used by GC flow or recovery flow to mark one specific chunk to be busy, caller should be
    // responsible to use release_chunk() interface to release it when no longer to use the chunk anymore.
    csharedChunk select_specific_chunk(const pg_id_t pg_id, const chunk_num_t);

    // this function will be used by GC flow to select a chunk for GC
    csharedChunk most_defrag_chunk();

    // this function is used to return a chunk back to ChunkSelector when sealing a shard, and will only be used by
    // Homeobject.
    void release_chunk(const pg_id_t pg_id, const chunk_num_t);

    /**
     * select chunks for pg, chunks need to be in same pdev.
     *
     * @param pg_id The ID of the pg.
     * @param pg_size The fix pg size.
     * @return An optional uint32_t value representing num_chunk, or std::nullopt if no space left.
     */
    std::optional< uint32_t > select_chunks_for_pg(pg_id_t pg_id, u_int64_t pg_size);

    std::shared_ptr< const std::vector <chunk_num_t> > get_pg_chunks(pg_id_t pg_id) const;

    // this should be called on each pg meta blk found
    void set_pg_chunks(pg_id_t pg_id, std::vector<chunk_num_t>&& chunk_ids);

    // this should be called after all pg meta blk recovered
    void recover_per_dev_chunk_heap();

    // this should be called after ShardManager is initialized and get all the open shards
    void recover_pg_chunk_heap(pg_id_t pg_id, const std::unordered_set< chunk_num_t >& excludingChunks);

    /**
     * Retrieves the block allocation hints for a given chunk.
     *
     * @param chunk_id The ID of the chunk.
     * @return The block allocation hints for the specified chunk.
     */
    homestore::blk_alloc_hints chunk_to_hints(chunk_num_t chunk_id) const;

    /**
     * Returns the number of available blocks of the given device id.
     *
     * @param dev_id (optional) The device ID. if nullopt, it returns the maximum available blocks among all devices.
     * @return The number of available blocks.
     */
    uint64_t avail_blks(std::optional< uint32_t > dev_id) const;

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
     * Returns the number of available chunks for a given device ID.
     *
     * @param dev_id The device ID.
     * @return The number of available chunks.
     */
    uint32_t avail_num_chunks(uint32_t dev_id) const;

    /**
     * @brief Returns the total number of chunks.
     *
     * This function returns the total number of chunks in the heap chunk selector.
     *
     * @return The total number of chunks.
     */
    uint32_t total_chunks() const;

    uint32_t get_chunk_size() const;

private:
    std::unordered_map< uint32_t, std::shared_ptr< ChunkHeap > > m_per_dev_heap;
    std::unordered_map< pg_id_t, std::shared_ptr< ChunkHeap > > m_per_pg_heap;

    // These mappings ensure "identical layout" by providing bidirectional indexing between virtual and real chunk IDs.
    // m_v2r_chunk_map: Maps each pg_id to a vector of real chunk IDs (r_chunk_id). The index in the vector corresponds to the virtual chunk ID (v_chunk_id).
    std::unordered_map< pg_id_t, std::shared_ptr< std::vector <chunk_num_t> > > m_v2r_chunk_map;
    // m_r2v_chunk_map: Maps each pg_id to a map that inversely maps real chunk IDs (r_chunk_id) to virtual chunk IDs (v_chunk_id).
    std::unordered_map< pg_id_t, std::shared_ptr< ChunkIdMap > > m_r2v_chunk_map;

    // hold all the chunks , selected or not
    std::unordered_map< chunk_num_t, csharedChunk > m_chunks;

    mutable std::shared_mutex m_chunk_selector_mtx;
    void add_chunk_internal(const chunk_num_t, bool add_to_heap = true);

    VChunkDefragHeap m_defrag_heap;
    std::mutex m_defrag_mtx;

    void remove_chunk_from_defrag_heap(const chunk_num_t);
};
} // namespace homeobject
