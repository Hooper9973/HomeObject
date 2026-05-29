#include "homeobj_fixture.hpp"

TEST_F(HomeObjectFixture, BasicGC) {
    const auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    const auto num_shards_per_chunk = SISL_OPTIONS["num_shards"].as< uint64_t >();
    const auto num_blobs_per_shard = 2 * SISL_OPTIONS["num_blobs"].as< uint64_t >();

    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    std::map< pg_id_t, HSHomeObject::HS_PG* > HS_PG_map;
    std::map< pg_id_t, uint64_t > pg_chunk_nums;
    std::map< shard_id_t, std::map< blob_id_t, uint64_t > > shard_blob_ids_map;
    auto chunk_selector = _obj_inst->chunk_selector();

    // create pgs
    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        // do not use HS_PG_map[i] to change anything, const cast just for compiling
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
        pg_blob_id[i] = 0;
        pg_chunk_nums[i] = chunk_selector->get_pg_chunks(i)->size();
    }

    // create multiple shards for each chunk
    for (uint64_t i = 0; i < num_shards_per_chunk; i++) {
        std::map< pg_id_t, std::vector< shard_id_t > > pg_open_shard_id_vec;

        // create a shard for each chunk
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            for (uint64_t j = 0; j < chunk_num; j++) {
                auto shard_seq = i * chunk_num + j + 1;
                auto derived_shard_id = make_new_shard_id(pg_id, shard_seq); // shard id start from 1
                auto shard = create_shard(pg_id, 64 * Mi, "shard meta:" + std::to_string(derived_shard_id));
                LOGINFO("create shard pg={} shard {} in chunk {}", pg_id, shard.id, j);
                ASSERT_EQ(derived_shard_id, shard.id);
                pg_open_shard_id_vec[pg_id].emplace_back(shard.id);
                pg_shard_id_vec[pg_id].emplace_back(shard.id);
            }
        }

        // Put blob for all shards in all pg's.
        auto new_shard_blob_ids_map = put_blobs(pg_open_shard_id_vec, num_blobs_per_shard, pg_blob_id);
        for (const auto& [shard_id, blob_to_blk_count] : new_shard_blob_ids_map) {
            shard_blob_ids_map[shard_id].insert(blob_to_blk_count.begin(), blob_to_blk_count.end());
        }

        // seal all shards and check
        for (const auto& [pg_id, shard_vec] : pg_open_shard_id_vec) {
            auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (const auto& shard_id : shard_vec) {
                // seal the shards so that they can be selected for gc
                auto shard_info = seal_shard(shard_id);
                EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);

                auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
                ASSERT_TRUE(chunk_opt.has_value());
                auto chunk_id = chunk_opt.value();

                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                ASSERT_TRUE(EXVchunk != nullptr);
                ASSERT_EQ(EXVchunk->m_state, ChunkState::AVAILABLE);
                ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
                auto vchunk_id = EXVchunk->m_v_chunk_id.value();
                ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

                ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
                ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
            }
        }

        for (const auto& [pg_id, hs_pg] : HS_PG_map) {
            uint64_t total_blob_occupied_blk_count{0};
            const auto& shard_vec = pg_shard_id_vec[pg_id];
            for (const auto& shard_id : shard_vec) {
                total_blob_occupied_blk_count += 2; /*header and footer*/
                for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                    total_blob_occupied_blk_count += blk_count;
                }
            }
            // check pg durable entities
            ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);

            // check pg index table, the valid blob index count should be equal to the blob count
            ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id]);
        }
    }

    // delete half of the blobs per shard.
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id];
            auto& blob_to_blk_count = shard_blob_ids_map[shard_id];
            for (uint64_t i = 0; i < num_blobs_per_shard / 2; i++) {
                ASSERT_FALSE(blob_to_blk_count.empty());
                auto it = blob_to_blk_count.begin();
                auto blob_id = it->first;
                shard_blob_ids_map_for_deletion[shard_id].insert(blob_id);
                blob_to_blk_count.erase(it);
            }
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // wait until all the deleted blobs are reclaimed
    bool all_deleted_blobs_have_been_gc{true};
    while (true) {
        // we need to recalculate this everytime, since gc might update a pchunk of the vchunk for a pg
        std::map< homestore::chunk_num_t, uint64_t > chunk_used_blk_count;

        for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();
            // now, the chunk state is not determined, maybe GC(being gc) or AVAILABLE(complete gc), skip checking it.
            uint32_t used_blks{2}; /* header and footer */

            for (const auto& [_, blk_count] : blob_to_blk_count) {
                used_blks += blk_count;
            }
            chunk_used_blk_count[chunk_id] += used_blks;
        }

        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < chunk_num; i++) {
                auto chunk_id = pg_chunks->at(i);
                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                const auto available_blk = EXVchunk->available_blks();
                const auto total_blks = EXVchunk->get_total_blks();
                if (total_blks - available_blk != chunk_used_blk_count[chunk_id]) {
                    LOGINFO("pg_id={}, chunk_id={}, available_blk={}, total_blk={}, use_blk={}, waiting for gc", pg_id,
                            chunk_id, available_blk, total_blks, chunk_used_blk_count[chunk_id]);

                    if (0 == EXVchunk->get_defrag_nblks()) {
                        // some unexpect async write or free happens, increase defrag num to trigger gc again.
                        homestore::data_service().async_free_blk(homestore::MultiBlkId(0, 1, chunk_id));
                    }

                    all_deleted_blobs_have_been_gc = false;
                    break;
                }
            }
            if (!all_deleted_blobs_have_been_gc) break;
        }
        if (all_deleted_blobs_have_been_gc) break;
        all_deleted_blobs_have_been_gc = true;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    // verify blob data after gc
    std::map< shard_id_t, std::set< blob_id_t > > remaining_shard_blobs;
    for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
        for (const auto& [blob_id, _] : blob_to_blk_count) {
            remaining_shard_blobs[shard_id].insert(blob_id);
        }
    }
    verify_shard_blobs(remaining_shard_blobs);
    verify_shard_meta(pg_shard_id_vec);
    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after gc , pg_chunks should changes, the vchunk shoud change to a new pchunk. however, we can not make
            // sure the pchunk is changed since it is probably that the shard is copied from chunk_1 to chunk_2 and then
            // from chunk_2 to chunk_1, since gc might be scheduled several times when we delete blobs

            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

            ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
            ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }

        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    restart();

    HS_PG_map.clear();

    for (uint64_t i = 1; i <= num_pgs; i++) {
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
    }

    chunk_selector = _obj_inst->chunk_selector();

    verify_shard_blobs(remaining_shard_blobs);

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after restart , the pchunk of a vchunk shoud not change
            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    // delete remaining blobs
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id] = remaining_shard_blobs[shard_id];
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // wait until all the deleted blobs are reclaimed
    while (true) {
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < chunk_num; i++) {
                auto chunk_id = pg_chunks->at(i);
                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                const auto available_blk = EXVchunk->available_blks();
                const auto total_blks = EXVchunk->get_total_blks();
                if (total_blks != available_blk) {

                    if (0 == EXVchunk->get_defrag_nblks()) {
                        // some unexpect async write or free happens, increase defrag num to trigger gc again.
                        homestore::data_service().async_free_blk(homestore::MultiBlkId(0, 1, chunk_id));
                    }

                    LOGINFO("pg_id={}, chunk_id={}, available_blk={}, total_blk={}, not empty, waiting for gc", pg_id,
                            chunk_id, available_blk, total_blks);
                    all_deleted_blobs_have_been_gc = false;
                    break;
                }
            }
            if (!all_deleted_blobs_have_been_gc) break;
        }
        if (all_deleted_blobs_have_been_gc) break;
        all_deleted_blobs_have_been_gc = true;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    for (const auto& [shard_id, _] : shard_blob_ids_map) {
        auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
        ASSERT_TRUE(chunk_opt.has_value());
        auto chunk_id = chunk_opt.value();
        auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);

        // the shard is empty
        ASSERT_EQ(0, EXVchunk->get_used_blks());
    }

    // after all blobs have been deleted,
    // 1 the pg index table should be empty
    // 2 check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), 0);
        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, 0);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, 0);
    }

    // if all blobs of shard are deleted , the shard should be deleted.
    // TODO:add more check after we have delete shard implementation
}

TEST_F(HomeObjectFixture, HandlingNoSpaceLeft) {
    const auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    const auto num_shards_per_chunk = SISL_OPTIONS["num_shards"].as< uint64_t >();
    const auto num_blobs_per_shard = 2 * SISL_OPTIONS["num_blobs"].as< uint64_t >();

    std::map< pg_id_t, std::vector< shard_id_t > > total_pg_open_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    std::map< pg_id_t, uint64_t > pg_chunk_nums;
    std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map;
    auto chunk_selector = _obj_inst->chunk_selector();

    // create pgs
    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        pg_blob_id[i] = 0;
        pg_chunk_nums[i] = chunk_selector->get_pg_chunks(i)->size();
    }

    // create multiple shards for each chunk , we seal all shards except the last one
    for (uint64_t i = 0; i < num_shards_per_chunk; i++) {
        std::map< pg_id_t, std::vector< shard_id_t > > pg_open_shard_id_vec;

        // create a shard for each chunk
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            for (uint64_t j = 0; j < chunk_num; j++) {
                auto shard = create_shard(pg_id, 64 * Mi, "shard meta");
                pg_open_shard_id_vec[pg_id].emplace_back(shard.id);
            }
        }

        // Put blob for all shards in all pg's.
        auto new_shard_blob_ids_map = put_blobs(pg_open_shard_id_vec, num_blobs_per_shard, pg_blob_id);

        for (const auto& [shard_id, blob_to_blk_count] : new_shard_blob_ids_map) {
            for (const auto& [blob_id, _] : blob_to_blk_count)
                shard_blob_ids_map[shard_id].insert(blob_id);
        }

        // seal all shards except the last one and check
        for (const auto& [pg_id, shard_vec] : pg_open_shard_id_vec) {
            auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (const auto& shard_id : shard_vec) {
                auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
                ASSERT_TRUE(chunk_opt.has_value());
                auto chunk_id = chunk_opt.value();

                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                ASSERT_TRUE(EXVchunk != nullptr);
                if (i < num_shards_per_chunk - 1) {
                    // seal the shards so that they can be selected for gc
                    auto shard_info = seal_shard(shard_id);
                    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);
                    // if not the last shard, the chunk should be available
                    ASSERT_EQ(EXVchunk->m_state, ChunkState::AVAILABLE);
                } else {
                    total_pg_open_shard_id_vec[pg_id].push_back(shard_id);
                    // if the last shard, the chunk should be inuse
                    ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
                }
            }
        }
    }

    // now we set all the last offset of the blk allocator of all the chunks to the end to simulater no_space_left in
    // all the followers.
    for (uint64_t i = 1; i <= num_pgs; i++) {
        run_on_pg_follower(i, [&]() {
            auto& data_service = homestore::data_service();
            auto blk_size = data_service.get_blk_size();
            auto pg_chunks = _obj_inst->chunk_selector()->get_pg_chunks(i);

            for (const auto& chunk : *(pg_chunks)) {
                auto vchunk = chunk_selector->get_extend_vchunk(chunk);
                ASSERT_TRUE(vchunk);
                auto available_blk_num = vchunk->available_blks();

                homestore::MultiBlkId all_remaining_blk;
                homestore::blk_alloc_hints hints;
                hints.chunk_id_hint = chunk;

                // allocate all the remaining blocks, so that there is no space left on this chunk
                const auto status = data_service.alloc_blks(available_blk_num * blk_size, hints, all_remaining_blk);

                LOGINFO("Set chunk {} to no_space_left, total_blks={}, available_blks={}, used_blks={}", chunk,
                        vchunk->get_total_blks(), vchunk->available_blks(), vchunk->get_used_blks());
                ASSERT_TRUE(status == homestore::BlkAllocStatus::SUCCESS);
                ASSERT_TRUE(vchunk->available_blks() == 0);
            }
        });
    }

    // now, trigger no space left in all chunks and all the put_blob should succeed
    auto new_shard_blob_ids_map = put_blobs(total_pg_open_shard_id_vec, num_blobs_per_shard, pg_blob_id);

    for (const auto& [shard_id, blob_to_blk_count] : new_shard_blob_ids_map) {
        for (const auto& [blob_id, _] : blob_to_blk_count)
            shard_blob_ids_map[shard_id].insert(blob_id);
    }

    verify_shard_blobs(shard_blob_ids_map);
}

TEST_F(HomeObjectFixture, BasicEGC) { EmergentGC(false); }

TEST_F(HomeObjectFixture, EGCWithCrashRecovery) { EmergentGC(true); }

void HomeObjectFixture::EmergentGC(bool with_crash_recovery) {
    const auto num_shards_per_chunk = SISL_OPTIONS["num_shards"].as< uint64_t >();
    const auto num_blobs_per_shard = 2 * SISL_OPTIONS["num_blobs"].as< uint64_t >();
    std::map< pg_id_t, std::vector< shard_id_t > > pg_shard_id_vec;
    std::map< pg_id_t, blob_id_t > pg_blob_id;
    std::map< pg_id_t, HSHomeObject::HS_PG* > HS_PG_map;
    std::map< pg_id_t, uint64_t > pg_chunk_nums;
    std::map< shard_id_t, std::map< blob_id_t, uint64_t > > shard_blob_ids_map;
    auto chunk_selector = _obj_inst->chunk_selector();
    const auto num_pgs = chunk_selector->get_pdev_chunks().size();

    for (uint16_t i = 1; i <= num_pgs; i++) {
        create_pg(i);
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        // do not use HS_PG_map[i] to change anything, const cast just for compiling
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
        pg_blob_id[i] = 0;
        pg_chunk_nums[i] = chunk_selector->get_pg_chunks(i)->size();
    }

    // create multiple shards for each chunk , we seal all shards except the last one
    for (uint64_t i = 0; i < num_shards_per_chunk; i++) {
        std::map< pg_id_t, std::vector< shard_id_t > > pg_open_shard_id_vec;

        // create a shard for each chunk
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            for (uint64_t j = 0; j < chunk_num; j++) {
                auto shard = create_shard(pg_id, 64 * Mi, "shard meta");
                pg_open_shard_id_vec[pg_id].emplace_back(shard.id);
                pg_shard_id_vec[pg_id].emplace_back(shard.id);
            }
        }

        // Put blob for all shards in all pg's.
        auto new_shard_blob_ids_map = put_blobs(pg_open_shard_id_vec, num_blobs_per_shard, pg_blob_id);

        for (const auto& [shard_id, blob_to_blk_count] : new_shard_blob_ids_map) {
            shard_blob_ids_map[shard_id].insert(blob_to_blk_count.begin(), blob_to_blk_count.end());
        }

        // seal all shards except the last one and check
        for (const auto& [pg_id, shard_vec] : pg_open_shard_id_vec) {
            auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (const auto& shard_id : shard_vec) {
                auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
                ASSERT_TRUE(chunk_opt.has_value());
                auto chunk_id = chunk_opt.value();

                auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
                ASSERT_TRUE(EXVchunk != nullptr);
                if (i < num_shards_per_chunk - 1) {
                    // seal the shards so that they can be selected for gc
                    auto shard_info = seal_shard(shard_id);
                    EXPECT_EQ(ShardInfo::State::SEALED, shard_info.state);
                    // if not the last shard, the chunk should be available
                    ASSERT_EQ(EXVchunk->m_state, ChunkState::AVAILABLE);
                } else {
                    // if the last shard, the chunk should be inuse
                    ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
                }
                ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
                auto vchunk_id = EXVchunk->m_v_chunk_id.value();
                ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

                ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
                ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
            }
        }
    }

    // delete half of the blobs per shard.
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id];
            auto& blob_to_blk_count = shard_blob_ids_map[shard_id];
            for (uint64_t i = 0; i < num_blobs_per_shard / 2; i++) {
                ASSERT_FALSE(blob_to_blk_count.empty());
                auto it = blob_to_blk_count.begin();
                auto blob_id = it->first;
                shard_blob_ids_map_for_deletion[shard_id].insert(blob_id);
                blob_to_blk_count.erase(it);
            }
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // do not seal the last shard and trigger gc mannually to simulate emergent gc
    auto gc_mgr = _obj_inst->gc_manager();
    std::vector< folly::SemiFuture< bool > > futs;

    if (with_crash_recovery) {
        const auto egc_thread_count_per_pdev = HS_BACKEND_DYNAMIC_CONFIG(reserved_chunk_num_per_pdev_for_egc);
#ifdef _PRERELEASE
        // for each emergent gc thread, we simutlate a crash.
        set_basic_flip("simulate_gc_crash_recovery", egc_thread_count_per_pdev * num_pgs);
#endif
        // trigger egc. since we have enabled the above flip, the gc task will return without removing gc_task_meta_blk,
        // so that they will be replayed when recovery
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < egc_thread_count_per_pdev; i++) {
                auto chunk_id = pg_chunks->at(i);
                futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id));
            }
        }
    } else {
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < chunk_num; i++) {
                auto chunk_id = pg_chunks->at(i);
                futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id));
            }
        }
    }

    // wait for all egc completed
    folly::collectAllUnsafe(futs)
        .thenValue([](auto&& results) {
            for (auto const& ok : results) {
                ASSERT_TRUE(ok.hasValue());
                // all egc task should be completed.
                ASSERT_TRUE(ok.value());
            }
        })
        .get();

    futs.clear();

    if (with_crash_recovery) {
        // this will recover gc task
        gc_mgr.reset();
        restart();

        gc_mgr = _obj_inst->gc_manager();
        chunk_selector = _obj_inst->chunk_selector();

        HS_PG_map.clear();
        for (uint64_t i = 1; i <= num_pgs; i++) {
            auto hs_pg = _obj_inst->get_hs_pg(i);
            ASSERT_TRUE(hs_pg != nullptr);
            HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
        }

        // then we gc all the chunks again
        for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
            const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
            for (uint64_t i{0}; i < chunk_num; i++) {
                auto chunk_id = pg_chunks->at(i);
                futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id));
            }
        }

        // wait for all egc completed
        folly::collectAllUnsafe(futs)
            .thenValue([](auto&& results) {
                for (auto const& ok : results) {
                    ASSERT_TRUE(ok.hasValue());
                    // all egc task should be completed.
                    ASSERT_TRUE(ok.value());
                }
            })
            .get();

        futs.clear();
    }

    // verify blob data after gc
    std::map< shard_id_t, std::set< blob_id_t > > remaining_shard_blobs;
    for (const auto& [shard_id, blob_to_blk_count] : shard_blob_ids_map) {
        for (const auto& [blob_id, _] : blob_to_blk_count) {
            remaining_shard_blobs[shard_id].insert(blob_id);
        }
    }
    verify_shard_blobs(remaining_shard_blobs);

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);

            // emergent gc, then chunk is still in use
            ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE)
                << "fail chunk_id=" << chunk_id << ", shard_id=" << shard_id;
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

            ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
            ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        // for each chunk, we have an open shard, which has only header.
        total_blob_occupied_blk_count -= pg_chunk_nums[pg_id];

        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    gc_mgr.reset();
    restart();

    HS_PG_map.clear();

    for (uint64_t i = 1; i <= num_pgs; i++) {
        auto hs_pg = _obj_inst->get_hs_pg(i);
        ASSERT_TRUE(hs_pg != nullptr);
        HS_PG_map[i] = const_cast< HSHomeObject::HS_PG* >(hs_pg);
    }

    chunk_selector = _obj_inst->chunk_selector();
    gc_mgr = _obj_inst->gc_manager();

    verify_shard_blobs(remaining_shard_blobs);

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        // after half blobs have been deleted, the tombstone indexes(half of the total blobs) have been removed by gc
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), pg_blob_id[pg_id] / 2);
        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            // emergent gc, then chunk is still in use
            ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after gc , pg_chunks should changes, the vchunk shoud change to a new pchunk.
            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);

            ASSERT_TRUE(EXVchunk->m_pg_id.has_value());
            ASSERT_EQ(EXVchunk->m_pg_id.value(), pg_id);
        }
    }

    // check pg durable entities
    for (const auto& [pg_id, hs_pg] : HS_PG_map) {
        uint64_t total_blob_occupied_blk_count{0};
        const auto& shard_vec = pg_shard_id_vec[pg_id];
        for (const auto& shard_id : shard_vec) {
            total_blob_occupied_blk_count += 2; /*header and footer*/
            for (const auto& [_, blk_count] : shard_blob_ids_map[shard_id]) {
                total_blob_occupied_blk_count += blk_count;
            }
        }
        // for each chunk, we have an open shard, which has only header.
        total_blob_occupied_blk_count -= pg_chunk_nums[pg_id];

        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, total_blob_occupied_blk_count);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, total_blob_occupied_blk_count);
    }

    // delete remaining blks
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        std::map< shard_id_t, std::set< blob_id_t > > shard_blob_ids_map_for_deletion;
        for (const auto& shard_id : shard_vec) {
            shard_blob_ids_map_for_deletion[shard_id] = remaining_shard_blobs[shard_id];
        }
        del_blobs(pg_id, shard_blob_ids_map_for_deletion);
    }

    // trigger egc for all chunks
    for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
        const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (uint64_t i{0}; i < chunk_num; i++) {
            auto chunk_id = pg_chunks->at(i);
            futs.emplace_back(gc_mgr->submit_gc_task(task_priority::emergent, chunk_id));
        }
    }

    // wait for all egc completed
    folly::collectAllUnsafe(futs)
        .thenValue([](auto&& results) {
            for (auto const& ok : results) {
                ASSERT_TRUE(ok.hasValue());
                // all egc task should be completed
                ASSERT_TRUE(ok.value());
            }
        })
        .get();

    futs.clear();

    // for each chunk in this pg, there is only one shard header
    for (const auto& [pg_id, chunk_num] : pg_chunk_nums) {
        const auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (uint64_t i{0}; i < chunk_num; i++) {
            auto chunk_id = pg_chunks->at(i);
            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);

            // the open shard is not sealed, so there is only the shard header for each shard
            ASSERT_EQ(EXVchunk->get_used_blks(), 1);
        }
    }

    // check vchunk to pchunk for every pg
    for (const auto& [pg_id, shard_vec] : pg_shard_id_vec) {
        auto& hs_pg = HS_PG_map[pg_id];
        // check pg durable entities. only shard header left, and every chunk has a open shard, so
        // total_occupied_blk_count is equal to the num of chunks in this pg since each chunk has a shard header.
        ASSERT_EQ(hs_pg->pg_sb_->total_occupied_blk_count, pg_chunk_nums[pg_id]);
        ASSERT_EQ(hs_pg->durable_entities().total_occupied_blk_count, pg_chunk_nums[pg_id]);

        // after all blobs have been deleted, the pg index table should be empty
        ASSERT_EQ(get_valid_blob_count_in_pg(pg_id), 0);

        auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
        for (const auto& shard_id : shard_vec) {
            auto chunk_opt = _obj_inst->get_shard_p_chunk_id(shard_id);
            ASSERT_TRUE(chunk_opt.has_value());
            auto chunk_id = chunk_opt.value();

            auto EXVchunk = chunk_selector->get_extend_vchunk(chunk_id);
            ASSERT_TRUE(EXVchunk != nullptr);
            ASSERT_EQ(EXVchunk->m_state, ChunkState::INUSE);
            ASSERT_TRUE(EXVchunk->m_v_chunk_id.has_value());
            auto vchunk_id = EXVchunk->m_v_chunk_id.value();

            // after gc , pg_chunks should changes, the vchunk shoud change to a new pchunk.
            ASSERT_EQ(pg_chunks->at(vchunk_id), chunk_id);
        }
    }

#ifdef _PRERELEASE
    remove_flip("simulate_gc_crash_recovery");
#endif

    // TODO:: add more check after we have delete shard implementation
}
// ===================================================================================================
// Issue1 reproduction: CREATE_SHARD stale pchunk race (GC / shard-blob route inconsistency).
//
// Reproduces the production incidents documented in
//   docs/gc_issues/2026-05-29-GC-SHARD-BLOB-ROUTE-INCONSISTENCY-LAGGY-PG.md (PG 39 / PG 3409).
//
// Real 3-process raft cluster. The race is forced on exactly ONE follower ("the laggy replica"):
//
//   1. shard1 is created on vchunk N (-> pchunk A) and filled with blobs.
//   2. The leader seals shard1 (releasing vchunk N) and immediately creates a successor shard2.
//      Because chunks_per_pg == 1, the leader reassigns the SAME vchunk N to shard2; shard2's blocks
//      are allocated on the follower at log-append time, capturing the current pchunk A.
//   3. On the laggy follower, shard2's CREATE_SHARD *commit* is paused right before its captured
//      p_chunk_id is persisted (this models the follower localize/commit reordering seen in prod).
//   4. While paused, the follower runs GC on vchunk N. GC relocates A -> B and remaps the live
//      vchunk->pchunk mapping (N -> B). pchunk A becomes a reserved chunk.
//   5. shard2's commit resumes and persists the STALE captured p_chunk_id = A.
//
// Result on the laggy follower: shard2 is recorded at pchunk A while the live vchunk map says N -> B.
// The other replicas (which did not hit the reorder + GC window) remain consistent at A. This exactly
// matches the production symptom where a single "laggy" replica routes a shard/blob to the wrong chunk.
//
// MUST be run with --chunks_per_pg=1 so the successor shard is forced to reuse the predecessor vchunk.
// On the UNFIXED code this test FAILS (the EXPECT_EQ below trips on the laggy follower).
//
// The pause point is reached via the iomgr flip "issue1_pause_create_shard_commit", so the entire
// reproduction is compiled out of release builds and only runs when the flip is armed under _PRERELEASE.
// ===================================================================================================
#ifdef _PRERELEASE
TEST_F(HomeObjectFixture, Issue1StalePChunkRouteAfterGC) {
    const pg_id_t pg_id = 1;
    const auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >();

    ASSERT_EQ(SISL_OPTIONS["chunks_per_pg"].as< uint64_t >(), 1u)
        << "This reproduction must be run with --chunks_per_pg=1 to force vchunk reuse by the successor shard";

    create_pg(pg_id);
    auto chunk_selector = _obj_inst->chunk_selector();

    if (!am_i_in_pg(pg_id)) {
        // not a member, just keep the sync barriers aligned and leave.
        g_helper->sync(); // arm barrier
        g_helper->sync(); // end barrier
        return;
    }

    // The laggy follower is the non-leader replica number 2 (leader defaults to replica 0).
    const bool i_am_leader = (g_helper->my_replica_id() == get_leader_id(pg_id));
    const bool i_am_repro_follower = (!i_am_leader) && (g_helper->replica_num() == 2);

    // ---- shard1: create and fill with blobs so its chunk holds movable data for GC ----
    auto shard1 = create_shard(pg_id, 64 * Mi, "issue1-shard1");
    ASSERT_NE(shard1.id, 0u);

    std::map< pg_id_t, std::vector< shard_id_t > > shards{{pg_id, {shard1.id}}};
    std::map< pg_id_t, blob_id_t > pg_blob_id{{pg_id, 0}};
    put_blobs(shards, num_blobs_per_shard, pg_blob_id);

    // record this replica's local vchunk N and pchunk A for shard1
    auto vchunk_N = _obj_inst->get_shard_v_chunk_id(shard1.id);
    auto pchunk_A = _obj_inst->get_shard_p_chunk_id(shard1.id);
    ASSERT_TRUE(vchunk_N.has_value());
    ASSERT_TRUE(pchunk_A.has_value());

    // ---- arm the repro flip on exactly one follower so quorum (leader + other follower) is unaffected ----
    if (i_am_repro_follower) {
        HSHomeObject::s_issue1_create_commit_gate.reset();
        set_basic_flip("issue1_pause_create_shard_commit", 1 /* count */, 100 /* percent */);
        LOGINFO("[issue1-repro] armed on follower replica={}, pg={}, vchunk={}, pchunk_A={}",
                g_helper->replica_num(), pg_id, vchunk_N.value(), pchunk_A.value());
    }

    g_helper->sync(); // make sure the hook is armed before the leader drives seal+create

    // ---- leader drives seal(shard1) then create(shard2) back-to-back, WITHOUT per-op sync barriers ----
    shard_id_t shard2_id = INVALID_UINT64_ID;
    run_on_pg_leader(pg_id, [&]() {
        auto tid = generateRandomTraceId();
        auto sealed = _obj_inst->shard_manager()->seal_shard(shard1.id, tid).get();
        RELEASE_ASSERT(!!sealed, "failed to seal shard1");
        auto created = _obj_inst->shard_manager()->create_shard(pg_id, 64 * Mi, "issue1-shard2", tid).get();
        RELEASE_ASSERT(!!created, "failed to create shard2");
        g_helper->set_uint64_id(created.value().id);
        LOGINFO("[issue1-repro] leader sealed shard1=0x{:x} and created shard2=0x{:x}", shard1.id,
                created.value().id);
    });

    // everyone learns shard2 id from IPC
    while ((shard2_id = g_helper->get_uint64_id()) == INVALID_UINT64_ID) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // ---- the laggy follower: wait until shard2 commit is paused, remap its vchunk via GC, then resume ----
    if (i_am_repro_follower) {
        auto& gate = HSHomeObject::s_issue1_create_commit_gate;
        ASSERT_TRUE(gate.wait_until_blocked(std::chrono::seconds(120)))
            << "shard2 CREATE_SHARD commit was never paused on the repro follower";
        LOGINFO("[issue1-repro] follower replica={} sees shard2 commit paused; captured_p_chunk={}, triggering "
                "emergent GC on pchunk={}",
                g_helper->replica_num(), gate.captured_chunk.load(), pchunk_A.value());

        // vchunk N has been released by shard1's seal commit (lower lsn, committed first); relocate it via GC.
        auto fut = _obj_inst->gc_manager()->submit_gc_task(task_priority::emergent, pchunk_A.value());
        bool gc_ok = std::move(fut).get();
        ASSERT_TRUE(gc_ok) << "emergent GC on pchunk=" << pchunk_A.value() << " failed";

        // release the paused commit so the stale captured p_chunk gets persisted into the shard map.
        gate.release();
    }

    // wait for shard2 to be created locally on every member
    while (!_obj_inst->shard_manager()->get_shard(shard2_id, 0).get()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    g_helper->sync();

    // ---- verification: shard2's recorded p_chunk must match the live vchunk->pchunk mapping ----
    auto v2 = _obj_inst->get_shard_v_chunk_id(shard2_id);
    auto p2 = _obj_inst->get_shard_p_chunk_id(shard2_id);
    ASSERT_TRUE(v2.has_value());
    ASSERT_TRUE(p2.has_value());
    ASSERT_EQ(v2.value(), vchunk_N.value()) << "successor shard2 did not reuse shard1's vchunk (need --chunks_per_pg=1)";

    auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
    ASSERT_TRUE(pg_chunks != nullptr);
    auto live_pchunk = pg_chunks->at(v2.value());

    LOGINFO("[issue1-repro] replica={} shard2 vchunk={} stored_p_chunk={} live_p_chunk={}", g_helper->replica_num(),
            v2.value(), p2.value(), live_pchunk);

    // Primary symptom (root cause shared by PG 39 and PG 3409): shard2's recorded p_chunk_id must agree
    // with the live vchunk->pchunk mapping. On the laggy follower it does not.
    EXPECT_EQ(p2.value(), live_pchunk)
        << "ISSUE1 REPRODUCED on replica " << static_cast< int >(g_helper->replica_num())
        << ": shard2 is routed to a STALE pchunk " << p2.value() << " while the live vchunk->pchunk mapping is "
        << live_pchunk << " (shard/blob route inconsistency, see PG 39 / PG 3409)";

    if (i_am_repro_follower) {
        // Secondary symptoms that characterise the two production shapes:
        //
        //  - PG 3409 ("fully-wrong old side"): shard2 (the *current* shard) points entirely at the old
        //    physical chunk, which GC has since turned into an orphaned reserved chunk that no longer
        //    belongs to this pg. Every route through shard2 is on the stale side.
        //
        //  - PG 39  ("old/new split"): the two eras are split across two physical chunks - shard1's data
        //    has been relocated to the NEW chunk (the live vchunk owner), while shard2's route stays on
        //    the OLD chunk.
        auto stale_chunk = chunk_selector->get_extend_vchunk(p2.value());   // old side (e.g. 87)
        auto live_chunk = chunk_selector->get_extend_vchunk(live_pchunk);   // new side (e.g. 90)
        ASSERT_TRUE(stale_chunk != nullptr);
        ASSERT_TRUE(live_chunk != nullptr);

        LOGINFO("[issue1-repro] PG3409-shape: shard2 stale_pchunk={} now belongs_to_pg={} (orphaned reserved chunk); "
                "PG39-shape: old_side={} vs new_side={} for the same vchunk={}",
                p2.value(), stale_chunk->m_pg_id.has_value(), p2.value(), live_pchunk, v2.value());

        // PG 3409: shard2's stale chunk has been orphaned by GC (no longer owned by the pg).
        EXPECT_FALSE(stale_chunk->m_pg_id.has_value())
            << "PG 3409 shape: shard2's stale chunk " << p2.value()
            << " is expected to be an orphaned reserved chunk after GC remap";

        // PG 39: the live vchunk owner (new side) differs from shard2's stale route (old side).
        EXPECT_NE(p2.value(), live_pchunk) << "PG 39 shape: expected an old/new split across two pchunks";
        EXPECT_TRUE(live_chunk->m_pg_id.has_value() && live_chunk->m_pg_id.value() == pg_id &&
                    live_chunk->m_v_chunk_id.has_value() && live_chunk->m_v_chunk_id.value() == v2.value())
            << "the new side " << live_pchunk << " should be the live owner of vchunk " << v2.value();
    }
}

// ===================================================================================================
// Issue2 reproduction: PUT_BLOB written back to a sealed / already-moved shard (late-tail stale route).
//
// Reproduces the production incident documented in
//   docs/gc_issues/2026-05-29-GC-SHARD-BLOB-ROUTE-INCONSISTENCY-LAGGY-PG.md (PG 4616 / Issue 2).
//
// Issue 2 is NOT the multi-shard vchunk contention of Issue 1. Here a SINGLE shard lifecycle fails to
// close: a PUT_BLOB whose admission check already passed (shard still OPEN) commits LATE, after the shard
// was sealed and GC moved the route, so the late commit republishes a STALE old-side pba into the pg index.
// Root cause (see RCA): the request is validated at admission time, but never re-validated at commit time.
//
// Real 3-process raft cluster; the race is forced on exactly ONE follower ("the laggy replica"):
//
//   1. shard1 is created (-> vchunk N -> pchunk A) and filled with blobs (the future "new side" data).
//   2. The leader seals shard1, then admits one more PUT_BLOB into the now-sealed shard via the internal
//      proposer path (_put_blob bypasses the public SEALED admission check, modelling the admission having
//      already raced ahead). This late blob is appended AFTER the seal, so on the follower it commits after
//      the seal commit. Its blocks are allocated at append time on the still-current pchunk A.
//   3. On the laggy follower the late PUT_BLOB *commit* is paused right before its pba is inserted into the
//      pg index.
//   4. While paused, the follower runs GC on chunk A. GC relocates shard1's existing blobs A -> B, updates
//      their index routes to B, and remaps the live vchunk mapping (N -> B). Chunk A becomes orphaned.
//   5. The late PUT_BLOB commit resumes and inserts its STALE pba (on chunk A) into the pg index.
//
// Result on the laggy follower: the pg index ends up with the bulk of blobs on the new side (B) and one
// residual blob still routed to the old side (A) - exactly the PG 4616 "one old-side blob remains" shape.
// The other replicas (no commit reorder + GC window) keep every blob on A, so they stay self-consistent.
//
// MUST be run with --chunks_per_pg=1. On the UNFIXED code this test FAILS on the laggy follower.
//
// The pause point is reached via the iomgr flip "issue2_pause_put_blob_commit", so the entire
// reproduction is compiled out of release builds and only runs when the flip is armed under _PRERELEASE.
// ===================================================================================================
TEST_F(HomeObjectFixture, Issue2StaleBlobRouteAfterSealAndGC) {
    const pg_id_t pg_id = 1;
    const auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >();

    ASSERT_EQ(SISL_OPTIONS["chunks_per_pg"].as< uint64_t >(), 1u)
        << "This reproduction must be run with --chunks_per_pg=1 so the single shard owns a single vchunk";

    create_pg(pg_id);
    auto chunk_selector = _obj_inst->chunk_selector();

    if (!am_i_in_pg(pg_id)) {
        g_helper->sync(); // arm barrier
        g_helper->sync(); // end barrier
        return;
    }

    const bool i_am_leader = (g_helper->my_replica_id() == get_leader_id(pg_id));
    const bool i_am_repro_follower = (!i_am_leader) && (g_helper->replica_num() == 2);

    // ---- shard1: create and fill with blobs (these become the "new side" data after GC) ----
    auto shard1 = create_shard(pg_id, 64 * Mi, "issue2-shard1");
    ASSERT_NE(shard1.id, 0u);

    std::map< pg_id_t, std::vector< shard_id_t > > shards{{pg_id, {shard1.id}}};
    std::map< pg_id_t, blob_id_t > pg_blob_id{{pg_id, 0}};
    put_blobs(shards, num_blobs_per_shard, pg_blob_id);

    auto vchunk_N = _obj_inst->get_shard_v_chunk_id(shard1.id);
    auto pchunk_A = _obj_inst->get_shard_p_chunk_id(shard1.id);
    ASSERT_TRUE(vchunk_N.has_value());
    ASSERT_TRUE(pchunk_A.has_value());

    // ---- arm the repro flip on exactly one follower so quorum (leader + other follower) is unaffected ----
    if (i_am_repro_follower) {
        HSHomeObject::s_issue2_put_commit_gate.reset();
        set_basic_flip("issue2_pause_put_blob_commit", 1 /* count */, 100 /* percent */);
        LOGINFO("[issue2-repro] armed on follower replica={}, pg={}, vchunk={}, pchunk_A={}",
                g_helper->replica_num(), pg_id, vchunk_N.value(), pchunk_A.value());
    }

    g_helper->sync(); // make sure the hook is armed before the leader drives seal + late put

    // ---- leader seals shard1, then admits ONE more blob into the sealed shard (no per-op sync barriers) ----
    blob_id_t late_blob_id = INVALID_UINT64_ID;
    run_on_pg_leader(pg_id, [&]() {
        auto tid = generateRandomTraceId();
        auto sealed = _obj_inst->shard_manager()->seal_shard(shard1.id, tid).get();
        RELEASE_ASSERT(!!sealed, "failed to seal shard1");

        // Admit a late PUT_BLOB via the internal proposer path. The public put() would reject a SEALED shard;
        // calling _put_blob directly models the admission check having already passed before the seal landed.
        // This entry gets a higher lsn than the seal, so on the laggy follower it commits AFTER the seal.
        auto blob = build_blob(num_blobs_per_shard /* deterministic content */);
        auto b = _obj_inst->_put_blob(sealed.value(), std::move(blob), tid).get();
        RELEASE_ASSERT(!!b, "late put_blob admission failed");
        g_helper->set_uint64_id(b.value());
        LOGINFO("[issue2-repro] leader sealed shard1=0x{:x} and admitted late blob_id={}", shard1.id, b.value());
    });

    // everyone learns the late blob id from IPC
    while ((late_blob_id = g_helper->get_uint64_id()) == INVALID_UINT64_ID) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // ---- the laggy follower: wait until the late put commit is paused, remap its vchunk via GC, then resume ----
    if (i_am_repro_follower) {
        auto& gate = HSHomeObject::s_issue2_put_commit_gate;
        ASSERT_TRUE(gate.wait_until_blocked(std::chrono::seconds(120)))
            << "the late PUT_BLOB commit was never paused on the repro follower";
        LOGINFO("[issue2-repro] follower replica={} sees late put commit paused; captured_pba_chunk={}, triggering "
                "emergent GC on pchunk={}",
                g_helper->replica_num(), gate.captured_chunk.load(), pchunk_A.value());

        // shard1's seal commit (lower lsn) already released vchunk N; relocate it via GC so the live mapping
        // moves N: A -> B while the late blob's pba still points at A.
        auto fut = _obj_inst->gc_manager()->submit_gc_task(task_priority::emergent, pchunk_A.value());
        bool gc_ok = std::move(fut).get();
        ASSERT_TRUE(gc_ok) << "emergent GC on pchunk=" << pchunk_A.value() << " failed";

        // release the paused commit so the stale old-side pba gets written into the pg index.
        gate.release();
    }

    // wait for the late blob to be committed locally on every member
    while (am_i_in_pg(pg_id) && !blob_exist(shard1.id, late_blob_id)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    g_helper->sync();

    // ---- verification: every committed blob route must agree with the live vchunk->pchunk mapping ----
    auto pg_chunks = chunk_selector->get_pg_chunks(pg_id);
    ASSERT_TRUE(pg_chunks != nullptr);
    auto live_pchunk = pg_chunks->at(vchunk_N.value());

    auto index_table = _obj_inst->get_hs_pg(pg_id)->index_table_;
    ASSERT_TRUE(index_table != nullptr);

    auto late_route = _obj_inst->get_blob_from_index_table(index_table, shard1.id, late_blob_id);
    ASSERT_TRUE(late_route.hasValue()) << "late blob missing from index";
    auto late_pchunk = late_route.value().chunk_num();

    LOGINFO("[issue2-repro] replica={} late_blob={} stored_pchunk={} live_pchunk={}", g_helper->replica_num(),
            late_blob_id, late_pchunk, live_pchunk);

    // Primary symptom: the late blob must be routed to the live vchunk owner. On the laggy follower it is not.
    EXPECT_EQ(late_pchunk, live_pchunk)
        << "ISSUE2 REPRODUCED on replica " << static_cast< int >(g_helper->replica_num()) << ": late blob "
        << late_blob_id << " is routed to a STALE pchunk " << late_pchunk
        << " while the live vchunk->pchunk mapping is " << live_pchunk
        << " (a sealed/already-moved shard accepted a late write, see PG 4616 / Issue 2)";

    if (i_am_repro_follower) {
        // PG 4616 "late-tail" shape: the bulk of blobs sit on the new side (B), and only the late blob
        // remains on the old side (A). Count them to make the split explicit.
        uint64_t on_new_side = 0, on_old_side = 0;
        for (blob_id_t b = 0; b < static_cast< blob_id_t >(num_blobs_per_shard); ++b) {
            auto r = _obj_inst->get_blob_from_index_table(index_table, shard1.id, b);
            if (!r.hasValue()) continue;
            if (r.value().chunk_num() == live_pchunk)
                ++on_new_side;
            else if (r.value().chunk_num() == pchunk_A.value())
                ++on_old_side;
        }

        auto stale_chunk = chunk_selector->get_extend_vchunk(late_pchunk);
        ASSERT_TRUE(stale_chunk != nullptr);

        LOGINFO("[issue2-repro] PG4616-shape: new_side(pchunk={}) holds {} blobs, old_side(pchunk={}) holds the late "
                "tail; late blob stale_pchunk={} belongs_to_pg={} (orphaned reserved chunk)",
                live_pchunk, on_new_side, pchunk_A.value(), late_pchunk, stale_chunk->m_pg_id.has_value());

        // The late blob's chunk is the old side, distinct from the live owner (the old/new split).
        EXPECT_NE(late_pchunk, live_pchunk) << "PG 4616 shape: expected a late old-side tail distinct from new side";
        // GC moved shard1's bulk data onto the new side.
        EXPECT_GT(on_new_side, 0u) << "expected the bulk of blobs to have been relocated to the new side by GC";
        // The old side that the late blob points at has been orphaned by GC.
        EXPECT_FALSE(stale_chunk->m_pg_id.has_value())
            << "PG 4616 shape: the late blob's old-side chunk " << late_pchunk
            << " is expected to be an orphaned reserved chunk after GC remap";
    }
}
#endif // _PRERELEASE
