#include "mock_homeobject.hpp"

namespace homeobject {
using namespace std::chrono_literals;
constexpr auto disk_latency = 15ms;

void MockHomeObject::put(shard_id shard, Blob const& blob, id_cb cb) {
    std::thread([this, shard, blob, cb]() {
        std::this_thread::sleep_for(disk_latency);
        blob_id id;
        {
            auto lg = std::scoped_lock(_data_lock);
            _shards.insert(shard);
            id = _cur_blob_id;
            auto [it, happened] = _in_memory_disk.emplace(id, blob);
            if (happened) { _cur_blob_id++; }
        }
        cb(id, std::nullopt);
    }).detach();
}

void MockHomeObject::get(shard_id shard, blob_id const& blob, uint64_t off, uint64_t len, get_cb cb) const {
    std::thread([this, shard, blob, cb]() {
        BlobError err = BlobError::OK;
        Blob ret;
        [&] {
            std::this_thread::sleep_for(disk_latency);
            auto lg = std::scoped_lock(_data_lock);
            if (auto const it = _shards.find(shard); it == _shards.end()) {
                err = BlobError::UNKNOWN_SHARD;
                return;
            }
            auto it = _in_memory_disk.end();
            if (it = _in_memory_disk.find(blob); it == _in_memory_disk.end()) {
                err = BlobError::UNKNOWN_BLOB;
                return;
            }

            auto const& read_blob = it->second;
            ret.body = sisl::io_blob(read_blob.body.size);
            ret.user_key = read_blob.user_key;
            ret.object_off = read_blob.object_off;
            std::memcpy(reinterpret_cast< void* >(ret.body.bytes), reinterpret_cast< void* >(read_blob.body.bytes),
                        ret.body.size);
        }();

        if (err == BlobError::OK) {
            cb(ret, std::nullopt);
        } else {
            cb(err, std::nullopt);
        }
    }).detach();
}
void MockHomeObject::del(shard_id shard, blob_id const& blob, BlobManager::ok_cb cb) {
    std::thread([this, shard, blob, cb]() {
        BlobError err = BlobError::OK;
        [&] {
            auto lg = std::scoped_lock(_data_lock);
            if (auto const it = _shards.find(shard); it == _shards.end()) {
                err = BlobError::UNKNOWN_SHARD;
                return;
            }
            auto it = _in_memory_disk.end();
            if (it = _in_memory_disk.find(blob); it == _in_memory_disk.end()) {
                err = BlobError::UNKNOWN_BLOB;
                return;
            }

            _in_memory_disk.erase(blob);
        }();

        cb(err, std::nullopt);
    }).detach();
}

} // namespace homeobject
