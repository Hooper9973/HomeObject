#include "homeobject.hpp"

namespace homeobject {

uint64_t ShardManager::max_shard_size() { return Gi; }

ShardManager::Result< ShardInfo > HSHomeObject::_create_shard(pg_id pg_owner, uint64_t size_bytes) {
    return folly::makeUnexpected(ShardError::UNKNOWN_PG);
}

ShardManager::Result< ShardInfo > HSHomeObject::_seal_shard(shard_id id) {
    return folly::makeUnexpected(ShardError::UNKNOWN_SHARD);
}

} // namespace homeobject