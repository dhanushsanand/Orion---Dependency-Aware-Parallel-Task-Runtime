#pragma once
/* ObjectStore: thread-safe, in-memory, callback-driven.
 *
 *  put(id, bytes)       publishes a value; wakes blocking getters and fires
 *                       every callback registered with on_ready(). Callbacks
 *                       run outside the lock, exactly once.
 *  get(id)              blocks on a condition variable (no spinning).
 *  on_ready(id, cb)     register interest; cb fires immediately if the value
 *                       already exists, otherwise at put() time.
 *
 * The map is sharded; each shard has its own mutex + condvar so unrelated
 * objects never contend. */
#include "types.hpp"
#include <chrono>
#include <condition_variable>
#include <expected>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace orion {

enum class StoreError { Timeout };

class ObjectStore {
public:
    using ReadyCallback = std::function<void(ObjectId, const Bytes&)>;

    explicit ObjectStore(std::size_t shards = 16);

    void put(ObjectId id, Bytes value);
    bool contains(ObjectId id) const;
    std::optional<Bytes> try_get(ObjectId id) const;

    /* blocking get; timeout 0 = wait forever */
    std::expected<Bytes, StoreError>
    get(ObjectId id, std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) const;

    void on_ready(ObjectId id, ReadyCallback cb);
    std::size_t size() const;

private:
    struct Entry {
        std::optional<Bytes> value;
        std::vector<ReadyCallback> waiters;
    };
    struct Shard {
        mutable std::mutex mu;
        mutable std::condition_variable cv;
        std::unordered_map<ObjectId, Entry> entries;
    };
    Shard& shard(ObjectId id) const { return shards_[id % shards_.size()]; }
    mutable std::vector<Shard> shards_;
};

} // namespace orion
