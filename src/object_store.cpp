#include "orion/object_store.hpp"
#include <utility>

namespace orion {

ObjectStore::ObjectStore(std::size_t shards) : shards_(shards ? shards : 1) {}

void ObjectStore::put(ObjectId id, Bytes value)
{
    std::vector<ReadyCallback> fire;
    Bytes copy;
    {
        Shard& s = shard(id);
        std::lock_guard lk(s.mu);
        Entry& e = s.entries[id];
        if (e.value) return;                      /* objects are immutable: first put wins */
        e.value = std::move(value);
        fire.swap(e.waiters);
        if (!fire.empty()) copy = *e.value;
        s.cv.notify_all();
    }
    for (auto& cb : fire) cb(id, copy);           /* outside the lock: callbacks may re-enter */
}

bool ObjectStore::contains(ObjectId id) const
{
    Shard& s = shard(id);
    std::lock_guard lk(s.mu);
    auto it = s.entries.find(id);
    return it != s.entries.end() && it->second.value.has_value();
}

std::optional<Bytes> ObjectStore::try_get(ObjectId id) const
{
    Shard& s = shard(id);
    std::lock_guard lk(s.mu);
    auto it = s.entries.find(id);
    if (it == s.entries.end()) return std::nullopt;
    return it->second.value;
}

std::expected<Bytes, StoreError>
ObjectStore::get(ObjectId id, std::chrono::milliseconds timeout) const
{
    Shard& s = shard(id);
    std::unique_lock lk(s.mu);
    auto ready = [&] { auto it = s.entries.find(id); return it != s.entries.end() && it->second.value; };
    if (timeout.count() == 0) s.cv.wait(lk, ready);
    else if (!s.cv.wait_for(lk, timeout, ready)) return std::unexpected(StoreError::Timeout);
    return *s.entries.find(id)->second.value;
}

void ObjectStore::on_ready(ObjectId id, ReadyCallback cb)
{
    Bytes copy;
    {
        Shard& s = shard(id);
        std::lock_guard lk(s.mu);
        Entry& e = s.entries[id];
        if (!e.value) { e.waiters.push_back(std::move(cb)); return; }
        copy = *e.value;
    }
    cb(id, copy);
}

std::size_t ObjectStore::size() const
{
    std::size_t n = 0;
    for (auto& s : shards_) {
        std::lock_guard lk(s.mu);
        for (auto& [_, e] : s.entries) n += e.value.has_value();
    }
    return n;
}

} // namespace orion
