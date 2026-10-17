#pragma once
/* Membership: who is in the cluster, their address, last reported load and
 * last time we heard from them. */
#include "message.hpp"
#include <chrono>
#include <map>
#include <mutex>

namespace orion::cluster {

struct Member {
    NodeInfo info;
    std::uint32_t load{};        /* last reported by heartbeat */
    std::uint32_t inflight{};    /* tasks we sent since that heartbeat */
    std::chrono::steady_clock::time_point last_seen;
    bool alive{true};
};

class Membership {
public:
    explicit Membership(NodeId self) : self_(self) {}

    void upsert(const NodeInfo& n);
    void heartbeat(NodeId id, std::uint32_t load);           /* also resets inflight */
    void note_submit(NodeId id);                             /* inflight++ */
    void expire(std::chrono::milliseconds max_silence);     /* marks silent peers dead */
    std::vector<Member> alive() const;                       /* excludes self */
    std::vector<NodeInfo> all_info() const;                  /* includes self */
    std::optional<NodeAddress> address(NodeId id) const;
    NodeId self() const { return self_; }
    std::size_t size() const { std::lock_guard lk(mu_); return members_.size(); }

private:
    NodeId self_;
    mutable std::mutex mu_;
    std::map<NodeId, Member> members_;
};

} // namespace orion::cluster
