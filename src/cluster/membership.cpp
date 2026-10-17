#include "orion/cluster/membership.hpp"

namespace orion::cluster {

void Membership::upsert(const NodeInfo& n)
{
    std::lock_guard lk(mu_);
    auto& m = members_[n.id];
    m.info = n;
    m.last_seen = std::chrono::steady_clock::now();
    m.alive = true;
}

void Membership::heartbeat(NodeId id, std::uint32_t load)
{
    std::lock_guard lk(mu_);
    auto it = members_.find(id);
    if (it == members_.end()) return;
    it->second.load = load;
    it->second.inflight = 0;
    it->second.last_seen = std::chrono::steady_clock::now();
    it->second.alive = true;
}

void Membership::note_submit(NodeId id)
{
    std::lock_guard lk(mu_);
    auto it = members_.find(id);
    if (it != members_.end()) it->second.inflight++;
}

void Membership::expire(std::chrono::milliseconds max_silence)
{
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lk(mu_);
    for (auto& [id, m] : members_)
        if (id != self_ && now - m.last_seen > max_silence) m.alive = false;
}

std::vector<Member> Membership::alive() const
{
    std::lock_guard lk(mu_);
    std::vector<Member> v;
    for (auto& [id, m] : members_) if (id != self_ && m.alive) v.push_back(m);
    return v;
}

std::vector<NodeInfo> Membership::all_info() const
{
    std::lock_guard lk(mu_);
    std::vector<NodeInfo> v;
    for (auto& [_, m] : members_) if (m.alive) v.push_back(m.info);
    return v;
}

std::optional<NodeAddress> Membership::address(NodeId id) const
{
    std::lock_guard lk(mu_);
    auto it = members_.find(id);
    if (it == members_.end()) return std::nullopt;
    return it->second.info.addr;
}

} // namespace orion::cluster
