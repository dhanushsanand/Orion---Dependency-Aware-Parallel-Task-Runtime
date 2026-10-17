#include "orion/cluster/scheduler.hpp"
#include <limits>

namespace orion::cluster {

Placement ClusterScheduler::place(const TaskSpec& t, const Membership& m, std::uint32_t self_load) const
{
    auto locality_penalty = [&](NodeId n) {
        double p = 0;
        for (ObjectId a : t.args) if (object_owner(a) != n) p += w_;
        return p;
    };
    Placement best{ m.self(), self_load + locality_penalty(m.self()) };
    for (const Member& peer : m.alive()) {
        double s = peer.load + peer.inflight + locality_penalty(peer.info.id);   /* inflight: tasks sent since its last heartbeat */
        if (s < best.score) best = { peer.info.id, s };     /* strict: ties stay local */
    }
    return best;
}

} // namespace orion::cluster
