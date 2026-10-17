#pragma once
/* ClusterScheduler: placement policy. Score = reported load + a locality
 * penalty for every input the candidate node does not own; the lowest score
 * wins, ties go to the local node (no network hop). */
#include "membership.hpp"

namespace orion::cluster {

struct Placement { NodeId node; double score; };

class ClusterScheduler {
public:
    explicit ClusterScheduler(double locality_weight = 2.0) : w_(locality_weight) {}
    Placement place(const TaskSpec& t, const Membership& m, std::uint32_t self_load) const;
private:
    double w_;
};

} // namespace orion::cluster
