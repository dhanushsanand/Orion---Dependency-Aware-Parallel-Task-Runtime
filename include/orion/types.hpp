#pragma once
#include <cstdint>
#include <vector>
#include <string>

namespace orion {

using Bytes   = std::vector<std::uint8_t>;
using NodeId  = std::uint16_t;
using TaskId  = std::uint64_t;

/* ObjectId layout:  [owner:12][creator:12][counter:40]
 * owner   = node that holds the value (where the producing task runs)
 * creator = node that allocated the id (guarantees global uniqueness)        */
using ObjectId = std::uint64_t;

constexpr ObjectId make_object_id(NodeId owner, NodeId creator, std::uint64_t counter) {
    return (static_cast<ObjectId>(owner & 0xFFF) << 52) |
           (static_cast<ObjectId>(creator & 0xFFF) << 40) |
           (counter & 0xFFFFFFFFFFull);
}
constexpr NodeId object_owner(ObjectId id)   { return static_cast<NodeId>(id >> 52); }
constexpr NodeId object_creator(ObjectId id) { return static_cast<NodeId>((id >> 40) & 0xFFF); }

struct NodeAddress {
    std::string host;
    std::uint16_t port{};
    std::string str() const { return host + ":" + std::to_string(port); }
};

} // namespace orion
