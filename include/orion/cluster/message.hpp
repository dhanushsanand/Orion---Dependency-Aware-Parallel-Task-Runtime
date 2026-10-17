#pragma once
/* Wire messages exchanged between nodes. The TCP backend serialises these
 * with the tiny binary encoder below; the gRPC backend maps them 1:1 onto
 * proto/orion.proto. */
#include "../task.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace orion::cluster {

struct NodeInfo {
    NodeId      id{};
    NodeAddress addr;
};

enum class MsgType : std::uint8_t {
    Hello      = 1,   /* new node -> seed: here I am           */
    Members    = 2,   /* seed -> everyone: current member list */
    Heartbeat  = 3,   /* periodic load report                  */
    SubmitTask = 4,   /* scheduler -> executor                 */
    GetObject  = 5,   /* requester -> owner                    */
    ObjectData = 6,   /* owner -> requester                    */
};

struct Message {
    MsgType type{};
    NodeId  from{};
    std::vector<NodeInfo> members;   /* Hello (1 entry) / Members */
    std::uint32_t load{};            /* Heartbeat */
    TaskSpec task;                   /* SubmitTask */
    ObjectId object{};               /* GetObject / ObjectData */
    Bytes    data;                   /* ObjectData */
};

Bytes encode(const Message& m);
std::optional<Message> decode(const Bytes& b);

} // namespace orion::cluster
