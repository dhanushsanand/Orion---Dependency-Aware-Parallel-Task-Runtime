#pragma once
/* Transport: the one interface the cluster layer needs from the network.
 * Backends: TcpTransport (built-in, length-prefixed frames) and GrpcTransport
 * (gRPC + Protobuf, built with WITH_GRPC=1). */
#include "message.hpp"
#include <functional>
#include <memory>

namespace orion::cluster {

class Transport {
public:
    using Handler = std::function<void(const Message&)>;
    virtual ~Transport() = default;

    virtual void start(NodeId self, const NodeAddress& listen, Handler h) = 0;
    virtual void stop() = 0;
    /* idempotent: establishes an outbound channel to `peer` */
    virtual bool connect(NodeId peer, const NodeAddress& addr) = 0;
    virtual bool send(NodeId peer, const Message& m) = 0;
    virtual bool connected(NodeId peer) const = 0;
};

std::unique_ptr<Transport> make_tcp_transport();
#ifdef ORION_WITH_GRPC
std::unique_ptr<Transport> make_grpc_transport();
#endif

} // namespace orion::cluster
