/* grpc_transport.cpp — gRPC + Protobuf backend (built with `make WITH_GRPC=1`).
 *
 * Each node hosts a `Node` gRPC service and keeps one channel + stub per peer.
 * Messages are mapped 1:1 between cluster::Message and orion.rpc.Envelope.
 *
 * NOTE: this file was written against the gRPC 1.5x / Protobuf 3.2x C++ API
 * and could not be compiled in the authoring environment (no gRPC toolchain
 * available), so expect to touch up minor API differences on first build. */
#ifdef ORION_WITH_GRPC
#include "orion/cluster/transport.hpp"
#include "orion.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <map>
#include <mutex>

namespace orion::cluster {

namespace {

rpc::Envelope to_proto(const Message& m)
{
    rpc::Envelope e;
    e.set_from(m.from);
    auto fill_node = [](rpc::NodeInfo* n, const NodeInfo& i) { n->set_id(i.id); n->set_host(i.addr.host); n->set_port(i.addr.port); };
    switch (m.type) {
    case MsgType::Hello:      if (!m.members.empty()) fill_node(e.mutable_hello()->mutable_node(), m.members[0]); break;
    case MsgType::Members:    for (auto& n : m.members) fill_node(e.mutable_members()->add_members(), n); break;
    case MsgType::Heartbeat:  e.mutable_heartbeat()->set_load(m.load); break;
    case MsgType::SubmitTask: {
        auto* t = e.mutable_submit_task();
        t->set_id(m.task.id); t->set_fn(m.task.fn); t->set_output(m.task.output); t->set_submitter(m.task.submitter);
        for (auto a : m.task.args) t->add_args(a);
        break;
    }
    case MsgType::GetObject:  e.mutable_get_object()->set_object(m.object); break;
    case MsgType::ObjectData: e.mutable_object_data()->set_object(m.object);
                              e.mutable_object_data()->set_data(m.data.data(), m.data.size()); break;
    }
    return e;
}

Message from_proto(const rpc::Envelope& e)
{
    Message m;
    m.from = static_cast<NodeId>(e.from());
    auto node = [](const rpc::NodeInfo& n) { return NodeInfo{ static_cast<NodeId>(n.id()), { n.host(), static_cast<std::uint16_t>(n.port()) } }; };
    switch (e.body_case()) {
    case rpc::Envelope::kHello:      m.type = MsgType::Hello; m.members.push_back(node(e.hello().node())); break;
    case rpc::Envelope::kMembers:    m.type = MsgType::Members; for (auto& n : e.members().members()) m.members.push_back(node(n)); break;
    case rpc::Envelope::kHeartbeat:  m.type = MsgType::Heartbeat; m.load = e.heartbeat().load(); break;
    case rpc::Envelope::kSubmitTask: {
        m.type = MsgType::SubmitTask;
        const auto& t = e.submit_task();
        m.task.id = t.id(); m.task.fn = t.fn(); m.task.output = t.output(); m.task.submitter = static_cast<NodeId>(t.submitter());
        for (auto a : t.args()) m.task.args.push_back(a);
        break;
    }
    case rpc::Envelope::kGetObject:  m.type = MsgType::GetObject; m.object = e.get_object().object(); break;
    case rpc::Envelope::kObjectData: m.type = MsgType::ObjectData; m.object = e.object_data().object();
                                     m.data.assign(e.object_data().data().begin(), e.object_data().data().end()); break;
    default: break;
    }
    return m;
}

class NodeService final : public rpc::Node::Service {
public:
    explicit NodeService(Transport::Handler h) : handler_(std::move(h)) {}
    grpc::Status Send(grpc::ServerContext*, const rpc::Envelope* req, rpc::Ack*) override {
        handler_(from_proto(*req));
        return grpc::Status::OK;
    }
private:
    Transport::Handler handler_;
};

class GrpcTransport final : public Transport {
public:
    ~GrpcTransport() override { stop(); }

    void start(NodeId self, const NodeAddress& listen, Handler h) override {
        self_ = self;
        service_ = std::make_unique<NodeService>(std::move(h));
        grpc::ServerBuilder b;
        b.AddListeningPort(listen.str(), grpc::InsecureServerCredentials());
        b.RegisterService(service_.get());
        server_ = b.BuildAndStart();
        if (!server_) throw std::runtime_error("GrpcTransport: cannot listen on " + listen.str());
    }
    void stop() override {
        if (server_) { server_->Shutdown(); server_.reset(); }
        std::lock_guard lk(mu_);
        stubs_.clear();
    }
    bool connect(NodeId peer, const NodeAddress& addr) override {
        std::lock_guard lk(mu_);
        if (stubs_.count(peer)) return true;
        auto ch = grpc::CreateChannel(addr.str(), grpc::InsecureChannelCredentials());
        stubs_[peer] = rpc::Node::NewStub(ch);
        return true;                      /* gRPC connects lazily; failures surface in send() */
    }
    bool send(NodeId peer, const Message& m) override {
        rpc::Node::Stub* stub;
        {
            std::lock_guard lk(mu_);
            auto it = stubs_.find(peer);
            if (it == stubs_.end()) return false;
            stub = it->second.get();
        }
        grpc::ClientContext ctx;
        rpc::Ack ack;
        return stub->Send(&ctx, to_proto(m), &ack).ok();
    }
    bool connected(NodeId peer) const override { std::lock_guard lk(mu_); return stubs_.count(peer) > 0; }

private:
    NodeId self_{};
    std::unique_ptr<NodeService> service_;
    std::unique_ptr<grpc::Server> server_;
    mutable std::mutex mu_;
    std::map<NodeId, std::unique_ptr<rpc::Node::Stub>> stubs_;
};

} // namespace

std::unique_ptr<Transport> make_grpc_transport() { return std::make_unique<GrpcTransport>(); }

} // namespace orion::cluster
#endif
