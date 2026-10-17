#include "orion/runtime.hpp"
#include <format>
#include <iostream>

namespace orion {

using cluster::Message;
using cluster::MsgType;

Runtime::Runtime(RuntimeConfig cfg)
    : self_(cfg.cluster ? cfg.cluster->id : NodeId{0}),
      pool_(cfg.workers),
      tasks_(store_, pool_, registry_),
      ccfg_(std::move(cfg.cluster)),
      membership_(self_)
{
    if (!ccfg_) return;

    transport_ = ccfg_->transport ? ccfg_->transport : std::shared_ptr<cluster::Transport>(cluster::make_tcp_transport());
    membership_.upsert({ self_, ccfg_->listen });
    tasks_.set_fetch_hook([this](ObjectId id) { request_object(id); });
    transport_->start(self_, ccfg_->listen, [this](const Message& m) { on_message(m); });

    if (ccfg_->seed) {
        membership_.upsert(*ccfg_->seed);
        if (!transport_->connect(ccfg_->seed->id, ccfg_->seed->addr))
            throw std::runtime_error("cannot reach seed " + ccfg_->seed->addr.str());
        Message hello{ .type = MsgType::Hello, .from = self_, .members = { { self_, ccfg_->listen } } };
        transport_->send(ccfg_->seed->id, hello);
    }
    heartbeat_thread_ = std::jthread([this](std::stop_token st) { heartbeat_loop(st); });
}

Runtime::~Runtime()
{
    if (heartbeat_thread_.joinable()) {
        { std::lock_guard lk(members_cv_mu_); heartbeat_thread_.request_stop(); }   /* under the mutex: no lost wake-up */
        members_cv_.notify_all();
        heartbeat_thread_.join();
    }
    if (transport_) transport_->stop();
}

ObjectId Runtime::new_object_id(NodeId owner)
{
    return make_object_id(owner, self_, counter_.fetch_add(1, std::memory_order_relaxed) + 1);
}

/* ------------------------------------------------------------ submission */
ObjectId Runtime::submit_spec(TaskSpec spec, RawFn body)
{
    spec.id = (static_cast<TaskId>(self_) << 48) | task_counter_.fetch_add(1, std::memory_order_relaxed);
    spec.submitter = self_;

    NodeId target = self_;
    if (transport_ && !body)                       /* only named functions can travel */
        target = scheduler_.place(spec, membership_, load()).node;

    spec.output = new_object_id(target);
    if (target == self_) {
        tasks_.submit(std::move(spec), std::move(body));
        return spec.output;
    }
    Message m{ .type = MsgType::SubmitTask, .from = self_, .task = spec };
    if (transport_->send(target, m)) {
        remote_submits_++;
        membership_.note_submit(target);
        return spec.output;
    }
    /* peer unreachable: run it here instead (re-own the output) */
    spec.output = new_object_id(self_);
    tasks_.submit(std::move(spec), nullptr);
    return spec.output;
}

Bytes Runtime::get_raw(ObjectId id, std::chrono::milliseconds timeout)
{
    if (transport_ && object_owner(id) != self_ && !store_.contains(id)) request_object(id);
    auto env = store_.get(id, timeout);
    if (!env) throw TaskError(std::format("timeout waiting for object {:#x}", id));
    if (envelope::is_error(*env)) throw TaskError(envelope::message(*env));
    return envelope::payload(*env);
}

void Runtime::wait_all()
{
    for (;;) {
        pool_.drain();
        if (tasks_.waiting() == 0 && pool_.pending() == 0 && pool_.running() == 0) return;
        /* tasks are waiting on inputs that will arrive via callbacks; park on
         * the pool again once those callbacks dispatch them */
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

std::uint32_t Runtime::load() const
{
    return static_cast<std::uint32_t>(pool_.pending() + pool_.running() + tasks_.waiting());
}

/* --------------------------------------------------------------- cluster */
void Runtime::request_object(ObjectId id)
{
    NodeId owner = object_owner(id);
    if (!transport_ || owner == self_) return;
    {
        std::lock_guard lk(fetch_mu_);
        if (!inflight_fetches_.insert(id).second) return;        /* already asked */
    }
    Message m{ .type = MsgType::GetObject, .from = self_, .object = id };
    transport_->send(owner, m);
    object_fetches_++;
}

void Runtime::broadcast_members()
{
    Message m{ .type = MsgType::Members, .from = self_, .members = membership_.all_info() };
    for (const auto& peer : membership_.alive()) {
        transport_->connect(peer.info.id, peer.info.addr);
        transport_->send(peer.info.id, m);
    }
}

void Runtime::on_message(const Message& m)
{
    switch (m.type) {
    case MsgType::Hello:
        for (auto& n : m.members) { membership_.upsert(n); transport_->connect(n.id, n.addr); }
        broadcast_members();
        members_cv_.notify_all();
        break;
    case MsgType::Members:
        for (auto& n : m.members) {
            if (n.id == self_) continue;
            membership_.upsert(n);
            transport_->connect(n.id, n.addr);
        }
        members_cv_.notify_all();
        break;
    case MsgType::Heartbeat:
        membership_.heartbeat(m.from, m.load);
        break;
    case MsgType::SubmitTask:
        tasks_.submit(m.task, nullptr);                 /* missing inputs are fetched via the hook */
        break;
    case MsgType::GetObject: {
        NodeId requester = m.from;
        store_.on_ready(m.object, [this, requester](ObjectId id, const Bytes& data) {
            Message reply{ .type = MsgType::ObjectData, .from = self_, .object = id, .data = data };
            transport_->send(requester, reply);
        });
        break;
    }
    case MsgType::ObjectData:
        { std::lock_guard lk(fetch_mu_); inflight_fetches_.erase(m.object); }
        store_.put(m.object, m.data);                  /* wakes getters, fires task callbacks */
        break;
    }
}

void Runtime::heartbeat_loop(std::stop_token st)
{
    std::unique_lock lk(members_cv_mu_);
    while (!st.stop_requested()) {
        lk.unlock();
        membership_.expire(ccfg_->expire_after);
        Message hb{ .type = MsgType::Heartbeat, .from = self_, .load = load() };
        for (const auto& peer : membership_.alive()) transport_->send(peer.info.id, hb);
        lk.lock();
        members_cv_.wait_for(lk, ccfg_->heartbeat, [&] { return st.stop_requested(); });
    }
}

bool Runtime::wait_for_members(std::size_t n, std::chrono::milliseconds timeout)
{
    std::unique_lock lk(members_cv_mu_);
    return members_cv_.wait_for(lk, timeout, [&] { return membership_.size() >= n; });
}

} // namespace orion
