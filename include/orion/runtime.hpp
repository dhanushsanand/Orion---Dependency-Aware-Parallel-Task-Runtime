#pragma once
/* Runtime: the user-facing facade.
 *
 *   orion::Runtime rt;                                  // single process
 *   rt.define<int(int,int)>("add", [](int a,int b){ return a+b; });
 *   auto x = rt.put(2), y = rt.put(3);
 *   auto z = rt.submit<int>("add", x, y);               // runs when x,y ready
 *   auto w = rt.submit<int>("add", z, 10);              // literal -> put()
 *   int result = w.get();
 *
 * Give the config a ClusterConfig and the same code schedules across nodes. */
#include "cluster/membership.hpp"
#include "cluster/scheduler.hpp"
#include "cluster/transport.hpp"
#include "object_store.hpp"
#include "task_manager.hpp"
#include "worker_pool.hpp"
#include <atomic>
#include <chrono>
#include <optional>
#include <thread>
#include <unordered_set>

namespace orion {

class Runtime;

template <class T>
class ObjectRef {
public:
    ObjectRef() = default;
    ObjectId id() const { return id_; }
    T get(std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) const;
private:
    friend class Runtime;
    ObjectRef(ObjectId id, Runtime* rt) : id_(id), rt_(rt) {}
    ObjectId id_{};
    Runtime* rt_{};
};

template <class T> struct is_object_ref : std::false_type {};
template <class T> struct is_object_ref<ObjectRef<T>> : std::true_type {};

struct ClusterConfig {
    NodeId id{1};
    NodeAddress listen{"127.0.0.1", 7000};
    std::optional<cluster::NodeInfo> seed;                 /* nullopt: this node is the seed */
    std::chrono::milliseconds heartbeat{300};
    std::chrono::milliseconds expire_after{3000};
    std::shared_ptr<cluster::Transport> transport;         /* nullptr: built-in TCP */
};

struct RuntimeConfig {
    std::size_t workers{std::thread::hardware_concurrency()};
    std::optional<ClusterConfig> cluster;
};

class Runtime {
public:
    explicit Runtime(RuntimeConfig cfg = {});
    ~Runtime();
    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;

    /* ---- functions ---- */
    template <class Sig, class F> void define(std::string name, F&& f) {
        registry_.define<Sig>(std::move(name), std::forward<F>(f));
    }

    /* ---- objects ---- */
    template <Encodable T> ObjectRef<T> put(const T& v) {
        ObjectId id = new_object_id(self_);
        store_.put(id, envelope::ok(Codec<T>::encode(v)));
        return ObjectRef<T>(id, this);
    }
    template <class T> T get(const ObjectRef<T>& r, std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
        return Codec<T>::decode(get_raw(r.id(), timeout));
    }
    /* envelope-unwrapped bytes; throws TaskError if the producing task failed */
    Bytes get_raw(ObjectId id, std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

    /* ---- tasks ---- */
    /* Registered function by name: may run on any node. Args are ObjectRefs or plain values. */
    template <class R, class... Args>
    ObjectRef<R> submit(const std::string& fn, Args&&... args) {
        TaskSpec spec; spec.fn = fn; spec.args = { to_id(std::forward<Args>(args))... };
        return ObjectRef<R>(submit_spec(std::move(spec), nullptr), this);
    }
    /* Closure: always runs on this node (closures cannot be shipped). */
    template <class F, class... Ts>
    auto submit_local(F&& f, const ObjectRef<Ts>&... args)
        -> ObjectRef<std::invoke_result_t<F, Ts...>> {
        using R = std::invoke_result_t<F, Ts...>;
        TaskSpec spec; spec.args = { args.id()... };
        RawFn body = detail::SigTraits<R(Ts...)>::wrap(std::forward<F>(f));
        return ObjectRef<R>(submit_spec(std::move(spec), std::move(body)), this);
    }

    /* block until no task on this node is waiting or running */
    void wait_all();

    /* ---- cluster ---- */
    NodeId node_id() const { return self_; }
    bool is_cluster() const { return transport_ != nullptr; }
    std::vector<cluster::NodeInfo> members() const { return membership_.all_info(); }
    bool wait_for_members(std::size_t n, std::chrono::milliseconds timeout);
    std::uint32_t load() const;
    TaskStats stats() const { return tasks_.stats(); }
    std::uint64_t remote_submits() const { return remote_submits_.load(); }
    std::uint64_t object_fetches() const { return object_fetches_.load(); }

private:
    template <class T> ObjectId to_id(T&& v) {
        if constexpr (is_object_ref<std::decay_t<T>>::value) return v.id();
        else return put(std::decay_t<T>(std::forward<T>(v))).id();
    }

    ObjectId new_object_id(NodeId owner);
    ObjectId submit_spec(TaskSpec spec, RawFn body);
    void request_object(ObjectId id);
    void on_message(const cluster::Message& m);
    void broadcast_members();
    void heartbeat_loop(std::stop_token st);

    NodeId self_{0};
    std::atomic<std::uint64_t> counter_{0}, task_counter_{0};
    std::atomic<std::uint64_t> remote_submits_{0}, object_fetches_{0};

    ObjectStore store_;
    WorkerPool pool_;
    FunctionRegistry registry_;
    TaskManager tasks_;

    /* cluster (only when configured) */
    std::optional<ClusterConfig> ccfg_;
    std::shared_ptr<cluster::Transport> transport_;
    cluster::Membership membership_;
    cluster::ClusterScheduler scheduler_;
    std::mutex fetch_mu_;
    std::unordered_set<ObjectId> inflight_fetches_;
    std::mutex members_cv_mu_;
    std::condition_variable members_cv_;
    std::jthread heartbeat_thread_;
};

template <class T>
T ObjectRef<T>::get(std::chrono::milliseconds timeout) const { return rt_->get(*this, timeout); }

} // namespace orion
