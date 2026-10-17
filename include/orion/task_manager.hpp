#pragma once
/* TaskManager: dependency tracking + dispatch-on-readiness.
 *
 * A submitted task registers an on_ready callback on every argument object.
 * Each callback decrements the task's remaining-dependency counter; the one
 * that drives it to zero pushes the task to the worker pool. Nothing ever
 * polls or sleeps waiting for inputs. Arguments that live on another node
 * are requested through the `fetch` hook (the cluster layer plugs in here;
 * in single-process mode it is a no-op). */
#include "object_store.hpp"
#include "task.hpp"
#include "worker_pool.hpp"
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace orion {

struct TaskStats {
    std::uint64_t submitted{}, completed{}, failed{}, waiting{};
};

class TaskManager {
public:
    using FetchHook = std::function<void(ObjectId)>;
    using DoneHook  = std::function<void(const TaskSpec&, bool ok)>;

    TaskManager(ObjectStore& store, WorkerPool& pool, FunctionRegistry& registry);

    void set_fetch_hook(FetchHook f) { fetch_ = std::move(f); }
    void set_done_hook(DoneHook f)   { done_ = std::move(f); }

    /* body == nullptr -> resolved from the registry by spec.fn */
    void submit(TaskSpec spec, RawFn body = nullptr);

    TaskStats stats() const;
    std::size_t waiting() const;

private:
    struct Pending {
        TaskSpec spec;
        RawFn body;
        std::atomic<std::size_t> remaining;
    };
    void dispatch(std::shared_ptr<Pending> p);
    void execute(const Pending& p);

    ObjectStore& store_;
    WorkerPool& pool_;
    FunctionRegistry& registry_;
    FetchHook fetch_;
    DoneHook done_;

    mutable std::mutex mu_;
    std::unordered_map<TaskId, std::shared_ptr<Pending>> waiting_;
    std::atomic<std::uint64_t> submitted_{0}, completed_{0}, failed_{0};
};

} // namespace orion
