#include "orion/task_manager.hpp"

namespace orion {

TaskManager::TaskManager(ObjectStore& store, WorkerPool& pool, FunctionRegistry& registry)
    : store_(store), pool_(pool), registry_(registry) {}

void TaskManager::submit(TaskSpec spec, RawFn body)
{
    submitted_++;
    auto p = std::make_shared<Pending>();
    p->spec = std::move(spec);
    p->body = std::move(body);
    /* +1 guard: the task cannot fire until every callback is registered,
     * even if all inputs are already present. */
    p->remaining.store(p->spec.args.size() + 1, std::memory_order_relaxed);

    {
        std::lock_guard lk(mu_);
        waiting_[p->spec.id] = p;
    }
    for (ObjectId dep : p->spec.args) {
        if (fetch_ && !store_.contains(dep)) fetch_(dep);
        store_.on_ready(dep, [this, p](ObjectId, const Bytes&) {
            if (p->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) dispatch(p);
        });
    }
    if (p->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) dispatch(p);
}

void TaskManager::dispatch(std::shared_ptr<Pending> p)
{
    {
        std::lock_guard lk(mu_);
        waiting_.erase(p->spec.id);
    }
    pool_.submit([this, p = std::move(p)] { execute(*p); });
}

void TaskManager::execute(const Pending& p)
{
    bool ok = true;
    Bytes result;
    try {
        std::vector<Bytes> args;
        args.reserve(p.spec.args.size());
        for (ObjectId id : p.spec.args) {
            auto env = store_.try_get(id);
            if (!env) throw TaskError("input vanished");
            if (envelope::is_error(*env))
                throw TaskError("upstream failure: " + envelope::message(*env));
            args.push_back(envelope::payload(*env));
        }
        RawFn body = p.body ? p.body : registry_.lookup(p.spec.fn);
        result = envelope::ok(body(args));
    } catch (const std::exception& e) {
        ok = false;
        result = envelope::error(e.what());
    } catch (...) {
        ok = false;
        result = envelope::error("unknown error");
    }
    (ok ? completed_ : failed_)++;
    store_.put(p.spec.output, std::move(result));   /* fires downstream callbacks */
    if (done_) done_(p.spec, ok);
}

TaskStats TaskManager::stats() const
{
    std::lock_guard lk(mu_);
    return { submitted_.load(), completed_.load(), failed_.load(), waiting_.size() };
}

std::size_t TaskManager::waiting() const { std::lock_guard lk(mu_); return waiting_.size(); }

} // namespace orion
