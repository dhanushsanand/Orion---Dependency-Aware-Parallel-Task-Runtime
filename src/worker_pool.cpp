#include "orion/worker_pool.hpp"

namespace orion {

WorkerPool::WorkerPool(std::size_t threads)
{
    if (threads == 0) threads = 1;
    workers_.reserve(threads);
    for (std::size_t i = 0; i < threads; i++)
        workers_.emplace_back([this](std::stop_token st) { run(st); });
}

WorkerPool::~WorkerPool()
{
    {
        /* the stop flag is part of the condition the workers wait on, so it
         * must change under the mutex or a worker between its predicate check
         * and its wait() would miss the notification (lost wake-up) */
        std::lock_guard lk(mu_);
        for (auto& w : workers_) w.request_stop();
    }
    cv_.notify_all();
    /* jthread joins in its destructor */
}

void WorkerPool::submit(Job job)
{
    {
        std::lock_guard lk(mu_);
        queue_.push_back(std::move(job));
    }
    cv_.notify_one();
}

std::size_t WorkerPool::pending() const { std::lock_guard lk(mu_); return queue_.size(); }
std::size_t WorkerPool::running() const { std::lock_guard lk(mu_); return running_; }

void WorkerPool::drain()
{
    std::unique_lock lk(mu_);
    idle_cv_.wait(lk, [&] { return queue_.empty() && running_ == 0; });
}

void WorkerPool::run(std::stop_token st)
{
    for (;;) {
        Job job;
        {
            std::unique_lock lk(mu_);
            cv_.wait(lk, [&] { return st.stop_requested() || !queue_.empty(); });
            if (queue_.empty()) return;               /* stop requested and nothing left */
            job = std::move(queue_.front());
            queue_.pop_front();
            running_++;
        }
        try { job(); } catch (...) { /* task bodies report their own failures */ }
        {
            std::lock_guard lk(mu_);
            running_--;
            if (queue_.empty() && running_ == 0) idle_cv_.notify_all();
        }
    }
}

} // namespace orion
