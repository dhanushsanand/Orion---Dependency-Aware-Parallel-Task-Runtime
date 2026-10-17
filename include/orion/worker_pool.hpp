#pragma once
/* WorkerPool: N threads pulling from a mutex-protected queue, sleeping on a
 * condition variable when idle (zero busy-waiting). */
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <stop_token>
#include <thread>
#include <vector>

namespace orion {

class WorkerPool {
public:
    using Job = std::function<void()>;

    explicit WorkerPool(std::size_t threads = std::thread::hardware_concurrency());
    ~WorkerPool();
    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    void submit(Job job);
    std::size_t pending() const;      /* queued, not yet picked up */
    std::size_t running() const;      /* currently executing */
    std::size_t size() const { return workers_.size(); }
    void drain();                     /* block until queue empty and nothing running */

private:
    void run(std::stop_token st);

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::condition_variable idle_cv_;
    std::deque<Job> queue_;
    std::size_t running_{0};
    std::vector<std::jthread> workers_;
};

} // namespace orion
