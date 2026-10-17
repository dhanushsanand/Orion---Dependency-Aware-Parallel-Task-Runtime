/* local_dag.cpp — single-process engine: a diamond-shaped DAG plus a
 * map/reduce fan-out, scheduled purely by data readiness. */
#include "orion/runtime.hpp"
#include <chrono>
#include <format>
#include <iostream>
#include <numeric>
#include <thread>

using namespace std::chrono_literals;

int main()
{
    orion::Runtime rt({ .workers = 4 });

    rt.define<int(int)>("slow_square", [](int x) { std::this_thread::sleep_for(100ms); return x * x; });
    rt.define<int(int, int)>("add", [](int a, int b) { return a + b; });
    rt.define<std::vector<int>(int)>("range", [](int n) { std::vector<int> v(n); std::iota(v.begin(), v.end(), 0); return v; });
    rt.define<long(std::vector<int>)>("sum", [](std::vector<int> v) { return std::accumulate(v.begin(), v.end(), 0L); });

    /*          a=3
     *         /   \
     *   sq(a)       sq(a)+1      <- both run in parallel
     *         \   /
     *        add(.,.)                                                          */
    auto t0 = std::chrono::steady_clock::now();
    auto a  = rt.put(3);
    auto l  = rt.submit<int>("slow_square", a);
    auto r  = rt.submit<int>("add", rt.submit<int>("slow_square", a), 1);
    auto d  = rt.submit<int>("add", l, r);
    int diamond = d.get();
    auto took = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    std::cout << std::format("diamond: {} (took {} ms: two 100 ms tasks ran concurrently)\n", diamond, took);

    /* fan-out / fan-in: 8 chunks summed in parallel, then combined with a local closure */
    std::vector<orion::ObjectRef<long>> partial;
    for (int i = 1; i <= 8; i++) partial.push_back(rt.submit<long>("sum", rt.submit<std::vector<int>>("range", i * 1000)));
    long total = 0;
    for (auto& p : partial) total += p.get();
    std::cout << std::format("fan-out total: {}\n", total);

    /* closures run locally and compose with registered tasks */
    auto doubled = rt.submit_local([](int x) { return x * 2; }, d);
    std::cout << std::format("closure: {}\n", doubled.get());

    /* failures propagate through the graph as exceptions on get() */
    rt.define<int(int)>("boom", [](int) -> int { throw std::runtime_error("kaboom"); });
    auto bad = rt.submit<int>("add", rt.submit<int>("boom", 1), 1);
    try { bad.get(); } catch (const orion::TaskError& e) { std::cout << std::format("propagated error: {}\n", e.what()); }

    auto s = rt.stats();
    std::cout << std::format("tasks: submitted={} completed={} failed={}\n", s.submitted, s.completed, s.failed);
}
