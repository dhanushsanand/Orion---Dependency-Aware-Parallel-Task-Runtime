/* cluster_demo.cpp — three nodes in one process (TCP on localhost), a
 * pipeline whose stages land on different nodes, and cross-node object fetch. */
#include "orion/runtime.hpp"
#include <format>
#include <iostream>
#include <numeric>

using namespace std::chrono_literals;

static void define_functions(orion::Runtime& rt)
{
    rt.define<std::vector<int>(int)>("range", [](int n) { std::vector<int> v(n); std::iota(v.begin(), v.end(), 1); return v; });
    rt.define<std::vector<int>(std::vector<int>)>("square_all", [](std::vector<int> v) { for (auto& x : v) x *= x; return v; });
    rt.define<long(std::vector<int>)>("sum", [](std::vector<int> v) { return std::accumulate(v.begin(), v.end(), 0L); });
    rt.define<long(long, long)>("add", [](long a, long b) { return a + b; });
}

int main()
{
    orion::cluster::NodeInfo seed{ 1, { "127.0.0.1", 7101 } };
    orion::Runtime n1({ .workers = 2, .cluster = orion::ClusterConfig{ .id = 1, .listen = seed.addr } });
    orion::Runtime n2({ .workers = 2, .cluster = orion::ClusterConfig{ .id = 2, .listen = { "127.0.0.1", 7102 }, .seed = seed } });
    orion::Runtime n3({ .workers = 2, .cluster = orion::ClusterConfig{ .id = 3, .listen = { "127.0.0.1", 7103 }, .seed = seed } });
    for (auto* rt : { &n1, &n2, &n3 }) define_functions(*rt);
    n1.wait_for_members(3, 3s);
    std::cout << std::format("cluster: {} members\n", n1.members().size());

    /* 6 independent pipelines submitted from node 1; the scheduler spreads
     * them by load, and each downstream stage prefers the node holding its input */
    std::vector<orion::ObjectRef<long>> results;
    for (int i = 1; i <= 6; i++) {
        auto r  = n1.submit<std::vector<int>>("range", i * 10000);
        auto sq = n1.submit<std::vector<int>>("square_all", r);
        results.push_back(n1.submit<long>("sum", sq));
    }
    auto total = n1.submit<long>("add", results[0], results[1]);
    for (std::size_t i = 2; i < results.size(); i++) total = n1.submit<long>("add", total, results[i]);
    std::cout << std::format("sum of squares over all pipelines = {}\n", total.get(10s));

    for (auto* rt : { &n1, &n2, &n3 }) {
        auto s = rt->stats();
        std::cout << std::format("node {}: executed={} remote_submits={} object_fetches={}\n",
                                 rt->node_id(), s.completed, rt->remote_submits(), rt->object_fetches());
    }
}
