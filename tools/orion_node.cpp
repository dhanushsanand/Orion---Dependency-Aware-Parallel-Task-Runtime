/* orion_node — run one cluster node as its own process.
 *
 *   ./orion_node --id 1 --port 7001                       # seed
 *   ./orion_node --id 2 --port 7002 --seed 1@127.0.0.1:7001
 *   ./orion_node --id 2 --port 7002 --seed 1@127.0.0.1:7001 --driver   # also submits work
 *
 * Every node registers the same demo functions (range / square_all / sum / add).
 * The --driver flag makes this node submit a workload and print the result;
 * without it the node just serves tasks until Ctrl-C. */
#include "orion/runtime.hpp"
#include <csignal>
#include <format>
#include <iostream>
#include <numeric>
#include <string>

using namespace std::chrono_literals;
static std::atomic<bool> g_stop{false};

int main(int argc, char** argv)
{
    orion::ClusterConfig cc;
    std::size_t workers = 2; bool driver = false;
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        auto next = [&] { return std::string(i + 1 < argc ? argv[++i] : ""); };
        if (a == "--id") cc.id = std::stoi(next());
        else if (a == "--port") cc.listen.port = std::stoi(next());
        else if (a == "--host") cc.listen.host = next();
        else if (a == "--workers") workers = std::stoul(next());
        else if (a == "--driver") driver = true;
        else if (a == "--seed") {
            std::string s = next();                    /* id@host:port */
            auto at = s.find('@'), colon = s.rfind(':');
            cc.seed = orion::cluster::NodeInfo{ static_cast<orion::NodeId>(std::stoi(s.substr(0, at))),
                                                { s.substr(at + 1, colon - at - 1), static_cast<std::uint16_t>(std::stoi(s.substr(colon + 1))) } };
        } else { std::cerr << "usage: orion_node --id N --port P [--seed ID@HOST:PORT] [--workers W] [--driver]\n"; return 1; }
    }

    orion::Runtime rt({ .workers = workers, .cluster = cc });
    rt.define<std::vector<int>(int)>("range", [](int n) { std::vector<int> v(n); std::iota(v.begin(), v.end(), 1); return v; });
    rt.define<std::vector<int>(std::vector<int>)>("square_all", [](std::vector<int> v) { for (auto& x : v) x *= x; return v; });
    rt.define<long(std::vector<int>)>("sum", [](std::vector<int> v) { return std::accumulate(v.begin(), v.end(), 0L); });
    rt.define<long(long, long)>("add", [](long a, long b) { return a + b; });
    std::cout << std::format("node {} listening on {}\n", rt.node_id(), cc.listen.str());

    if (driver) {
        std::this_thread::sleep_for(500ms);           /* let membership settle */
        std::cout << std::format("members: {}\n", rt.members().size());
        std::vector<orion::ObjectRef<long>> parts;
        for (int i = 1; i <= 8; i++)
            parts.push_back(rt.submit<long>("sum", rt.submit<std::vector<int>>("square_all", rt.submit<std::vector<int>>("range", i * 1000))));
        auto total = rt.submit<long>("add", parts[0], parts[1]);
        for (std::size_t i = 2; i < parts.size(); i++) total = rt.submit<long>("add", total, parts[i]);
        std::cout << std::format("result = {}  (remote submits: {}, object fetches: {})\n",
                                 total.get(30s), rt.remote_submits(), rt.object_fetches());
        return 0;
    }
    std::signal(SIGINT, [](int) { g_stop = true; });
    while (!g_stop) std::this_thread::sleep_for(200ms);
    std::cout << std::format("node {}: executed {} tasks\n", rt.node_id(), rt.stats().completed);
}
