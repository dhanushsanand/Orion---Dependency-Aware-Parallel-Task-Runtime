/* Three-node cluster over loopback TCP: discovery, remote placement,
 * cross-node dependency fetch, failure propagation across nodes, and a
 * message-codec round trip. */
#include "orion/runtime.hpp"
#include <iostream>
#include <numeric>

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << ": " #x "\n"; std::exit(1); } } while (0)
using namespace std::chrono_literals;

static void defs(orion::Runtime& rt)
{
    rt.define<int(int)>("inc", [](int a) { return a + 1; });
    rt.define<long(long, long)>("add", [](long a, long b) { return a + b; });
    rt.define<int(int)>("slow", [](int a) { std::this_thread::sleep_for(30ms); return a; });
    rt.define<std::vector<int>(int)>("range", [](int n) { std::vector<int> v(n); std::iota(v.begin(), v.end(), 1); return v; });
    rt.define<long(std::vector<int>)>("sum", [](std::vector<int> v) { return std::accumulate(v.begin(), v.end(), 0L); });
    rt.define<int(int)>("boom", [](int) -> int { throw std::runtime_error("remote boom"); });
}

int main()
{
    /* codec round trip */
    {
        orion::cluster::Message m{ .type = orion::cluster::MsgType::SubmitTask, .from = 7 };
        m.task = { 99, "fn", { 1, 2, 3 }, 42, 7 };
        auto d = orion::cluster::decode(orion::cluster::encode(m));
        CHECK(d && d->from == 7 && d->task.id == 99 && d->task.fn == "fn" && d->task.args.size() == 3 && d->task.output == 42);
        orion::cluster::Message o{ .type = orion::cluster::MsgType::ObjectData, .from = 1, .object = 5, .data = {1, 2, 3} };
        auto od = orion::cluster::decode(orion::cluster::encode(o));
        CHECK(od && od->object == 5 && od->data.size() == 3);
        CHECK(!orion::cluster::decode({ 4, 0 }));          /* truncated */
    }

    orion::cluster::NodeInfo seed{ 1, { "127.0.0.1", 7201 } };
    orion::Runtime n1({ .workers = 1, .cluster = orion::ClusterConfig{ .id = 1, .listen = seed.addr } });
    orion::Runtime n2({ .workers = 1, .cluster = orion::ClusterConfig{ .id = 2, .listen = { "127.0.0.1", 7202 }, .seed = seed } });
    orion::Runtime n3({ .workers = 1, .cluster = orion::ClusterConfig{ .id = 3, .listen = { "127.0.0.1", 7203 }, .seed = seed } });
    for (auto* r : { &n1, &n2, &n3 }) defs(*r);
    CHECK(n1.wait_for_members(3, 3s));
    CHECK(n2.wait_for_members(3, 3s));
    CHECK(n3.wait_for_members(3, 3s));

    /* a chain submitted from n1: first hop prefers n1 (owns input), later
     * hops follow the data; whatever the placement, the result is right */
    auto x = n1.submit<int>("inc", 1);
    for (int i = 0; i < 50; i++) x = n1.submit<int>("inc", x);
    CHECK(x.get(10s) == 52);

    /* force remote placement: flood n1 with slow tasks so its load is high,
     * then submit more — the scheduler must send some elsewhere */
    std::vector<orion::ObjectRef<int>> slow;
    for (int i = 0; i < 12; i++) slow.push_back(n1.submit<int>("slow", i));
    int sum = 0; for (auto& s : slow) sum += s.get(10s);
    CHECK(sum == 66);
    CHECK(n1.remote_submits() > 0);

    /* data locality + cross-node fetch: n3 submits a task whose only input
     * lives on n2 -> the scheduler ships the task to n2 (no copy); n1 then
     * reads the result, which requires a fetch from its owner */
    auto on2 = n2.submit<std::vector<int>>("range", 1000);
    auto on3 = n3.submit<long>("sum", on2);
    CHECK(orion::object_owner(on3.id()) == 2);
    CHECK(n3.remote_submits() == 1);
    CHECK(n1.get(on3, 10s) == 500500);
    CHECK(n1.object_fetches() > 0);
    /* and a genuine cross-node dependency: two inputs on different nodes,
     * so whoever runs it must fetch at least one */
    auto a2 = n2.submit<long>("add", 1L, 2L);
    auto a3 = n3.submit<long>("add", 10L, 20L);
    auto both = n1.submit<long>("add", a2, a3);
    CHECK(both.get(10s) == 33);
    CHECK(n1.object_fetches() + n2.object_fetches() + n3.object_fetches() >= 2);

    /* failures propagate across nodes */
    bool threw = false;
    try { n2.submit<int>("inc", n3.submit<int>("boom", 1)).get(10s); }
    catch (const orion::TaskError& e) { threw = std::string(e.what()).find("remote boom") != std::string::npos; }
    CHECK(threw);

    /* everybody did some work */
    std::uint64_t total = 0, failed = 0;
    for (auto* r : { &n1, &n2, &n3 }) { r->wait_all(); total += r->stats().completed; failed += r->stats().failed; }
    CHECK(total == 51 + 12 + 2 + 3);     /* incs, slows, range+sum, adds */
    CHECK(failed == 2);                  /* boom + the inc that consumed it */
    std::cout << "test_cluster: PASS (n1=" << n1.stats().completed << " n2=" << n2.stats().completed
              << " n3=" << n3.stats().completed << ")\n";
}
