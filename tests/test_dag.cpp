#include "orion/runtime.hpp"
#include <chrono>
#include <iostream>
#include <numeric>
#include <random>

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << ": " #x "\n"; std::exit(1); } } while (0)
using namespace std::chrono_literals;

int main()
{
    orion::Runtime rt({ .workers = 8 });
    rt.define<int(int, int)>("add", [](int a, int b) { return a + b; });
    rt.define<int(int)>("inc", [](int a) { return a + 1; });
    rt.define<int(int)>("slow", [](int a) { std::this_thread::sleep_for(50ms); return a; });
    rt.define<std::string(std::string, std::string)>("cat", [](std::string a, std::string b) { return a + b; });

    /* 1. deep chain: 1000 sequential dependencies */
    auto cur = rt.submit<int>("inc", 0);
    for (int i = 1; i < 1000; i++) cur = rt.submit<int>("inc", cur);
    CHECK(cur.get() == 1000);

    /* 2. wide fan-in: binary reduction tree over 1024 leaves */
    std::vector<orion::ObjectRef<int>> level;
    for (int i = 0; i < 1024; i++) level.push_back(rt.put(1));
    while (level.size() > 1) {
        std::vector<orion::ObjectRef<int>> next;
        for (std::size_t i = 0; i < level.size(); i += 2) next.push_back(rt.submit<int>("add", level[i], level[i + 1]));
        level.swap(next);
    }
    CHECK(level[0].get() == 1024);

    /* 3. parallelism: 8 independent 50 ms tasks finish in ~50 ms, not 400 */
    auto t0 = std::chrono::steady_clock::now();
    std::vector<orion::ObjectRef<int>> par;
    for (int i = 0; i < 8; i++) par.push_back(rt.submit<int>("slow", i));
    int s = 0; for (auto& p : par) s += p.get();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    CHECK(s == 28 && ms < 200);

    /* 4. random DAG: each task depends on two earlier tasks; verify against a serial model */
    std::mt19937 rng(42);
    std::vector<orion::ObjectRef<int>> nodes;
    std::vector<long> model;
    for (int i = 0; i < 2000; i++) {
        if (i < 2) { nodes.push_back(rt.put(i + 1)); model.push_back(i + 1); continue; }
        std::uniform_int_distribution<int> d(0, i - 1);
        int a = d(rng), b = d(rng);
        nodes.push_back(rt.submit<int>("add", nodes[a], nodes[b]));
        model.push_back(static_cast<int>(model[a] + model[b]));   /* same wraparound as int */
    }
    for (int i = 0; i < 2000; i++) CHECK(nodes[i].get() == static_cast<int>(model[i]));

    /* 5. strings, closures, mixed literal/ref args */
    auto hello = rt.submit<std::string>("cat", std::string("hello, "), rt.put(std::string("world")));
    auto shout = rt.submit_local([](std::string v) { for (auto& c : v) c = std::toupper(c); return v; }, hello);
    CHECK(shout.get() == "HELLO, WORLD");

    /* 6. error propagation through several hops + unknown function */
    rt.define<int(int)>("boom", [](int) -> int { throw std::runtime_error("boom"); });
    auto bad = rt.submit<int>("inc", rt.submit<int>("inc", rt.submit<int>("boom", 1)));
    bool threw = false;
    try { bad.get(); } catch (const orion::TaskError& e) { threw = std::string(e.what()).find("boom") != std::string::npos; }
    CHECK(threw);
    threw = false;
    try { rt.submit<int>("nope", 1).get(); } catch (const orion::TaskError&) { threw = true; }
    CHECK(threw);

    rt.wait_all();
    auto st = rt.stats();
    CHECK(st.waiting == 0 && st.failed == 4 && st.completed == st.submitted - 4);
    std::cout << "test_dag: PASS (" << st.completed << " tasks)\n";
}
