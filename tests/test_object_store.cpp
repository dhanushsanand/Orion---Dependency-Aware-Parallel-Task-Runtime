#include "orion/object_store.hpp"
#include "orion/worker_pool.hpp"
#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>

#define CHECK(x) do { if (!(x)) { std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << ": " #x "\n"; std::exit(1); } } while (0)
using namespace std::chrono_literals;

int main()
{
    orion::ObjectStore st;
    /* callback before put fires at put; callback after put fires immediately; each exactly once */
    std::atomic<int> fired{0};
    st.on_ready(1, [&](orion::ObjectId, const orion::Bytes& b) { CHECK(b.size() == 1 && b[0] == 7); fired++; });
    CHECK(fired == 0);
    st.put(1, {7});
    CHECK(fired == 1);
    st.on_ready(1, [&](orion::ObjectId, const orion::Bytes&) { fired++; });
    CHECK(fired == 2);
    st.put(1, {9});                                  /* immutable: ignored */
    CHECK(st.try_get(1)->at(0) == 7 && fired == 2);

    /* blocking get wakes on put (no spin); timeout path */
    CHECK(!st.get(2, 20ms).has_value());
    std::jthread producer([&] { std::this_thread::sleep_for(30ms); st.put(2, {1, 2, 3}); });
    auto v = st.get(2);
    CHECK(v && v->size() == 3);

    /* many producers / consumers across shards */
    constexpr int N = 20000;
    std::atomic<int> cbs{0};
    {
        orion::WorkerPool pool(8);
        for (int i = 0; i < N; i++) {
            st.on_ready(100 + i, [&](orion::ObjectId, const orion::Bytes&) { cbs++; });
            pool.submit([&, i] { st.put(100 + i, {std::uint8_t(i)}); });
        }
        pool.drain();
    }
    CHECK(cbs == N);
    CHECK(st.size() == N + 2);

    /* worker pool: drain waits for running jobs; destructor finishes queued work */
    std::atomic<int> ran{0};
    {
        orion::WorkerPool pool(2);
        for (int i = 0; i < 100; i++) pool.submit([&] { std::this_thread::sleep_for(1ms); ran++; });
    }
    CHECK(ran == 100);
    std::cout << "test_object_store: PASS\n";
}
