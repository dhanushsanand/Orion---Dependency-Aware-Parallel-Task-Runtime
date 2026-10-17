# Orion — dependency-aware parallel task runtime

A minimal, Ray-style distributed task runtime in modern C++ (C++23). You
describe a computation as a DAG of tasks over immutable objects; Orion runs
each task the instant its inputs exist, on a thread pool in one process or
across a cluster of nodes, with zero busy-waiting anywhere.

```cpp
orion::Runtime rt;                                            // single process
rt.define<int(int, int)>("add", [](int a, int b) { return a + b; });

auto a = rt.put(2);
auto b = rt.submit<int>("add", a, 3);        // literal args are put() for you
auto c = rt.submit<int>("add", b, b);        // scheduled when b is ready
std::cout << c.get() << "\n";                // 10

orion::Runtime node({ .workers = 4, .cluster = orion::ClusterConfig{ .id = 2,
    .listen = { "10.0.0.2", 7000 }, .seed = {{ 1, { "10.0.0.1", 7000 } }} } });
// identical API; the scheduler now places tasks across the cluster
```

```
$ make && make test
test_object_store: PASS
test_dag: PASS (4031 tasks)
test_cluster: PASS (n1=34 n2=18 n3=16)      # 3 nodes over loopback TCP
```

## Architecture

```
            Runtime  (include/orion/runtime.hpp)
            put / submit / get / define        ─┐
                 │                               │ ClusterScheduler
                 ▼                               │ (load + data locality)
   ┌──────────────────────┐  SubmitTask          ▼
   │     TaskManager      │◀────────────── Transport ──▶ other nodes
   │ dependency tracking  │  GetObject /     (TCP or gRPC)
   │ dispatch-on-ready    │  ObjectData         ▲
   └──────────────────────┘                     │ Membership
        │ on_ready()        ▲ put()              │ (Hello, Members, Heartbeat)
        ▼                   │
   ┌──────────────┐    ┌──────────────┐
   │ ObjectStore  │    │  WorkerPool  │
   │ sharded,     │    │ mutex queue +│
   │ callbacks,   │    │ condvar,     │
   │ condvar get  │    │ N threads    │
   └──────────────┘    └──────────────┘
```

### Object store (`object_store.hpp`)
Immutable `ObjectId → bytes`. Sharded map, one mutex + condition variable per
shard. `get()` blocks on the condvar; `on_ready()` registers a callback that
fires exactly once, immediately if the value exists or at `put()` time.
Callbacks run outside the lock so they may re-enter the store.

### Worker pool (`worker_pool.hpp`)
`std::jthread` workers pull from a mutex-protected deque and sleep on a
condition variable when it is empty. Stop requests are made under the mutex
so a worker between its predicate check and its wait cannot miss the wake-up.

### Task manager (`task_manager.hpp`) — the heart of it
A submitted task registers an `on_ready` callback on each input. Every
callback decrements an atomic remaining-dependency counter; the one that
reaches zero pushes the task into the pool. Tasks are therefore dispatched
*immediately upon data readiness* — nothing scans a ready list, nothing
polls. Inputs that live on another node are requested through a hook that
the cluster layer plugs in.

Every stored value is an *envelope* (`status byte + payload`), so a failing
task stores an error that propagates through its dependents and across nodes
and surfaces as `orion::TaskError` on `get()`.

### Cluster (`cluster/`)
* **Discovery** — a node sends `Hello` to a seed; the seed upserts it and
  broadcasts the `Members` list; every node connects to every member.
  Heartbeats carry the sender's load; silent peers are marked dead.
* **Placement** — `ClusterScheduler::place` scores each live node as
  `reported load + tasks sent since its last heartbeat + locality penalty`
  (one unit per input the node does not own); lowest score wins, ties stay
  local. The output `ObjectId` encodes its owner node in the top bits, so any
  node can find any object without a directory service.
* **Cross-node dependencies** — an executor missing an input sends
  `GetObject` to the owner; the owner answers `ObjectData` when the object is
  ready (via `on_ready`, so it never blocks). Fetches are de-duplicated.
* **Transport** — one small interface (`start / connect / send`). The built-in
  backend is TCP with length-prefixed frames and a hand-rolled binary codec.
  `make WITH_GRPC=1` builds the gRPC + Protobuf backend from `proto/orion.proto`
  (one `Node.Send(Envelope)` RPC with a `oneof` body). The gRPC backend could
  not be compiled in the authoring environment, so expect minor API touch-ups.

## Building and running

```
make               # liborion.a, orion_node, local_dag, cluster_demo
make test          # unit, DAG and 3-node cluster tests
make tsan          # ThreadSanitizer
make asan          # Address/UB sanitizer
make WITH_GRPC=1   # gRPC backend (needs protoc, grpc_cpp_plugin, libgrpc++)

./local_dag        # diamond DAG, fan-out/fan-in, closures, error propagation
./cluster_demo     # 3 nodes in one process

# separate processes:
./orion_node --id 1 --port 7001
./orion_node --id 2 --port 7002 --seed 1@127.0.0.1:7001
./orion_node --id 3 --port 7003 --seed 1@127.0.0.1:7001 --driver   # submits work
```

## API summary

| call | meaning |
|---|---|
| `define<R(Args...)>(name, fn)` | register a function (all nodes must register the same set) |
| `put(value)` → `ObjectRef<T>` | store a value locally |
| `submit<R>(name, args...)` → `ObjectRef<R>` | DAG task; args are `ObjectRef`s or literals; may run on any node |
| `submit_local(lambda, refs...)` | closure task; always runs on this node |
| `ref.get(timeout)` / `rt.get(ref)` | blocking read; fetches from the owner node if needed; throws `TaskError` |
| `wait_all()` | block until this node has nothing waiting or running |
| `stats()`, `members()`, `load()` | introspection |

Values cross the store/wire through `Codec<T>` (built-ins: trivially copyable
types, `std::string`, `std::vector<trivial>`); specialise it for your types.

## Tests
| test | covers |
|---|---|
| `test_object_store` | callback-once semantics, immutability, blocking get + timeout, 20k concurrent puts, pool drain/shutdown |
| `test_dag` | 1000-deep chain, 1024-leaf reduction tree, parallel timing, 2000-node random DAG checked against a serial model, strings/closures, multi-hop error propagation |
| `test_cluster` | message codec round trip, discovery of 3 nodes, remote placement under load, data-locality placement, cross-node fetch, failure propagation across nodes |

## Known limitations / future work
* No fault tolerance: a task on a node that dies is not re-executed.
* Objects are never garbage-collected (no reference counting).
* Placement uses heartbeat-reported load, so it can be a few hundred ms stale.
* Closures cannot be shipped; only registered functions run remotely.
* Fixed `Codec` set; no automatic serialisation of arbitrary structs.

## Layout
```
include/orion/      public headers (runtime, task, task_manager, object_store, worker_pool, codec)
include/orion/cluster/  message, transport, membership, scheduler
src/                implementations; src/cluster/tcp_transport.cpp, grpc_transport.cpp
proto/orion.proto   gRPC service + messages
examples/           local_dag.cpp, cluster_demo.cpp
tools/orion_node.cpp   standalone node binary
tests/              test_object_store, test_dag, test_cluster
```
