#pragma once
/* Task description + function registry.
 *
 * Every object in the store is an *envelope*: 1 status byte (0 = ok,
 * 1 = error) followed by the payload (encoded value or UTF-8 error text).
 * This is what lets failures propagate through the DAG and across nodes. */
#include "codec.hpp"
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace orion {

struct TaskSpec {
    TaskId                id{};
    std::string           fn;          /* registered function name ("" = local closure) */
    std::vector<ObjectId> args;
    ObjectId              output{};
    NodeId                submitter{};
};

using RawFn = std::function<Bytes(std::span<const Bytes>)>;

struct TaskError : std::runtime_error { using std::runtime_error::runtime_error; };

namespace envelope {
    inline Bytes ok(Bytes payload)          { payload.insert(payload.begin(), 0); return payload; }
    inline Bytes error(const std::string& m){ Bytes b{1}; b.insert(b.end(), m.begin(), m.end()); return b; }
    inline bool  is_error(const Bytes& b)   { return b.empty() || b[0] != 0; }
    inline Bytes payload(const Bytes& b)    { return Bytes(b.begin() + 1, b.end()); }
    inline std::string message(const Bytes& b) { return std::string(b.begin() + 1, b.end()); }
}

/* name -> RawFn. Nodes in a cluster must register the same function set. */
class FunctionRegistry {
public:
    template <class Sig, class F> void define(std::string name, F&& f);
    RawFn lookup(const std::string& name) const {
        std::shared_lock lk(mu_);
        auto it = fns_.find(name);
        if (it == fns_.end()) throw TaskError("unknown function: " + name);
        return it->second;
    }
    std::vector<std::string> names() const {
        std::shared_lock lk(mu_);
        std::vector<std::string> v; for (auto& [k, _] : fns_) v.push_back(k); return v;
    }
private:
    mutable std::shared_mutex mu_;
    std::map<std::string, RawFn> fns_;
};

namespace detail {
template <class R, class... Args, class F, std::size_t... I>
RawFn wrap_impl(F f, std::index_sequence<I...>) {
    return [f = std::move(f)](std::span<const Bytes> in) -> Bytes {
        if (in.size() != sizeof...(Args)) throw TaskError("arity mismatch");
        return Codec<R>::encode(f(Codec<std::decay_t<Args>>::decode(in[I])...));
    };
}
template <class Sig> struct SigTraits;
template <class R, class... Args> struct SigTraits<R(Args...)> {
    using Ret = R;
    template <class F> static RawFn wrap(F f) {
        return wrap_impl<R, Args...>(std::move(f), std::index_sequence_for<Args...>{});
    }
};
} // namespace detail

template <class Sig, class F>
void FunctionRegistry::define(std::string name, F&& f)
{
    RawFn raw = detail::SigTraits<Sig>::wrap(std::forward<F>(f));
    std::unique_lock lk(mu_);
    fns_[std::move(name)] = std::move(raw);
}

} // namespace orion
