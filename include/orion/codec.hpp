#pragma once
/* Codec<T>: how a value crosses the object store / the wire.
 * Built-ins cover trivially-copyable types, std::string and std::vector<trivial>.
 * Users specialise Codec<T> for their own types. */
#include "types.hpp"
#include <concepts>
#include <cstring>
#include <stdexcept>
#include <type_traits>

namespace orion {

template <class T> struct Codec;   // primary: intentionally undefined

template <class T>
    requires std::is_trivially_copyable_v<T> && (!std::is_pointer_v<T>)
struct Codec<T> {
    static Bytes encode(const T& v) {
        Bytes b(sizeof(T));
        std::memcpy(b.data(), &v, sizeof(T));
        return b;
    }
    static T decode(const Bytes& b) {
        if (b.size() != sizeof(T)) throw std::runtime_error("Codec: size mismatch");
        T v; std::memcpy(&v, b.data(), sizeof(T));
        return v;
    }
};

template <> struct Codec<std::string> {
    static Bytes encode(const std::string& s) { return Bytes(s.begin(), s.end()); }
    static std::string decode(const Bytes& b) { return std::string(b.begin(), b.end()); }
};

template <class T>
    requires std::is_trivially_copyable_v<T>
struct Codec<std::vector<T>> {
    static Bytes encode(const std::vector<T>& v) {
        Bytes b(v.size() * sizeof(T));
        if (!v.empty()) std::memcpy(b.data(), v.data(), b.size());
        return b;
    }
    static std::vector<T> decode(const Bytes& b) {
        if (b.size() % sizeof(T)) throw std::runtime_error("Codec: size mismatch");
        std::vector<T> v(b.size() / sizeof(T));
        if (!v.empty()) std::memcpy(v.data(), b.data(), b.size());
        return v;
    }
};

template <class T>
concept Encodable = requires(const T& t, const Bytes& b) {
    { Codec<T>::encode(t) } -> std::same_as<Bytes>;
    { Codec<T>::decode(b) } -> std::same_as<T>;
};

} // namespace orion
