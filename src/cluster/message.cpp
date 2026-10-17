#include "orion/cluster/message.hpp"
#include <cstring>

namespace orion::cluster {

namespace {
struct Writer {
    Bytes out;
    void u8(std::uint8_t v)   { out.push_back(v); }
    void u16(std::uint16_t v) { for (int i = 0; i < 2; i++) out.push_back((v >> (8 * i)) & 0xFF); }
    void u32(std::uint32_t v) { for (int i = 0; i < 4; i++) out.push_back((v >> (8 * i)) & 0xFF); }
    void u64(std::uint64_t v) { for (int i = 0; i < 8; i++) out.push_back((v >> (8 * i)) & 0xFF); }
    void bytes(const Bytes& b){ u32(b.size()); out.insert(out.end(), b.begin(), b.end()); }
    void str(const std::string& s) { u32(s.size()); out.insert(out.end(), s.begin(), s.end()); }
};
struct Reader {
    const Bytes& in; std::size_t pos = 0; bool ok = true;
    bool need(std::size_t n) { if (pos + n > in.size()) { ok = false; return false; } return true; }
    std::uint8_t  u8()  { if (!need(1)) return 0; return in[pos++]; }
    std::uint16_t u16() { if (!need(2)) return 0; std::uint16_t v = 0; for (int i = 0; i < 2; i++) v |= std::uint16_t(in[pos++]) << (8 * i); return v; }
    std::uint32_t u32() { if (!need(4)) return 0; std::uint32_t v = 0; for (int i = 0; i < 4; i++) v |= std::uint32_t(in[pos++]) << (8 * i); return v; }
    std::uint64_t u64() { if (!need(8)) return 0; std::uint64_t v = 0; for (int i = 0; i < 8; i++) v |= std::uint64_t(in[pos++]) << (8 * i); return v; }
    Bytes bytes() { auto n = u32(); if (!need(n)) return {}; Bytes b(in.begin() + pos, in.begin() + pos + n); pos += n; return b; }
    std::string str() { auto n = u32(); if (!need(n)) return {}; std::string s(in.begin() + pos, in.begin() + pos + n); pos += n; return s; }
};
}

Bytes encode(const Message& m)
{
    Writer w;
    w.u8(static_cast<std::uint8_t>(m.type));
    w.u16(m.from);
    switch (m.type) {
    case MsgType::Hello:
    case MsgType::Members:
        w.u32(m.members.size());
        for (auto& n : m.members) { w.u16(n.id); w.str(n.addr.host); w.u16(n.addr.port); }
        break;
    case MsgType::Heartbeat:
        w.u32(m.load);
        break;
    case MsgType::SubmitTask:
        w.u64(m.task.id); w.str(m.task.fn); w.u64(m.task.output); w.u16(m.task.submitter);
        w.u32(m.task.args.size());
        for (auto a : m.task.args) w.u64(a);
        break;
    case MsgType::GetObject:
        w.u64(m.object);
        break;
    case MsgType::ObjectData:
        w.u64(m.object); w.bytes(m.data);
        break;
    }
    return std::move(w.out);
}

std::optional<Message> decode(const Bytes& b)
{
    Reader r{b};
    Message m;
    m.type = static_cast<MsgType>(r.u8());
    m.from = r.u16();
    switch (m.type) {
    case MsgType::Hello:
    case MsgType::Members: {
        auto n = r.u32();
        if (n > 4096) return std::nullopt;
        for (std::uint32_t i = 0; i < n && r.ok; i++) {
            NodeInfo ni; ni.id = r.u16(); ni.addr.host = r.str(); ni.addr.port = r.u16();
            m.members.push_back(std::move(ni));
        }
        break;
    }
    case MsgType::Heartbeat: m.load = r.u32(); break;
    case MsgType::SubmitTask: {
        m.task.id = r.u64(); m.task.fn = r.str(); m.task.output = r.u64(); m.task.submitter = r.u16();
        auto n = r.u32();
        if (n > 65536) return std::nullopt;
        for (std::uint32_t i = 0; i < n && r.ok; i++) m.task.args.push_back(r.u64());
        break;
    }
    case MsgType::GetObject:  m.object = r.u64(); break;
    case MsgType::ObjectData: m.object = r.u64(); m.data = r.bytes(); break;
    default: return std::nullopt;
    }
    if (!r.ok) return std::nullopt;
    return m;
}

} // namespace orion::cluster
