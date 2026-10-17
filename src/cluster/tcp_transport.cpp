/* tcp_transport.cpp — built-in backend: one TCP connection per peer,
 * frames = u32 little-endian length + encoded Message. One reader thread
 * per connection; writes are serialised per connection with a mutex. */
#include "orion/cluster/transport.hpp"
#include <arpa/inet.h>
#include <cstring>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace orion::cluster {

namespace {

bool write_all(int fd, const void* buf, std::size_t len)
{
    const char* p = static_cast<const char*>(buf);
    while (len) {
        ssize_t n = ::send(fd, p, len, MSG_NOSIGNAL);
        if (n <= 0) return false;
        p += n; len -= static_cast<std::size_t>(n);
    }
    return true;
}
bool read_all(int fd, void* buf, std::size_t len)
{
    char* p = static_cast<char*>(buf);
    while (len) {
        ssize_t n = ::recv(fd, p, len, 0);
        if (n <= 0) return false;
        p += n; len -= static_cast<std::size_t>(n);
    }
    return true;
}

class TcpTransport final : public Transport {
    struct Conn {
        int fd;
        std::mutex wmu;
        NodeId peer{0};
        bool registered{false};
        explicit Conn(int f) : fd(f) {}
        ~Conn() { if (fd >= 0) ::close(fd); }
    };

public:
    ~TcpTransport() override { stop(); }

    void start(NodeId self, const NodeAddress& listen, Handler h) override
    {
        self_ = self; handler_ = std::move(h);
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        sockaddr_in sa{}; sa.sin_family = AF_INET; sa.sin_port = htons(listen.port);
        sa.sin_addr.s_addr = listen.host.empty() || listen.host == "0.0.0.0"
                             ? htonl(INADDR_ANY) : ::inet_addr(listen.host.c_str());
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0 || ::listen(listen_fd_, 64) != 0)
            throw std::runtime_error("TcpTransport: cannot listen on " + listen.str());
        acceptor_ = std::jthread([this] { accept_loop(); });
    }

    void stop() override
    {
        bool expected = false;
        if (!stopping_.compare_exchange_strong(expected, true)) return;
        if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);    /* wakes accept() */
        if (acceptor_.joinable()) acceptor_.join();
        if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
        std::vector<std::shared_ptr<Conn>> conns;
        {
            std::lock_guard lk(mu_);
            for (auto& [_, c] : by_peer_) conns.push_back(c);
            for (auto& c : anonymous_) conns.push_back(c);
            by_peer_.clear(); anonymous_.clear();
        }
        for (auto& c : conns) ::shutdown(c->fd, SHUT_RDWR);
        std::vector<std::jthread> threads;
        { std::lock_guard lk(mu_); threads.swap(readers_); }
        threads.clear();                                    /* joins every reader */
    }

    bool connect(NodeId peer, const NodeAddress& addr) override
    {
        {
            std::lock_guard lk(mu_);
            if (by_peer_.count(peer)) return true;
        }
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1; ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        sockaddr_in sa{}; sa.sin_family = AF_INET; sa.sin_port = htons(addr.port);
        sa.sin_addr.s_addr = ::inet_addr(addr.host == "localhost" ? "127.0.0.1" : addr.host.c_str());
        if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0) { ::close(fd); return false; }
        auto c = std::make_shared<Conn>(fd);
        c->peer = peer; c->registered = true;
        {
            std::lock_guard lk(mu_);
            if (by_peer_.count(peer)) return true;       /* lost the race: keep the existing one */
            by_peer_[peer] = c;
            readers_.emplace_back([this, c] { read_loop(c); });
        }
        return true;
    }

    bool send(NodeId peer, const Message& m) override
    {
        std::shared_ptr<Conn> c;
        {
            std::lock_guard lk(mu_);
            auto it = by_peer_.find(peer);
            if (it == by_peer_.end()) return false;
            c = it->second;
        }
        Bytes payload = encode(m);
        std::uint32_t len = payload.size();
        std::uint8_t hdr[4] = { std::uint8_t(len), std::uint8_t(len >> 8), std::uint8_t(len >> 16), std::uint8_t(len >> 24) };
        std::lock_guard wl(c->wmu);
        return write_all(c->fd, hdr, 4) && write_all(c->fd, payload.data(), payload.size());
    }

    bool connected(NodeId peer) const override { std::lock_guard lk(mu_); return by_peer_.count(peer) > 0; }

private:
    void accept_loop()
    {
        for (;;) {
            int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) return;                       /* listen socket closed */
            int one = 1; ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            auto c = std::make_shared<Conn>(fd);
            {
                std::lock_guard lk(mu_);
                if (stopping_) return;
                anonymous_.push_back(c);
                readers_.emplace_back([this, c] { read_loop(c); });
            }
        }
    }

    void read_loop(std::shared_ptr<Conn> c)
    {
        for (;;) {
            std::uint8_t hdr[4];
            if (!read_all(c->fd, hdr, 4)) break;
            std::uint32_t len = hdr[0] | hdr[1] << 8 | hdr[2] << 16 | std::uint32_t(hdr[3]) << 24;
            if (len > (64u << 20)) break;
            Bytes payload(len);
            if (!read_all(c->fd, payload.data(), len)) break;
            auto msg = decode(payload);
            if (!msg) break;
            if (!c->registered) {                     /* inbound: learn the peer from its first message */
                std::lock_guard lk(mu_);
                c->peer = msg->from; c->registered = true;
                /* if we already hold an outbound channel to this peer, keep
                 * using that one for sends; this socket stays receive-only
                 * and remains in `anonymous_` so stop() can still close it */
                if (!by_peer_.count(msg->from)) { by_peer_[msg->from] = c; std::erase(anonymous_, c); }
            }
            handler_(*msg);
        }
        std::lock_guard lk(mu_);
        auto it = by_peer_.find(c->peer);
        if (it != by_peer_.end() && it->second == c) by_peer_.erase(it);
        std::erase(anonymous_, c);
    }

    NodeId self_{};
    Handler handler_;
    int listen_fd_{-1};
    std::jthread acceptor_;
    std::atomic<bool> stopping_{false};
    mutable std::mutex mu_;
    std::map<NodeId, std::shared_ptr<Conn>> by_peer_;
    std::vector<std::shared_ptr<Conn>> anonymous_;   /* inbound sockets not used for sending */
    std::vector<std::jthread> readers_;
};

} // namespace

std::unique_ptr<Transport> make_tcp_transport() { return std::make_unique<TcpTransport>(); }

} // namespace orion::cluster
