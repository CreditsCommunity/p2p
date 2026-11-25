#ifndef NET_PING_REGISTRY_H
#define NET_PING_REGISTRY_H

#include <chrono>
#include <optional>
#include <unordered_map>

#include "common.h"

namespace net {

// The pings the pinger has outstanding, one per node id, each bound to the endpoint it was sent to.
//
// A ping response carries no nonce: the official wire format has none, and official peers build their
// response from their own host data, so they could not echo one. A response therefore counts as the
// answer to our ping only when it comes from the exact endpoint (address and UDP port) the latest ping
// to that id went to, within `window` of that ping; a response from anywhere else, or late, changes
// nothing (the ping stays outstanding and is retried or times out as before). Residual risk: whoever
// can receive or guess a ping to that endpoint and send datagrams with that endpoint's address as
// source (on the path, or spoofing the source address within the window) can still answer for it; a
// binding that excludes this needs an authenticated handshake, a wire change agreed with the official
// nodes. Not thread-safe: the pinger guards it.
class PingRegistry {
 public:
  using Clock = std::chrono::steady_clock;

  explicit PingRegistry(Clock::duration window) : window_(window) {}

  // A ping sent to `to` now. A new entry starts at `initial_count`; an id with a ping outstanding
  // counts one more unanswered ping, and its endpoint and time become this ping's.
  void Sent(const NodeEntrance& to, uint8_t initial_count, Clock::time_point now) {
    auto it = pings_.find(to.id);
    if (it == pings_.end()) {
      pings_.emplace(to.id, Ping{initial_count, to.address, to.udp_port, now});
      return;
    }
    ++it->second.unanswered;
    it->second.address = to.address;
    it->second.udp_port = to.udp_port;
    it->second.sent = now;
  }

  // True when `from` (its id and the endpoint its datagram came from) answers the outstanding ping to
  // that id: same address and UDP port, within the window. The ping is then done.
  bool Answer(const NodeEntrance& from, Clock::time_point now) {
    auto it = pings_.find(from.id);
    if (it == pings_.end()) {
      return false;
    }
    const auto& p = it->second;
    if (p.address != from.address || p.udp_port != from.udp_port || now < p.sent || now - p.sent > window_) {
      return false;
    }
    pings_.erase(it);
    return true;
  }

  // The latest ping to that id went to `holder`'s endpoint (address, UDP port), is still unanswered,
  // and its whole window has passed: a response window that really expired. The pinger asks this in
  // its timer, never from the count of pings sent (which goes up when a ping is sent, and which a
  // replacement ping provoked by anyone starts high).
  bool Overdue(const NodeEntrance& holder, Clock::time_point now) const {
    auto it = pings_.find(holder.id);
    if (it == pings_.end()) {
      return false;
    }
    const auto& p = it->second;
    return p.address == holder.address && p.udp_port == holder.udp_port && now >= p.sent && now - p.sent >= window_;
  }

  // the unanswered pings of an outstanding id
  std::optional<uint8_t> Unanswered(const NodeId& id) const {
    auto it = pings_.find(id);
    if (it == pings_.end()) {
      return std::nullopt;
    }
    return it->second.unanswered;
  }

  void Erase(const NodeId& id) { pings_.erase(id); }
  void Clear() { pings_.clear(); }

 private:
  struct Ping {
    uint8_t unanswered;
    bi::address address;
    uint16_t udp_port;
    Clock::time_point sent;
  };

  const Clock::duration window_;
  std::unordered_map<NodeId, Ping> pings_;
};

} // namespace net
#endif // NET_PING_REGISTRY_H
