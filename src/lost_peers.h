#ifndef NET_LOST_PEERS_H
#define NET_LOST_PEERS_H

#include <algorithm>
#include <chrono>
#include <optional>
#include <vector>

#include "common.h"

namespace net {

// The peers the routing table lost because they stopped answering its pings, kept to be pinged again.
//
// The pinger evicts a node after kMaxPingsBeforeRemove unanswered pings (about half a minute). After a
// network cut every node of the other side is evicted that way, and nothing sends it a datagram again:
// the k-buckets keep only the reachable side, the lookups ask only that side, and the boot nodes are
// looked up again only when the table is empty. The two sides then never find each other when the
// connection comes back. A lost peer is pinged again (one datagram) at its recorded endpoint, with a
// backoff; its answer brings it back into the k-buckets, and our ping makes it add us back too.
//
// Trust model. A node id is a public key, but nothing in the p2p protocol proves it: a datagram's id is
// the sender's word, and an official peer's ping response carries no nonce or signature (it is built
// from the peer's own host data) and cannot be made to. So ids are unauthenticated, as in the official
// Kademlia; what is bound is the endpoint. Entries are kept per endpoint, (id, address, UDP port): a
// node that claims an id from another endpoint adds an entry of its own (at most kMaxEndpointsPerId per
// id) and cannot replace, pin or refresh another endpoint's entry. An entry is "verified" only by a
// ping response from its own endpoint within the response window of the latest ping Due gave for it
// (Verified). It leaves only once that endpoint is back in the k-buckets and has answered a ping of
// ours there (Remove, by the routing table). Nothing here ever displaces an entry of the k-buckets: a
// lost peer whose id they hold at another endpoint is kept out by the endpoint check, as in the
// official code, until the pinger evicts that holder (three unanswered pings); the lost entry is
// pinged meanwhile and comes back by the usual add. Re-adding an entry never refreshes when it was
// lost, so it is forgotten kForgetAfter after the first loss.
//
// Only a peer whose answer to a ping of ours was verified the same way while it was in the k-buckets
// is kept (see PingRegistry), at most kMaxPeers entries; when full, the entry that had been answering
// for the shortest time leaves first, so ids that come and go quickly cannot push out the peers of a
// long cut. Not thread-safe: the routing table guards it.
class LostPeers {
 public:
  using Clock = std::chrono::steady_clock;

  static constexpr size_t kMaxPeers = 64;
  static constexpr size_t kMaxEndpointsPerId = 4;
  // pings sent per call of Due (the pinger calls it every 8 s)
  static constexpr size_t kMaxPerCall = 16;
  static constexpr std::chrono::seconds kFirstRetry{30};
  static constexpr std::chrono::seconds kMaxRetry{180};
  static constexpr std::chrono::hours kForgetAfter{24};

  // `tenure`: how long the peer had been answering at that endpoint in the k-buckets. An entry already
  // there for the same id and endpoint keeps its loss time and schedule (only a longer tenure is
  // taken). False if not kept: the list, or the id's endpoints, are full of entries with a tenure at
  // least as long.
  bool Add(const NodeEntrance& node, Clock::duration tenure, Clock::time_point now) {
    auto it = Find(node);
    if (it != peers_.end()) {
      it->tenure = std::max(it->tenure, tenure);
      return true;
    }
    if (!MakeRoom([&node](const Entry& e) { return e.node.id == node.id; }, kMaxEndpointsPerId, tenure) ||
        !MakeRoom([](const Entry&) { return true; }, kMaxPeers, tenure)) {
      return false;
    }
    peers_.push_back(Entry{node, tenure, now, now + kFirstRetry, 0, std::nullopt});
    return true;
  }

  // `from` (an id and the endpoint its ping response came from) answers the latest ping Due gave for
  // that entry: the same id, address and UDP port, within `window` of that ping. The result is when it
  // was lost; nothing changes (Remove, once it is back in the k-buckets at that endpoint).
  std::optional<Clock::time_point> Verified(const NodeEntrance& from, Clock::time_point now, Clock::duration window) const {
    auto it = std::find_if(peers_.begin(), peers_.end(), [&from](const Entry& e) { return SameEndpoint(e.node, from); });
    if (it == peers_.end() || !it->pinged || now < *it->pinged || now - *it->pinged > window) {
      return std::nullopt;
    }
    return it->lost;
  }

  // that id at that endpoint is back (the routing table holds it there and it answered a ping of
  // ours there): when it was lost, if it was in the list
  std::optional<Clock::time_point> Remove(const NodeEntrance& node) {
    auto it = Find(node);
    if (it == peers_.end()) {
      return std::nullopt;
    }
    auto lost = it->lost;
    peers_.erase(it);
    return lost;
  }

  bool Contains(const NodeId& id) const {
    return std::find_if(peers_.begin(), peers_.end(), [&id](const Entry& e) { return e.node.id == id; }) != peers_.end();
  }

  size_t Size() const noexcept { return peers_.size(); }

  // The entries to ping now, at their endpoints whatever the k-buckets hold for their ids: at most
  // kMaxPerCall, the longest waiting first; the time of each ping is recorded (Verified). Each one's
  // next ping moves out: kFirstRetry after it was lost, then twice the previous wait each time up to
  // kMaxRetry. Entries lost kForgetAfter ago or longer are forgotten.
  std::vector<NodeEntrance> Due(Clock::time_point now) {
    peers_.erase(std::remove_if(peers_.begin(), peers_.end(),
                                [now](const Entry& e) { return now - e.lost >= kForgetAfter; }),
                 peers_.end());

    std::vector<Entry*> due;
    for (auto& e : peers_) {
      if (e.next <= now) {
        due.push_back(&e);
      }
    }
    std::sort(due.begin(), due.end(), [](const Entry* a, const Entry* b) { return a->next < b->next; });
    if (due.size() > kMaxPerCall) {
      due.resize(kMaxPerCall);
    }

    std::vector<NodeEntrance> result;
    result.reserve(due.size());
    for (auto* e : due) {
      ++e->attempts;
      e->next = now + Wait(e->attempts);
      e->pinged = now;
      result.push_back(e->node);
    }
    return result;
  }

  // every entry, the most recently lost first (an id may come with several endpoints: the first one
  // of an id is the endpoint it was last seen at)
  void Get(std::vector<NodeEntrance>& result) const {
    std::vector<const Entry*> sorted;
    for (const auto& e : peers_) {
      sorted.push_back(&e);
    }
    std::stable_sort(sorted.begin(), sorted.end(), [](const Entry* a, const Entry* b) { return a->lost > b->lost; });
    for (const auto* e : sorted) {
      result.push_back(e->node);
    }
  }

  // the wait after the attempt-th ping: kFirstRetry doubled attempt times, at most kMaxRetry
  static Clock::duration Wait(uint32_t attempts) {
    Clock::duration wait = kFirstRetry;
    for (uint32_t i = 0; i < attempts && wait < kMaxRetry; ++i) {
      wait *= 2;
    }
    return std::min<Clock::duration>(wait, kMaxRetry);
  }

  static bool SameEndpoint(const NodeEntrance& a, const NodeEntrance& b) {
    return a.id == b.id && a.address == b.address && a.udp_port == b.udp_port;
  }

 private:
  struct Entry {
    NodeEntrance node;
    Clock::duration tenure;
    Clock::time_point lost;
    Clock::time_point next;
    uint32_t attempts;
    // the latest ping Due gave for it
    std::optional<Clock::time_point> pinged;
  };

  std::vector<Entry>::iterator Find(const NodeEntrance& node) {
    return std::find_if(peers_.begin(), peers_.end(), [&node](const Entry& e) { return SameEndpoint(e.node, node); });
  }

  // at most `cap` entries matching `pred` once one is added: the shortest tenure among them leaves if
  // shorter than `tenure`; false if none does
  template <class Pred>
  bool MakeRoom(Pred pred, size_t cap, Clock::duration tenure) {
    size_t count = 0;
    auto shortest = peers_.end();
    for (auto it = peers_.begin(); it != peers_.end(); ++it) {
      if (!pred(*it)) continue;
      ++count;
      if (shortest == peers_.end() || it->tenure < shortest->tenure) shortest = it;
    }
    if (count < cap) return true;
    if (shortest->tenure >= tenure) return false;
    peers_.erase(shortest);
    return true;
  }

  std::vector<Entry> peers_;
};

} // namespace net
#endif // NET_LOST_PEERS_H
