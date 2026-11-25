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
// connection comes back. A lost peer is pinged again (one datagram) with a backoff, and comes back into
// the k-buckets when it answers, as any node that sends a datagram; it adds us back on our ping too.
//
// Only a peer that answered at least one ping of ours while in the k-buckets is kept (its endpoint
// was real), at most kMaxPeers of them; when the list is full, the one that had been answering for
// the shortest time leaves first, so ids that come and go quickly cannot push out the peers of a long
// cut. A peer is forgotten kForgetAfter after it was lost. Not thread-safe: the routing table guards it.
class LostPeers {
 public:
  using Clock = std::chrono::steady_clock;

  static constexpr size_t kMaxPeers = 64;
  // pings sent per call of Due (the pinger calls it every 8 s)
  static constexpr size_t kMaxPerCall = 16;
  static constexpr std::chrono::seconds kFirstRetry{30};
  static constexpr std::chrono::seconds kMaxRetry{180};
  static constexpr std::chrono::hours kForgetAfter{24};

  // `tenure`: how long the peer had been answering in the k-buckets. False if it was not kept (the
  // list is full of peers with a tenure at least as long).
  bool Add(const NodeEntrance& node, Clock::duration tenure, Clock::time_point now) {
    auto it = Find(node.id);
    if (it != peers_.end()) {
      it->node = node;
      it->tenure = std::max(it->tenure, tenure);
      it->lost = now;
      it->next = now + kFirstRetry;
      it->attempts = 0;
      return true;
    }
    if (peers_.size() >= kMaxPeers) {
      auto shortest = std::min_element(peers_.begin(), peers_.end(),
                                       [](const Entry& a, const Entry& b) { return a.tenure < b.tenure; });
      if (shortest->tenure >= tenure) {
        return false;
      }
      peers_.erase(shortest);
    }
    peers_.push_back(Entry{node, tenure, now, now + kFirstRetry, 0});
    return true;
  }

  // when it was lost, if it was in the list
  std::optional<Clock::time_point> Remove(const NodeId& id) {
    auto it = Find(id);
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

  // The peers to ping now, at most kMaxPerCall, the longest waiting first. Each one's next ping moves
  // out: kFirstRetry after it was lost, then twice the previous wait each time up to kMaxRetry. The
  // peers lost kForgetAfter ago or longer are forgotten.
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
      result.push_back(e->node);
    }
    return result;
  }

  void Get(std::vector<NodeEntrance>& result) const {
    for (const auto& e : peers_) {
      result.push_back(e.node);
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

 private:
  struct Entry {
    NodeEntrance node;
    Clock::duration tenure;
    Clock::time_point lost;
    Clock::time_point next;
    uint32_t attempts;
  };

  std::vector<Entry>::iterator Find(const NodeId& id) {
    return std::find_if(peers_.begin(), peers_.end(), [&id](const Entry& e) { return e.node.id == id; });
  }

  std::vector<Entry> peers_;
};

} // namespace net
#endif // NET_LOST_PEERS_H
