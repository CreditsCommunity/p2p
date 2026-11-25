#ifndef NET_ROUTING_TABLE_H
#define NET_ROUTING_TABLE_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <limits>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <vector>

#include "common.h"
#include "k_bucket.h"
#include "kademlia_datagram.h"
#include "lost_peers.h"
#include "ping_registry.h"
#include "udp.h"

namespace net {

enum class RoutingTableEventType {
  kNodeAdded,
  kNodeRemoved,
  kNodeFound,
  kNodeNotFound
};

// Routing table's owner must implement this interface
class RoutingTableEventHandler {
 public:
  virtual ~RoutingTableEventHandler() = default;
  virtual void HandleRoutTableEvent(const NodeEntrance&, RoutingTableEventType) = 0;
  virtual bool IsEndpointBanned(const bi::address& addr, uint16_t port) = 0;
};

class RoutingTable : public UdpSocketEventHandler {
 public:
  RoutingTable(ba::io_context& io, RoutingTableEventHandler& host);
  ~RoutingTable() override;

  // Ends the pinger and explorer threads and closes the UDP socket. Called once the io_context
  // has stopped and its threads have ended (~Host), so that nothing else uses the socket.
  // Idempotent; the destructor calls it too.
  void Stop();

  // starts lookup if not started yet
  void AddNodes(const std::vector<NodeEntrance>&);

  // Empties the k-buckets (the host hears every node removed), forgets the pings and lookups in
  // flight and looks the network up again from `nodes`: a node comes back when it answers, with
  // the endpoint its datagrams come from.
  void Reset(const std::vector<NodeEntrance>& nodes);

  bool HasNode(const NodeId&, NodeEntrance&);
  void StartFindNode(const NodeId&);

  void GetKnownNodes(std::vector<NodeEntrance>&);
  // the k-buckets' nodes, whatever full discovery says
  void GetBucketNodes(std::vector<NodeEntrance>&);

  std::vector<NodeEntrance> GetBroadcastList(const NodeId&);

  // the peers lost because they stopped answering, still to be pinged again (see LostPeers)
  void GetLostNodes(std::vector<NodeEntrance>&);

  // Called by the pinger thread on each of its rounds: pings the lost peers whose turn it is (one
  // datagram each, no eviction or retry of its own), and every kBootRecheckInterval the configured
  // boot nodes that are not in the k-buckets. A node that answers comes back like any node that sends
  // a datagram, and the node pinged adds us as well. So two parts of the network cut off from each
  // other for longer than the pinger waits find each other again when the cut ends, whichever side's
  // boot nodes they list.
  void PingLostAndBootNodes();

  static NodeId Distance(const NodeId&, const NodeId&);
  static uint16_t KBucketIndex(const NodeId& target, const NodeId& id);

  static constexpr uint16_t kIvalidIndex = std::numeric_limits<uint16_t>::max();

 protected:
  void OnSocketClosed(const boost::system::error_code&) override {}

  void OnPacketReceived(const bi::udp::endpoint& from,
                        const ByteVector& data) override;

 private:
  static constexpr uint16_t kMaxDatagramSize = 1472; // 1500(ethernet payload) - 20(ip header) - 8(udp header)

  // k should be chosen such that any given k nodes
  // are very unlikely to fail within an hour of each other
  static constexpr uint8_t k = 16;

  // if kBroadcastReplication == k, topology based broadcast becomes flooding;
  // if it equals to 1, it takes logN time to spread a message through network
  // but whole subtrees may not receive the message
  static constexpr uint8_t kBroadcastReplication = 5;

  static constexpr uint8_t kMaxPingsBeforeRemove = 3;
  static constexpr std::chrono::seconds kPingExpirationSeconds{8};

  static constexpr std::chrono::seconds kDiscoveryInterval{60};
  static constexpr std::chrono::seconds kDiscoveryExpirationSeconds{30};

  // how often the boot nodes missing from the k-buckets are pinged again, and how many at most
  static constexpr std::chrono::seconds kBootRecheckInterval{600};
  static constexpr size_t kMaxBootRecheck = 64;
  // the lookups a lost peer that comes back starts: at most one in this interval
  static constexpr std::chrono::seconds kRediscoveryLookupInterval{10};

  bool CheckEndpoint(const KademliaDatagram&);

  void HandlePing(const KademliaDatagram&);
  void HandleFindNode(const KademliaDatagram&);

  uint16_t KBucketIndex(const NodeId& id) const noexcept;

  // Adds the sender of a direct datagram, whose endpoint CheckEndpoint matched, or promotes it.
  // answered: the datagram answers a ping of ours (verified, see PingRegistry); refresh_user_data:
  // it did and carried user data, so the sender's entry takes its user data. Only for that sender: a
  // node listed by another one (FindNode replies) is never written here.
  void UpdateKBuckets(const NodeEntrance& sender, bool answered, bool refresh_user_data);

  // the k-buckets hold that id at that address and UDP port
  bool HoldsEndpoint(const NodeEntrance&);

  // a lost peer answered our ping from its recorded endpoint and left the lost list: logged, and a
  // lookup through it so that its side learns of us and we of its side
  void OnLostPeerBack(const NodeEntrance&, std::chrono::steady_clock::time_point lost_at);

  // the pinger evicted this node of the k-buckets (k_bucket_mux_ held): kept as a lost peer if it
  // had answered a ping of ours
  void OnEvicted(const NodeEntrance&);


  static NodeId RandomId();

  void OnNodeFound(const NodeEntrance&);
  void OnNodeNotFound(const NodeId&);

  void NotifyHost(const NodeEntrance& node, RoutingTableEventType);

  // Returns k closest nodes to target id.
  // Or total_nodes_ nodes if total_nodes_ < k.
  std::vector<NodeEntrance> NearestNodes(const NodeId&);

  template<class KademliaDatagram>
  void SendToSocket(KademliaDatagram&&, const std::vector<NodeEntrance>& dest_nodes);

  class NetExplorer {
   public:
    NetExplorer(RoutingTable&, bool full_discovery);
    ~NetExplorer();

    void Start(std::future<void>&& stop_condition);
    void Join();
    void Find(const NodeId&, const std::vector<NodeEntrance>& find_list);
    void CheckFindNodeResponce(const KademliaDatagram&);
    void GetKnownNodes(std::vector<NodeEntrance>&);
    // forgets the lookups in flight
    void Clear();

   private:
    void DiscoveryRoutine(std::future<void>&&);
    void UpdateNodes();
    void UpdateNodes(const std::vector<NodeEntrance>&);

    RoutingTable& routing_table_;
    const bool full_discovery_;
    std::thread discovery_thread_;

    Mutex find_node_mux_;
    std::unordered_map<NodeId, std::vector<NodeId>> find_node_sent_;
    // bumped by Clear: the timer of a lookup started before ends nothing started after
    uint64_t find_generation_ = 0;

    static constexpr std::chrono::seconds kUpdateNodesInterval{60 * 10};
    Mutex nodes_mux_;
    struct Nodes {
      struct NodeEqual {
        bool operator()(const NodeEntrance& l, const NodeEntrance& r) const {
          return l.id == r.id;
        }
      };

      std::unordered_set<NodeEntrance, std::hash<NodeEntrance>, NodeEqual> actual;
      std::unordered_set<NodeEntrance, std::hash<NodeEntrance>, NodeEqual> updates;
    } nodes_;
  };

  class Pinger {
   public:
    Pinger(RoutingTable&);
    ~Pinger();

    void Start(std::future<void>&& stop_condition);
    void Join();
    void SendPing(const NodeEntrance& target, KBucket& bucket,
                  std::shared_ptr<NodeEntrance> replacer = nullptr);
    // true when the response answers an outstanding ping of ours: from the endpoint that ping went
    // to, within kPingExpirationSeconds of it (see PingRegistry)
    bool CheckPingResponce(const KademliaDatagram&);
    // forgets the pings in flight (their timers then find nothing to do)
    void Clear();


   private:
    void PingRoutine(std::future<void>&&);

    RoutingTable& routing_table_;
    std::thread ping_thread_;

    Mutex ping_mux_;
    PingRegistry ping_sent_;
  };

  const NodeEntrance host_data_;
  UdpSocket<kMaxDatagramSize>::Ptr socket_;
  RoutingTableEventHandler& host_;

  ba::io_context& io_;

  const uint16_t kBucketsNum;
  Mutex k_bucket_mux_;
  KBucket* k_buckets_;
  std::atomic<size_t> total_nodes_{0};

  // since when each k-bucket node has been answering our pings (its first answer while in the
  // k-buckets); guarded by k_bucket_mux_
  std::unordered_map<NodeId, std::chrono::steady_clock::time_point> answering_since_;

  // after k_bucket_mux_ when both are held, never the other way round
  Mutex lost_mux_;
  LostPeers lost_;
  // the pinger thread's only
  std::chrono::steady_clock::time_point next_boot_recheck_;
  std::atomic<std::chrono::steady_clock::time_point> last_rediscovery_lookup_{};

  Pinger pinger_;
  NetExplorer explorer_;

  std::promise<void> pinger_stopper_;
  std::promise<void> discovery_stopper_;
  std::atomic<bool> stopped_{false};

  // NotifyHost threads still running (Stop waits for them, see there)
  Mutex notify_mux_;
  std::condition_variable notify_cv_;
  size_t notify_running_ = 0;
};

inline NodeId RoutingTable::Distance(const NodeId& a, const NodeId& b) {
  return a ^ b;
}

inline uint16_t RoutingTable::KBucketIndex(const NodeId& id) const noexcept {
  return RoutingTable::KBucketIndex(host_data_.id, id);
}

inline uint16_t RoutingTable::KBucketIndex(const NodeId& target, const NodeId& id) {
  static size_t max_id_bits = id.size() * 8;

  auto clz = Distance(target, id).GetCLZ();
  return clz == max_id_bits ? kIvalidIndex : clz;
}

template<class KademliaDatagram>
void RoutingTable::SendToSocket(KademliaDatagram&& d, const std::vector<NodeEntrance>& nodes) {
  for (auto& dest : nodes) {
    socket_->Send(d.ToUdp(dest));
  }
}

} // namespace net
#endif // NET_ROUTING_TABLE_H
