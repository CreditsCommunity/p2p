#include "routing_table.h"

#include <algorithm>
#include <random>
#include <thread>

#include "network.h"
#include "utils/log.h"

namespace net {

RoutingTable::RoutingTable(ba::io_context& io,
                           RoutingTableEventHandler& host)
    : host_data_(Network::Instance().GetHostContacts()),
      socket_(UdpSocket<kMaxDatagramSize>::Create(io,
          bi::udp::endpoint(host_data_.address, host_data_.udp_port),
          static_cast<UdpSocketEventHandler&>(*this))),
      host_(host),
      io_(io),
      kBucketsNum(static_cast<uint16_t>(host_data_.id.size() * 8)), // num of bits in NodeId
      k_buckets_(new KBucket[kBucketsNum]),
      pinger_(*this),
      explorer_(*this, Network::Instance().GetConfig().full_net_discovery) {
  // Host::Run has just looked the boot nodes up
  next_boot_recheck_ = std::chrono::steady_clock::now() + kBootRecheckInterval;
  socket_->Open();

  pinger_.Start(pinger_stopper_.get_future());
  explorer_.Start(discovery_stopper_.get_future());
}

RoutingTable::~RoutingTable() {
  // the pinger and explorer threads use the k-buckets: they end first
  Stop();

  Guard g(k_bucket_mux_);
  delete []k_buckets_;
}

void RoutingTable::Stop() {
  if (stopped_.exchange(true)) return;

  pinger_stopper_.set_value();
  discovery_stopper_.set_value();

  // the threads send on the socket: closed once they have ended
  pinger_.Join();
  explorer_.Join();

  socket_->Close();

  // The io thread and the pinger and explorer threads have ended (~Host joins the io thread
  // first), so no new notification starts; those still running call into the host, which is
  // being destroyed: wait for them, a bounded time so that a blocked handler cannot hang the exit.
  UniqueGuard g(notify_mux_);
  if (!notify_cv_.wait_for(g, std::chrono::seconds(5), [this] { return notify_running_ == 0; })) {
    LOG(ERROR) << "Routing table stopped with " << notify_running_ << " host notification(s) still running";
  }
}

void RoutingTable::AddNodes(const std::vector<NodeEntrance>& nodes) {
  if (total_nodes_.load() == 0) {
    explorer_.Find(host_data_.id, nodes);
  } else {
    Guard g(k_bucket_mux_);
    for (auto& n : nodes) {
      if (n.id == host_data_.id) continue;
      pinger_.SendPing(n, k_buckets_[KBucketIndex(n.id)]);
    }
  }
}

void RoutingTable::Reset(const std::vector<NodeEntrance>& nodes) {
  std::vector<NodeEntrance> removed;
  {
    Guard g(k_bucket_mux_);
    for (size_t i = 0; i < kBucketsNum; ++i) {
      const auto& bucket_nodes = k_buckets_[i].GetNodes();
      removed.insert(removed.end(), bucket_nodes.begin(), bucket_nodes.end());
      k_buckets_[i].Clear();
    }
    total_nodes_ = 0;
    // the lost peers stay: they are not in `nodes` and still to be pinged again
    answering_since_.clear();
  }
  pinger_.Clear();
  explorer_.Clear();

  for (const auto& n : removed) {
    NotifyHost(n, RoutingTableEventType::kNodeRemoved);
  }

  // the table is empty: a lookup of our own id from these nodes (AddNodes)
  std::vector<NodeEntrance> unique;
  for (const auto& n : nodes) {
    if (n.id == host_data_.id) continue;
    if (std::find_if(unique.begin(), unique.end(), [&n](const NodeEntrance& u) { return u.id == n.id; }) == unique.end()) {
      unique.push_back(n);
    }
  }
  AddNodes(unique);
}

bool RoutingTable::HasNode(const NodeId& id, NodeEntrance& result) {
  Guard g(k_bucket_mux_);
  return k_buckets_[KBucketIndex(id)].Get(id, result);
}

void RoutingTable::StartFindNode(const NodeId& id) {
  explorer_.Find(id, NearestNodes(id));
}

void RoutingTable::GetKnownNodes(std::vector<NodeEntrance>& result) {
  result.clear();

  if (Network::Instance().GetConfig().full_net_discovery) {
    explorer_.GetKnownNodes(result);

    // The explorer keeps the user data it first saw for a node, often from another node's reply.
    // A node in the k-buckets has the value of its own latest datagram: that one is shown.
    Guard g(k_bucket_mux_);
    for (auto& n : result) {
      auto index = KBucketIndex(n.id);
      if (index == kIvalidIndex) continue;
      NodeEntrance direct;
      if (k_buckets_[index].Get(n.id, direct) && direct.user_data) n.user_data = direct.user_data;
    }
    return;
  }

  Guard g(k_bucket_mux_);
  for (size_t i = 0; i < kBucketsNum; ++i) {
    const auto& nodes = k_buckets_[i].GetNodes();
    result.insert(result.end(), nodes.begin(), nodes.end());
  }
}

void RoutingTable::GetBucketNodes(std::vector<NodeEntrance>& result) {
  result.clear();
  Guard g(k_bucket_mux_);
  for (size_t i = 0; i < kBucketsNum; ++i) {
    const auto& nodes = k_buckets_[i].GetNodes();
    result.insert(result.end(), nodes.begin(), nodes.end());
  }
}

void RoutingTable::GetLostNodes(std::vector<NodeEntrance>& result) {
  result.clear();
  Guard g(lost_mux_);
  lost_.Get(result);
}

void RoutingTable::PingLostAndBootNodes() {
  using namespace std::chrono;
  const auto now = steady_clock::now();

  // the lost peers at their recorded endpoint, even when their id is in the k-buckets again: an entry
  // leaves the list only when that endpoint answers (LostPeers::Answered, in OnPacketReceived)
  std::vector<NodeEntrance> targets;
  {
    Guard g(lost_mux_);
    targets = lost_.Due(now);
  }

  if (now >= next_boot_recheck_) {
    next_boot_recheck_ = now + kBootRecheckInterval;
    auto& config = Network::Instance().GetConfig();
    const auto boot_nodes = config.use_default_boot_nodes ? GetDefaultBootNodes() : config.custom_boot_nodes;
    for (size_t i = 0; i < boot_nodes.size() && i < kMaxBootRecheck; ++i) {
      const auto& n = boot_nodes[i];
      if (n.id == host_data_.id) continue;
      Guard g(k_bucket_mux_);
      auto index = KBucketIndex(n.id);
      if (index != kIvalidIndex && k_buckets_[index].Exists(n.id)) continue;
      targets.push_back(n);
    }
  }

  size_t sent = 0;
  for (const auto& n : targets) {
    if (n.id == host_data_.id) continue;
    if (host_.IsEndpointBanned(n.address, n.udp_port)) continue;

    PingDatagram ping(host_data_);
    socket_->Send(ping.ToUdp(n));
    ++sent;
  }

  if (sent) {
    LOG(DEBUG) << "Rediscovery: pinged " << sent << " lost peer(s) or boot node(s) missing from the routing table";
  }
}

void RoutingTable::OnEvicted(const NodeEntrance& node) {
  auto it = answering_since_.find(node.id);
  if (it == answering_since_.end()) return;  // never answered a ping of ours
  const auto now = std::chrono::steady_clock::now();
  const auto tenure = now - it->second;
  answering_since_.erase(it);

  bool kept = false;
  {
    Guard g(lost_mux_);
    kept = lost_.Add(node, tenure, now);
  }
  LOG(INFO) << "Rediscovery: " << IdToBase58(node.id) << " stopped answering after "
            << std::chrono::duration_cast<std::chrono::seconds>(tenure).count() << " s in the routing table"
            << (kept ? ", it will be pinged again" : ", not kept (lost list full)");
}

std::vector<NodeEntrance> RoutingTable::GetBroadcastList(const NodeId& received_from) {
  std::vector<NodeEntrance> ret;
  int32_t index = KBucketIndex(received_from);
  if (index == kIvalidIndex) index = -1;

  Guard g(k_bucket_mux_);
  for (int32_t i = kBucketsNum - 1; i > index; --i) {
    if (!k_buckets_[i].Size()) continue;

    uint8_t added_in_subtree = 0;
    auto& nodes = k_buckets_[i].GetNodes();
    for (auto& n : nodes) {
      ret.push_back(n);
      if (++added_in_subtree == kBroadcastReplication) break;
    }
  }

  return ret;
}

void RoutingTable::OnPacketReceived(const bi::udp::endpoint& from, const ByteVector& data) {
  if (host_.IsEndpointBanned(from.address(), from.port())) return;

  auto packet = KademliaDatagram::ReinterpretUdpPacket(from, data);
  if (!packet) return;

  if (!CheckEndpoint(*packet)) {
    LOG(DEBUG) << "Endpoint check failed from " << from.address() << ", " << from.port();
    return;
  }

  // a node's user data (version, capabilities) is refreshed only from the answer to a ping of
  // ours: an unsolicited datagram with a forged source address cannot change it
  bool answered = false;
  bool refresh_user_data = false;
  // a lost peer answering, from its recorded endpoint, the ping PingLostAndBootNodes sent it
  std::optional<std::chrono::steady_clock::time_point> lost_at;

  switch (packet->DatagramType()) {
    case PingDatagram::type :
      HandlePing(*packet);
      break;
    case PingRespDatagram::type : {
      answered = pinger_.CheckPingResponce(*packet);
      refresh_user_data = answered && packet->has_user_data;
      Guard g(lost_mux_);
      lost_at = lost_.Answered(packet->node_from, std::chrono::steady_clock::now(), kPingExpirationSeconds);
      break;
    }
    case FindNodeDatagram::type :
      HandleFindNode(*packet);
      break;
    case FindNodeRespDatagram::type :
      explorer_.CheckFindNodeResponce(*packet);
      break;
    default :
      break;
  }

  UpdateKBuckets(packet->node_from, answered, refresh_user_data);

  if (lost_at) {
    OnLostPeerBack(packet->node_from, *lost_at);
  }
}

bool RoutingTable::CheckEndpoint(const KademliaDatagram& d) {
  if (host_data_.id == d.node_from.id) {
    return false;
  }

  NodeEntrance existing_contacts;
  auto& node_from = d.node_from;
  if (!HasNode(node_from.id, existing_contacts)) return true;

  return node_from.address == existing_contacts.address &&
         node_from.udp_port == existing_contacts.udp_port;
}

void RoutingTable::HandlePing(const KademliaDatagram& d) {
  PingRespDatagram answer(host_data_);
  socket_->Send(answer.ToUdp(d.node_from));
}

void RoutingTable::HandleFindNode(const KademliaDatagram& d) {
  auto& find_node = dynamic_cast<const FindNodeDatagram&>(d);
  auto requested_nodes = NearestNodes(find_node.target);

  FindNodeRespDatagram answer(host_data_, find_node.target, std::move(requested_nodes));
  socket_->Send(answer.ToUdp(d.node_from));
}

void RoutingTable::UpdateKBuckets(const NodeEntrance& node, bool answered, bool refresh_user_data) {
  if (node.id == host_data_.id) return;

  {
    Guard g(k_bucket_mux_);
    KBucket& bucket = k_buckets_[KBucketIndex(node.id)];
    NodeEntrance existing;
    if (bucket.Get(node.id, existing)) {
      const bool same_endpoint = existing.address == node.address && existing.udp_port == node.udp_port;
      // The entry is refreshed only from its own endpoint (checked again under the lock: the entry
      // may have changed since CheckEndpoint), and only its user data (version, capabilities):
      // address and ports stay as recorded.
      if (refresh_user_data && existing.user_data != node.user_data && same_endpoint) {
        existing.user_data = node.user_data;
        bucket.Update(existing);
      }
      if (answered && same_endpoint) {
        answering_since_.emplace(node.id, std::chrono::steady_clock::now());
      }
      bucket.Promote(node.id);
    } else if (bucket.Size() < k) {
      bucket.AddNode(node);
      ++total_nodes_;
      if (answered) {
        answering_since_.emplace(node.id, std::chrono::steady_clock::now());
      }
      NotifyHost(node, RoutingTableEventType::kNodeAdded);
    } else {
      pinger_.SendPing(bucket.LeastRecentlySeen(), bucket, std::make_shared<NodeEntrance>(node));
    }
  }
}

void RoutingTable::OnLostPeerBack(const NodeEntrance& node, std::chrono::steady_clock::time_point lost_at) {
  const auto now = std::chrono::steady_clock::now();
  LOG(INFO) << "Rediscovery: " << IdToBase58(node.id) << " answers again at its endpoint "
            << std::chrono::duration_cast<std::chrono::seconds>(now - lost_at).count()
            << " s after it stopped answering";
  // A lookup of a random id through it, as the explorer makes every minute through the nearest
  // nodes: every node the lookup reaches hears from us and adds us, and those that answer come back
  // here. At most one every kRediscoveryLookupInterval, however many lost peers come back at once.
  auto last = last_rediscovery_lookup_.load();
  if (now - last >= kRediscoveryLookupInterval && last_rediscovery_lookup_.compare_exchange_strong(last, now)) {
    explorer_.Find(RandomId(), {node});
  }
}

NodeId RoutingTable::RandomId() {
  thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<uint32_t> dist;
  NodeId id;
  uint32_t* ptr = id.GetPtr();
  std::generate(ptr, ptr + id.size() / sizeof(uint32_t), [&]() -> uint32_t { return dist(gen); });
  return id;
}

std::vector<NodeEntrance> RoutingTable::NearestNodes(const NodeId& target) {
  auto comparator = [](const std::pair<uint16_t, NodeEntrance>& n1,
                       const std::pair<uint16_t, NodeEntrance>& n2) {
    return n1.first > n2.first;
  };

  std::multiset<std::pair<uint16_t, NodeEntrance>, decltype(comparator)>
      nearest_nodes(comparator);

  {
   Guard g(k_bucket_mux_);
   for (size_t i = 0; i < kBucketsNum; ++i) {
     auto& nodes = k_buckets_[i].GetNodes();
     std::for_each(nodes.begin(), nodes.end(),
                     [&nearest_nodes, &target](const NodeEntrance& node) {
                       nearest_nodes.insert(
                         std::make_pair(RoutingTable::KBucketIndex(target, node.id), node));

                       if (nearest_nodes.size() > k) {
                         nearest_nodes.erase(--nearest_nodes.end());
                       }
                     });
   }
  }

  std::vector<NodeEntrance> ret;
  for (auto& n : nearest_nodes) {
    ret.emplace_back(std::move(n.second));
  }
  return ret;
}

void RoutingTable::OnNodeFound(const NodeEntrance& node) {
  if (node.id == host_data_.id) return;
  NotifyHost(node, RoutingTableEventType::kNodeFound);
}

void RoutingTable::OnNodeNotFound(const NodeId& id) {
  NodeEntrance node;
  node.id = id;
  NotifyHost(node, RoutingTableEventType::kNodeNotFound);
}

void RoutingTable::NotifyHost(const NodeEntrance& node, RoutingTableEventType event) {
  {
   Guard g(notify_mux_);
   ++notify_running_;
  }
  try {
    std::thread t([this, node, event]() {
                    try {
                      host_.HandleRoutTableEvent(node, event);
                    } catch (...) {}
                    Guard g(notify_mux_);
                    --notify_running_;
                    notify_cv_.notify_all();
                  });
    t.detach();
  } catch (...) {
    Guard g(notify_mux_);
    --notify_running_;
  }
}
} // namespace net
