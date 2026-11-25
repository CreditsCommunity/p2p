#ifndef NET_HOST_H
#define NET_HOST_H

#include <chrono>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <vector>

#include "banman.h"
#include "common.h"
#include "connection.h"
#include "network.h"
#include "routing_table.h"

namespace net {

// Host's owner must implement this interface
class HostEventHandler {
 public:
  virtual ~HostEventHandler() = default;
  virtual void OnMessageReceived(const NodeId& from, ByteVector&& message) = 0;
  virtual void OnNodeDiscovered(const NodeId&) = 0;
  virtual void OnNodeRemoved(const NodeId&) = 0;
};

class Host : public RoutingTableEventHandler, public BanManOwner, public ConnectionOwner {
 public:
  Host(const Config&, HostEventHandler&);
  ~Host();

  void Run();

  void SendDirect(const NodeId& to, ByteVector&& msg);
  void SendBroadcast(ByteVector&& msg);
  void SendBroadcastIfNoConnection(const NodeId& to, ByteVector&& msg);

  void AddKnownNodes(const std::vector<NodeEntrance>&);
  void GetKnownNodes(std::vector<NodeEntrance>&);
  // the k-buckets only: nodes whose datagrams came from the endpoint recorded (never the explorer's
  // list of nodes other nodes named, which full discovery returns from GetKnownNodes)
  void GetRoutingTableNodes(std::vector<NodeEntrance>&);
  // a direct message to a node of the k-buckets, at the endpoint recorded; nothing (false) for any
  // other id: no queue, no lookup
  bool SendDirectIfKnown(const NodeId& to, ByteVector&& msg);
  // the peers the routing table lost because they stopped answering and still pings again (at most
  // 64 entries, up to 4 endpoints per id, each for 24 h at most), the most recently lost first: with
  // the k-bucket nodes, what to dial again after a restart
  void GetLostNodes(std::vector<NodeEntrance>&);

  // What a restart of the process rebuilds, without one: every connection is closed, the pending
  // connections, the send queues and the unreachable peers are forgotten, and the routing table is
  // emptied (the handler hears every node removed) and looked up again from the boot nodes and the
  // nodes it held, each of which must answer again to come back, with the endpoint it has now.
  // For a node that keeps missing what the network sends it after a network outage.
  // Pending dials are abandoned (closed without marking their peers unreachable). The pinger's timers
  // in flight find their pings forgotten and end, but one that fires after a new ping to the same node
  // takes it for its own: that node, or the replacer of an old ping, may be evicted or added one ping
  // early, and then answers or is evicted like any other.
  void Reset();

  void Ban(const NodeId&);
  void Unban(const NodeId&);
  void ClearBanList();
  void GetBanList(std::set<BanEntry>&) const;

 protected:
  // RoutingTableEventHandler
  void HandleRoutTableEvent(const NodeEntrance&, RoutingTableEventType) override;
  bool IsEndpointBanned(const bi::address& addr, uint16_t port) override;

  // BanManOwner
  void OnIdBanned(const NodeId&) override;
  void OnIdUnbanned(const NodeId&) override {}

  // ConnectionOwner
  void OnPacketReceived(Packet&&) override;
  void OnConnected(Packet&& conn_pack, Connection::Ptr) override;
  void OnConnectionDropped(const NodeId& remote_node, bool active, Connection::DropReason,
                           const Connection* conn) override;
  void OnPendingConnectionError(const NodeId&, Connection::DropReason) override;

 private:
  void TcpListen();
  void StartAccept();

  void SendDirect(const NodeEntrance&, const Packet&);
  bool IsDuplicate(const Packet&);
  void InsertNewBroadcast(const Packet&);
  void InsertNewBroadcastId(const Packet::Id& id); // doesn't lock broadcast_id_mux_

  Packet FormPacket(Packet::Type, ByteVector&&, const NodeId& receiver);
  void SendPacket(const NodeEntrance& receiver, Packet&&);

  void Connect(const NodeEntrance&);
  Connection::Ptr IsConnected(const NodeId&);

  void RemoveFromPendingConn(const NodeId&);
  bool HasPendingConnection(const NodeId&);

  void AddToSendQueue(const NodeId&, Packet&&);
  void ClearSendQueue(const NodeId&);
  void CheckSendQueue(const NodeId&, Connection::Ptr);
  void DropConnections(const NodeId&);

  bool IsUnreachable(const NodeId&);
  void AddToUnreachable(const NodeId&);
  void RemoveFromUnreachable(const NodeId&);

  ba::io_context io_;
  bi::tcp::acceptor acceptor_;
  HostEventHandler& event_handler_;
  NodeId my_id_;
  std::shared_ptr<RoutingTable> routing_table_;

  Mutex broadcast_id_mux_;
  constexpr static size_t kMaxBroadcastIds_ = 10000;
  std::unordered_set<Packet::Id> broadcast_ids_;

  Mutex send_mux_;
  constexpr static size_t kMaxSendQueueSize_ = 1000;
  size_t packets_to_send_;
  std::unordered_map<NodeId, std::vector<Packet>> send_queue_;

  std::thread working_thread_;

  Mutex conn_mux_;
  std::unordered_multimap<NodeId, Connection::Ptr> connections_;

  Mutex pend_conn_mux_;
  std::unordered_set<NodeId> pending_connections_;
  // the connections of those dials (Reset abandons them)
  std::unordered_map<NodeId, Connection::Ptr> pending_dials_;

  Mutex unreachable_mux_;
  constexpr static std::chrono::seconds kMaxSecondsInUnreachablePool_{120};
  std::unordered_map<NodeId, std::chrono::steady_clock::time_point> unreachable_peers_;

  std::unique_ptr<BanMan> ban_man_ = nullptr;
};
} // namespace net
#endif // NET_HOST_H
