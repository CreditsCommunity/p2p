#include "host.h"

#include <cmath>

#include "utils/log.h"

namespace net {

Host::Host(const Config& config, HostEventHandler& event_handler)
    : io_(2),
      acceptor_(io_),
      event_handler_(event_handler),
      routing_table_(nullptr),
      packets_to_send_(0) {
  InitLogger();

  auto& net = Network::Instance();
  try {
    net.Init(config);
  } catch (const std::exception& e) {
    LOG(FATAL) << e.what();
    throw;
  } catch (...) {
    LOG(FATAL) << "Problems with network!";
    throw;
  }

  my_id_ = net.GetHostContacts().id;

  routing_table_ = std::make_shared<RoutingTable>(io_, static_cast<RoutingTableEventHandler&>(*this));

  ban_man_ = std::make_unique<BanMan>(kBanFileName, static_cast<BanManOwner&>(*this),
      routing_table_);

  self_check_timer_ = std::make_unique<DeadlineTimer>(io_);

  TcpListen();
}

Host::~Host() {
  io_.stop();

  if (working_thread_.joinable()) {
    working_thread_.join();
  }

  // No handler runs any more: the routing table's threads end and its socket closes, and the
  // Network singleton drops its reference (Run) so that the routing table goes with this host,
  // while io_ still exists, not at static destruction against a destroyed io_context.
  routing_table_->Stop();
  Network::Instance().SetRoutingTable(nullptr);
}

void Host::Run() {
  auto& net = Network::Instance();
  net.SetRoutingTable(routing_table_);

  auto& config = net.GetConfig();
  routing_table_->AddNodes(
          config.use_default_boot_nodes ?
          GetDefaultBootNodes() : config.custom_boot_nodes);

  ScheduleSelfCheck(kSelfCheckFirst_);

  working_thread_ = std::thread([this] { io_.run(); });
}

void Host::Ban(const NodeId& peer) {
  ban_man_->Ban(peer);
}

void Host::Unban(const NodeId& peer) {
  ban_man_->Unban(peer);
}

void Host::ClearBanList() {
  ban_man_->Clear();
}

void Host::GetBanList(std::set<BanEntry>& ret_container) const {
  ban_man_->GetBanned(ret_container);
}

void Host::OnIdBanned(const NodeId& peer) {
  DropConnections(peer);
  ClearSendQueue(peer);
  RemoveFromPendingConn(peer);
}

void Host::HandleRoutTableEvent(const NodeEntrance& node, RoutingTableEventType event) {
  switch (event) {
    case RoutingTableEventType::kNodeFound : {
      ban_man_->OnNodeFound(node);

      auto conn = IsConnected(node.id);
      if (!conn) {
        Connect(node);
      } else {
        CheckSendQueue(node.id, conn);
      }
      break;
    }

    case RoutingTableEventType::kNodeNotFound :
      ban_man_->OnNodeNotFound(node.id);
      ClearSendQueue(node.id);
      RemoveFromPendingConn(node.id);
      break;

    case RoutingTableEventType::kNodeAdded :
      LOG(DEBUG) << "ROUTING TABLE: add " << IdToBase58(node.id);
      RemoveFromUnreachable(node.id);
      event_handler_.OnNodeDiscovered(node.id);
      break;

    case RoutingTableEventType::kNodeRemoved :
      LOG(DEBUG) << "ROUTING TABLE: remove " << IdToBase58(node.id);
      event_handler_.OnNodeRemoved(node.id);
      break;

    case RoutingTableEventType::kOwnEndpointFound :
      ProbeOwnEndpoint(node);
      break;
  }
}

bool Host::IsEndpointBanned(const bi::address& addr, uint16_t port) {
  return ban_man_->IsBanned(BanEntry{addr, port});
}

void Host::OnPacketReceived(Packet&& packet) {
  if (packet.IsDirect() && packet.header.receiver == my_id_) {
    event_handler_.OnMessageReceived(packet.header.sender, std::move(packet.data));
  } else if (packet.IsBroadcast() && !IsDuplicate(packet)) {
    auto nodes = routing_table_->GetBroadcastList(packet.header.receiver);
    packet.header.receiver = my_id_;
    for (const auto& n : nodes) {
      SendDirect(n, packet);
    }
    event_handler_.OnMessageReceived(packet.header.sender, std::move(packet.data));
  }
}

bool Host::IsDuplicate(const Packet& packet) {
  auto id = packet.GetId();
  Guard g(broadcast_id_mux_);
  if (broadcast_ids_.find(id) == broadcast_ids_.end()) {
    InsertNewBroadcastId(id);
    return false;
  }
  return true;
}

void Host::InsertNewBroadcast(const Packet& packet) {
  auto id = packet.GetId();
  Guard g(broadcast_id_mux_);
  InsertNewBroadcastId(id);
}

void Host::InsertNewBroadcastId(const Packet::Id& id) {
  if (broadcast_ids_.size() == kMaxBroadcastIds_) {
    broadcast_ids_.erase(broadcast_ids_.begin());
  }
  broadcast_ids_.insert(id);
}

void Host::SendDirect(const NodeId& receiver, ByteVector&& data) {
  if (receiver == my_id_) {
    return;
  }

  auto pack = FormPacket(Packet::Type::kDirect, std::move(data), receiver);

  NodeEntrance receiver_contacts;
  if (routing_table_->HasNode(receiver, receiver_contacts)) {
    SendPacket(receiver_contacts, std::move(pack));
  } else {
    AddToSendQueue(receiver, std::move(pack));
    routing_table_->StartFindNode(receiver);
  }
}

void Host::SendDirect(const NodeEntrance& receiver, const Packet& packet) {
  auto copy = packet;
  SendPacket(receiver, std::move(copy));
}

void Host::SendBroadcast(ByteVector&& data) {
  auto pack = FormPacket(Packet::Type::kBroadcast, std::move(data), my_id_);
  InsertNewBroadcast(pack);
  auto nodes = routing_table_->GetBroadcastList(my_id_);
  for (const auto& n : nodes) {
    SendDirect(n, pack);
  }
}

void Host::SendBroadcastIfNoConnection(const NodeId& receiver, ByteVector&& data) {
  if (receiver == my_id_) {
    return;
  }

  auto conn = IsConnected(receiver);
  if (conn) {
    conn->Send(FormPacket(Packet::Type::kDirect, std::move(data), receiver));
    return;
  }

  SendBroadcast(std::move(data));

  NodeEntrance receiver_contacts;
  if (!routing_table_->HasNode(receiver, receiver_contacts)) {
    routing_table_->StartFindNode(receiver);
  } else {
    Connect(receiver_contacts);
  }
}

Packet Host::FormPacket(Packet::Type type, ByteVector&& data, const NodeId& receiver) {
  Packet result;
  result.data = std::move(data);

  auto& h = result.header;
  h.type = type;
  h.data_size = result.data.size();
  h.sender = my_id_;
  h.receiver = receiver;

  return result;
}

void Host::SendPacket(const NodeEntrance& receiver, Packet&& pack) {
  auto conn = IsConnected(receiver.id);
  if (conn) {
    conn->Send(std::move(pack));
    return;
  }

  AddToSendQueue(receiver.id, std::move(pack));
  Connect(receiver);
}

Connection::Ptr Host::IsConnected(const NodeId& peer) {
  Guard g(conn_mux_);
  auto it = connections_.find(peer);
  if (it != connections_.end()) {
    return it->second;
  }
  return nullptr;
}

void Host::Connect(const NodeEntrance& peer) {
  if (IsEndpointBanned(peer.address, peer.tcp_port)) {
    ClearSendQueue(peer.id);
    RemoveFromPendingConn(peer.id);
    return;
  }

  if (IsUnreachable(peer.id)) {
    ClearSendQueue(peer.id);
    return;
  }

  if (HasPendingConnection(peer.id)) {
    return;
  }

  auto new_conn = Connection::Create(static_cast<ConnectionOwner&>(*this), io_);
  {
    Guard g(pend_conn_mux_);
    pending_dials_[peer.id] = new_conn;
  }
  new_conn->Connect(Connection::Endpoint(peer.address, peer.tcp_port), FormRegistration(peer.id));
}

Packet Host::FormRegistration(const NodeId& receiver) {
  auto reg = FormPacket(Packet::Type::kRegistration, Network::Instance().GetRegistrationData(), receiver);
  reg.header.reserved |= kRegAnswerAnyway;
  return reg;
}

namespace {
std::string EndpointText(const Connection::Ptr& conn) {
  // GetEndpoint throws once the socket is closed
  try {
    auto ep = conn->GetEndpoint();
    return ep.address().to_string() + ":" + std::to_string(ep.port());
  } catch (...) {
    return "?";
  }
}
} // namespace

bool Host::RefuseMisdirected(const Packet& reg, const Connection::Ptr& conn) {
  const NodeId& meant = reg.header.receiver;
  const NodeId& from = reg.header.sender;
  // a registration names the node dialled (a zero id: none, taken as for this node)
  const bool for_another = meant != my_id_ && meant != NodeId();
  const bool from_self = from == my_id_;
  if (!for_another && !from_self) {
    return false;
  }

  conn->MarkNotTaken();
  if (for_another && !from_self) {
    WarnAnnouncedHere(meant, "a peer dialled it and reached this node: its connection is refused");
  }
  if (reg.header.reserved & kRegAnswerAnyway) {
    // the dialler drops this answer (it names another node than the one dialled) and logs who answered
    conn->Send(FormPacket(Packet::Type::kRegistration, Network::Instance().GetRegistrationData(), from));
    LOG(DEBUG) << "Connection from " << IdToBase58(from) << " meant for " << IdToBase58(meant)
               << ": answered, not taken";
  } else {
    conn->Abandon();
    LOG(DEBUG) << "Connection from " << IdToBase58(from) << " meant for " << IdToBase58(meant) << ": refused";
  }
  return true;
}

void Host::WarnAnnouncedHere(const NodeId& other, const char* how) {
  // once per id every kWarnInterval_, and at most kMaxWarnLines_ lines in such a window whatever the
  // ids: a peer that dials with random receiver ids gets a count, not a line per connection
  uint64_t suppressed = 0;
  {
    Guard g(self_check_mux_);
    const auto now = std::chrono::steady_clock::now();
    auto it = warned_announced_.find(other);
    if (it != warned_announced_.end() && now - it->second < kWarnInterval_) {
      return;
    }
    if (now - warn_window_start_ >= kWarnInterval_) {
      warn_window_start_ = now;
      warn_lines_ = 0;
    }
    if (warn_lines_ >= kMaxWarnLines_) {
      ++warn_suppressed_;
      return;
    }
    ++warn_lines_;
    suppressed = warn_suppressed_;
    warn_suppressed_ = 0;
    if (warned_announced_.size() >= kMaxWarned_) {
      warned_announced_.clear();
    }
    warned_announced_[other] = now;
  }
  // p2p WARNING lines go nowhere (utils/log.cc): INFO reaches the console and debug.log
  LOG(INFO) << "WARNING: another node, " << IdToBase58(other) << ", is announced at this node's address ("
            << how << "). Two nodes behind one public address need different p2p ports, each forwarded "
            << "to its own node: the other node's operator must change its port"
            << (suppressed ? " (and " + std::to_string(suppressed) + " more such connection(s) not logged)" : std::string());
}

void Host::ScheduleSelfCheck(std::chrono::seconds after) {
  self_check_timer_->expires_from_now(boost::posix_time::seconds(after.count()));
  self_check_timer_->async_wait([this](const boost::system::error_code& e) {
                                  if (e == ba::error::operation_aborted) {
                                    return;
                                  }
                                  // the previous check: every dial of its own address failed, none answered
                                  bool hint = false;
                                  {
                                    Guard g(self_check_mux_);
                                    hint = probe_failed_ && !probe_answered_;
                                    probe_failed_ = probe_answered_ = false;
                                  }
                                  if (hint) {
                                    LOG(INFO) << "Hint: nobody answered at the address the peers record for this "
                                              << "node. If it should accept incoming connections, check that its p2p "
                                              << "port is forwarded to it (some routers do not loop a connection to their "
                                              << "own public address back: then this hint is moot); a node without a "
                                              << "forwarded port still "
                                              << "works through the connections it opens itself";
                                  }
                                  routing_table_->StartFindNode(my_id_);
                                  ScheduleSelfCheck(kSelfCheckInterval_);
                                });
}

void Host::ProbeOwnEndpoint(const NodeEntrance& seen) {
  if (seen.address.is_unspecified()) {
    return;
  }
  // an official peer records the source port of this node's own connection (UpdateTcpPort), where
  // nothing listens: the address it saw with the port this node announces is where its peers dial it
  const uint16_t announced = Network::Instance().GetHostContacts().tcp_port;
  ProbeEndpoint(seen.address, announced);
  if (seen.tcp_port != announced) {
    ProbeEndpoint(seen.address, seen.tcp_port);
  }
}

void Host::ProbeEndpoint(const bi::address& address, uint16_t tcp_port) {
  if (tcp_port == 0 || IsEndpointBanned(address, tcp_port)) {
    return;
  }
  const std::string key = address.to_string() + ":" + std::to_string(tcp_port);
  {
    Guard g(self_check_mux_);
    const auto now = std::chrono::steady_clock::now();
    auto it = probed_endpoints_.find(key);
    if (it != probed_endpoints_.end() && now - it->second < kSelfCheckInterval_ - std::chrono::seconds(30)) {
      return;
    }
    if (probed_endpoints_.size() >= kMaxWarned_) {
      probed_endpoints_.clear();
    }
    probed_endpoints_[key] = now;
  }
  LOG(DEBUG) << "Checking the address a peer records for this node: " << key;
  // not a pending dial: OnConnected recognises it by the id dialled (this node's own)
  auto conn = Connection::Create(static_cast<ConnectionOwner&>(*this), io_);
  conn->Connect(Connection::Endpoint(address, tcp_port), FormRegistration(my_id_));
}

void Host::OnOwnEndpointAnswered(const NodeId& answered_by, const Connection::Ptr& conn) {
  const std::string ep = EndpointText(conn);
  bool first_answer = false;
  {
    Guard g(self_check_mux_);
    probe_answered_ = true;
    if (answered_by == my_id_ && !reachable_reported_) {
      reachable_reported_ = first_answer = true;
    }
  }
  if (answered_by != my_id_) {
    LOG(ERROR) << "Another node, " << IdToBase58(answered_by) << ", answers at this node's address " << ep
               << ": the peers record this node there and their connections reach that node instead. Two "
               << "nodes behind one public address need different p2p ports, each forwarded to its own "
               << "node: give this node another port ([host_input] port) and forward it to this node "
               << "(checked again every 10 minutes: after a change of address one such line can be stale)";
  } else if (first_answer) {
    LOG(INFO) << "This node answers at the address its peers record for it: " << ep;
  }
}

void Host::AddKnownNodes(const std::vector<NodeEntrance>& nodes) {
  routing_table_->AddNodes(nodes);
}

void Host::GetKnownNodes(std::vector<NodeEntrance>& nodes) {
  routing_table_->GetKnownNodes(nodes);
}

void Host::GetRoutingTableNodes(std::vector<NodeEntrance>& nodes) {
  routing_table_->GetBucketNodes(nodes);
}

void Host::GetLostNodes(std::vector<NodeEntrance>& nodes) {
  routing_table_->GetLostNodes(nodes);
}

bool Host::SendDirectIfKnown(const NodeId& receiver, ByteVector&& data) {
  if (receiver == my_id_) {
    return false;
  }
  NodeEntrance receiver_contacts;
  if (!routing_table_->HasNode(receiver, receiver_contacts)) {
    return false;
  }
  SendPacket(receiver_contacts, FormPacket(Packet::Type::kDirect, std::move(data), receiver));
  return true;
}

void Host::Reset() {
  auto& config = Network::Instance().GetConfig();
  std::vector<NodeEntrance> nodes = config.use_default_boot_nodes ?
                                    GetDefaultBootNodes() : config.custom_boot_nodes;
  std::vector<NodeEntrance> held;
  routing_table_->GetKnownNodes(held);
  nodes.insert(nodes.end(), held.begin(), held.end());

  size_t closed = 0;
  {
    // conn_mux_ is held throughout (then pend_conn_mux_, the order OnConnected takes them in): a dial
    // that registers meanwhile either is in connections_ already and is closed here, or finds itself
    // abandoned in OnConnected and is never taken
    Guard g(conn_mux_);
    // their handlers end with an error and report the drop, which finds nothing left to erase; the
    // sockets are closed on the io thread, like every other operation on them
    for (auto& c : connections_) {
      ba::post(io_, [conn = c.second]() { conn->Close(); });
      ++closed;
    }
    connections_.clear();

    std::unordered_map<NodeId, Connection::Ptr> dials;
    {
      Guard p(pend_conn_mux_);
      pending_connections_.clear();
      dials.swap(pending_dials_);
    }
    // the dials in flight end without a word: reported, a dial cut here would mark its peer
    // unreachable for 120 s, right when the routing table looks it up again
    for (auto& d : dials) {
      d.second->Abandon();
    }
  }
  {
    Guard g(send_mux_);
    send_queue_.clear();
    packets_to_send_ = 0;
  }
  {
    Guard g(unreachable_mux_);
    unreachable_peers_.clear();
  }

  LOG(INFO) << "Reset: " << closed << " connection(s) closed, routing table of " << held.size()
            << " node(s) looked up again";
  routing_table_->Reset(nodes);
}

void Host::TcpListen() {
  auto& contacts = Network::Instance().GetHostContacts();

  try {
    bi::tcp::endpoint ep(contacts.address, contacts.tcp_port);
    acceptor_.open(ep.protocol());
    acceptor_.set_option(ba::socket_base::reuse_address(true));
    acceptor_.bind(ep);
    acceptor_.listen();
  } catch (...) {
    LOG(ERROR) << "Could not start listening on port " << contacts.tcp_port << ".";
    return;
  }

  if (acceptor_.is_open()) {
    LOG(INFO) << "Start listen on port " << contacts.tcp_port;
    StartAccept();
  }
}

void Host::StartAccept() {
  if (acceptor_.is_open()) {
    acceptor_.async_accept([this](const boost::system::error_code& err, bi::tcp::socket sock) {
                             if (!acceptor_.is_open()) {
                               LOG(INFO) << "Cannot accept new connection, acceptor is not open.";
                               return;
                             }

                             if (err) {
                               LOG(INFO) << "Cannot accept new connection: "
                                          << err.value() << ", " << err.message();
                               return;
                             }

                             boost::system::error_code sock_err;
                             auto ep = sock.remote_endpoint(sock_err);

                             if (sock_err) {
                                LOG(DEBUG) << "Cannot accept new connection, no access to remoted endpoint: "
                                           << sock_err.value() << ", " << sock_err.message();
                                StartAccept();
                                return;
                             }

                             if (IsEndpointBanned(ep.address(), ep.port())) {
                               LOG(INFO) << "Block connection from banned endpoint " << ep;
                               StartAccept();
                               return;
                             }

                             auto new_conn = Connection::Create(static_cast<ConnectionOwner&>(*this), io_, std::move(sock));
                             new_conn->StartRead();

                             StartAccept();
                           }
    );
  }
}

void Host::OnConnected(Packet&& conn_pack, Connection::Ptr new_conn) {
  const auto& remote_node = conn_pack.header.sender;

  // this node's probe of its own address (ProbeOwnEndpoint): never taken. Answered by an official
  // node, that node takes the probe under this node's id and, once it is closed here, its
  // OnConnectionDropped erases its passive connections with this node: once every 10 minutes, and only
  // while the misconfiguration this reports lasts (accepted)
  if (new_conn->IsActive() && new_conn->RemoteNode() == my_id_) {
    OnOwnEndpointAnswered(remote_node, new_conn);
    new_conn->Abandon();
    return;
  }
  // a dial meant for another node (two nodes announce this node's address): never taken, and the
  // connections with that peer are left alone
  if (!new_conn->IsActive() && RefuseMisdirected(conn_pack, new_conn)) {
    return;
  }

  Guard g(conn_mux_);
  // abandoned by Reset (which holds conn_mux_ while it abandons the dials): never taken
  if (new_conn->IsDropped()) {
    LOG(DEBUG) << "Registration of an abandoned connection with " << IdToBase58(remote_node) << " ignored";
    return;
  }

  // a dial answered by another node than the one dialled: not taken under the other id (the dialled
  // node is not there; its pending dial ends as a failed one)
  if (new_conn->IsActive() && remote_node != new_conn->RemoteNode()) {
    const NodeId dialled = new_conn->RemoteNode();
    LOG(DEBUG) << "Dial to " << IdToBase58(dialled) << " answered by " << IdToBase58(remote_node) << ": dropped";
    if (remote_node == my_id_) {
      WarnAnnouncedHere(dialled, "this node dialled it and reached itself");
    }
    new_conn->Abandon();
    AddToUnreachable(dialled);
    ClearSendQueue(dialled);
    RemoveFromPendingConn(dialled);
    return;
  }

  connections_.insert(std::make_pair(remote_node, new_conn));

  if (!new_conn->IsActive()) {
    new_conn->Send(FormPacket(Packet::Type::kRegistration,
                              Network::Instance().GetRegistrationData(),
                              remote_node));
    LOG(DEBUG) << "New passive connection with " << IdToBase58(remote_node);
  } else {
    LOG(DEBUG) << "New active connection with " << IdToBase58(remote_node);
    RemoveFromPendingConn(new_conn->RemoteNode());
  }

  CheckSendQueue(remote_node, new_conn);
  Network::Instance().OnConnected(std::move(conn_pack), new_conn);
}

void Host::OnConnectionDropped(const NodeId& remote_node, bool active,
                               Connection::DropReason drop_reason, const Connection* conn) {
  // a dial meant for another node, answered but never taken (RefuseMisdirected)
  if (conn && conn->IsNotTaken()) {
    return;
  }
  Guard g(conn_mux_);
  auto range = connections_.equal_range(remote_node);
  for (auto it = range.first; it != range.second;) {
    // only the connection that dropped: erasing every connection of that direction also took out a
    // newer one to the same peer, which then stayed open but unused, and the peer looked unconnected
    if (it->second.get() == conn) {
      it = connections_.erase(it);
      LOG(DEBUG) << "Connection with " << IdToBase58(remote_node)
                << " was closed, active: " << active << ". Reason: "
                << Connection::DropReasonToString(drop_reason);
    } else {
      ++it;
    }
  }

  if (!connections_.count(remote_node)) {
    ClearSendQueue(remote_node);
  }
  Network::Instance().OnConnectionDropped(remote_node, active);
}

void Host::AddToSendQueue(const NodeId& id, Packet&& pack) {
   Guard g(send_mux_);

   if (packets_to_send_ == kMaxSendQueueSize_) {
     auto it = send_queue_.begin();
     packets_to_send_ -= it->second.size();
     send_queue_.erase(it);
   }

   send_queue_[id].emplace_back(std::move(pack));
   ++packets_to_send_;
}

void Host::ClearSendQueue(const NodeId& id) {
  Guard g(send_mux_);
  auto it = send_queue_.find(id);
  if (it != send_queue_.end()) {
    packets_to_send_ -= it->second.size();
    send_queue_.erase(it);
  }
}

void Host::CheckSendQueue(const NodeId& id, Connection::Ptr conn) {
  Guard g(send_mux_);
  auto it = send_queue_.find(id);
  if (it != send_queue_.end()) {
    packets_to_send_ -= it->second.size();
    for (auto& p : it->second) {
      conn->Send(std::move(p));
    }
    send_queue_.erase(it);
  }
}

void Host::OnPendingConnectionError(const NodeId& id, Connection::DropReason drop_reason) {
  // a probe of this node's own address (ProbeOwnEndpoint) that nobody answered: not a dial, no
  // unreachable or pending bookkeeping; the next self-check gives a hint if no probe answered
  if (id == my_id_) {
    LOG(DEBUG) << "Nobody answered the probe of this node's own address, reason "
               << Connection::DropReasonToString(drop_reason);
    Guard g(self_check_mux_);
    probe_failed_ = true;
    return;
  }
  LOG(DEBUG) << "Pending connection with " << IdToBase58(id)
            << " was closed, reason " << Connection::DropReasonToString(drop_reason);

  if (drop_reason == Connection::DropReason::kConnectionError || drop_reason == Connection::DropReason::kTimeout) {
    AddToUnreachable(id);
  }

  ClearSendQueue(id);
  RemoveFromPendingConn(id);
}

void Host::RemoveFromPendingConn(const NodeId& id) {
  Guard g(pend_conn_mux_);
  pending_connections_.erase(id);
  pending_dials_.erase(id);
}

bool Host::HasPendingConnection(const NodeId& id) {
  Guard g(pend_conn_mux_);
  if (pending_connections_.find(id) != pending_connections_.end()) {
    return true;
  }
  pending_connections_.insert(id);
  return false;
}

void Host::DropConnections(const NodeId& id) {
  Guard g(conn_mux_);
  auto range = connections_.equal_range(id);
  for (auto it = range.first; it != range.second;) {
    ba::post(io_, [conn = it->second]() { conn->Close(); });
    it = connections_.erase(it);
    LOG(DEBUG) << "Manualy drop connection with " << IdToBase58(id);
  }
}

bool Host::IsUnreachable(const NodeId& peer) {
  Guard g(unreachable_mux_);

  using namespace std::chrono;
  auto now = steady_clock::now();

  for (auto it = unreachable_peers_.begin(); it != unreachable_peers_.end();) {
    auto seconds_unreachable = duration_cast<seconds>(now - it->second);

    if (seconds_unreachable >= kMaxSecondsInUnreachablePool_) {
      it = unreachable_peers_.erase(it);
    } else {
      ++it;
    }
  }

  return unreachable_peers_.find(peer) != unreachable_peers_.end();
}

void Host::AddToUnreachable(const NodeId& peer) {
  Guard g(unreachable_mux_);
  unreachable_peers_[peer] = std::chrono::steady_clock::now();
}

void Host::RemoveFromUnreachable(const NodeId& peer) {
  Guard g(unreachable_mux_);
  unreachable_peers_.erase(peer);
}
} // namespace net
