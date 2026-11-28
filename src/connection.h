#ifndef NET_CONNECTION_H
#define NET_CONNECTION_H

#include <deque>
#include <memory>
#include <string>

#include "common.h"

namespace net {

class ConnectionOwner;

class Connection : public std::enable_shared_from_this<Connection> {
 public:
  using Ptr = std::shared_ptr<Connection>;
  using Endpoint = bi::tcp::endpoint;

  enum DropReason : uint8_t {
    kTimeout,
    kReadError,
    kWriteError,
    kProtocolCorrupted,
    kConnectionError
  };

  static std::string DropReasonToString(DropReason);

  static auto Create(ConnectionOwner& h, ba::io_context& io) {
    return Ptr(new Connection(h, io));
  }

  static auto Create(ConnectionOwner& h, ba::io_context& io, bi::tcp::socket&& s) {
    return Ptr(new Connection(h, io, std::move(s)));
  }

  void Connect(const Endpoint&, Packet&& reg_pack);

  ~Connection() { Close(); }
  void Close();
  // closes without reporting anything to the owner: its handlers end at once (Host::Reset, for the
  // dials still pending, which must not mark their peers unreachable). Safe from any thread: the socket
  // is closed on the io thread, and a buffer an async_write in flight uses is freed only by that
  // write's handler
  void Abandon();

  void Send(Packet&&);
  void StartRead();

  bool IsActive() const noexcept { return active_; }
  bool IsConnected() const;
  // dropped or abandoned: it sends nothing more and its owner must not take it
  bool IsDropped() const noexcept { return dropped_; }
  // a registered passive connection its owner did not take (a dial meant for another node, see
  // Host::RefuseMisdirected): its end is reported to nobody
  void MarkNotTaken() noexcept { not_taken_ = true; }
  bool IsNotTaken() const noexcept { return not_taken_; }
  // the node this connection was dialled to (active), or the registered sender (passive)
  const NodeId& RemoteNode() const noexcept { return remote_node_; }

  Endpoint GetEndpoint() const { return socket_.remote_endpoint(); }

 private:
  constexpr static uint16_t kTimeoutSeconds = 10;
  // packet data is read (and its buffer grown) at most this much at a time
  constexpr static size_t kReadChunkSize = 1024 * 1024;

  // active connection
  Connection(ConnectionOwner&, ba::io_context&);
  // passive connection
  Connection(ConnectionOwner&, ba::io_context&, bi::tcp::socket&&);

  void StartWrite();
  void ClearSendQueue();
  void ReadData();
  void OnPacketRead();
  bool CheckRead(const boost::system::error_code&, size_t expected, size_t len);
  void Drop(DropReason);

  void ResetTimer();

  ConnectionOwner& host_;
  bi::tcp::socket socket_;
  Packet packet_;

  Mutex send_mux_;
  std::deque<ByteVector> send_queue_;

  std::atomic<bool> registation_passed_ = false;
  std::atomic<bool> dropped_ = false;
  std::atomic<bool> timer_started_ = false;
  std::atomic<bool> not_taken_ = false;
  const bool active_;

  NodeId remote_node_;
  ba::deadline_timer deadline_;
};

class ConnectionOwner {
 public:
  virtual void OnPacketReceived(Packet&&) = 0;
  virtual void OnConnected(Packet&& conn_pack, Connection::Ptr) = 0;
  // the connection that dropped: only that one leaves the owner's table (a newer connection with the
  // same peer and direction stays)
  virtual void OnConnectionDropped(const NodeId& remote_node, bool active, Connection::DropReason,
                                   const Connection* conn) = 0;
  virtual void OnPendingConnectionError(const NodeId&, Connection::DropReason) = 0;
};

} // namespace net
#endif // NET_CONNECTION_H
