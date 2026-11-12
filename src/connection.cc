#include "connection.h"

#include <atomic>
#include <chrono>
#include <iterator>
#include <string>
#include <unordered_map>

#include "utils/log.h"

namespace net {

namespace {

// Log of the headers announcing more than kMaxPacketSize: at most one line a minute per remote
// address, with the number of such headers since the previous line from that address.
class OversizedHeaderLog {
 public:
  void Report(const bi::address& addr, size_t announced, size_t limit) {
    using Clock = std::chrono::steady_clock;
    const auto now = Clock::now();
    const uint64_t total = ++total_;
    uint64_t count = 0;
    {
      Guard g(mux_);
      if (peers_.size() >= kMaxAddresses) {
        for (auto it = peers_.begin(); it != peers_.end();) {
          it = now - it->second.last >= kInterval ? peers_.erase(it) : std::next(it);
        }
        if (peers_.size() >= kMaxAddresses) peers_.clear();
      }
      // one line a minute per address and limit (registration or later packet)
      auto res = peers_.try_emplace(addr.to_string() + "/" + std::to_string(limit));
      auto& entry = res.first->second;
      ++entry.count;
      if (!res.second && now - entry.last < kInterval) return;
      count = entry.count;
      entry.count = 0;
      entry.last = now;
    }
    LOG(ERROR) << "Packet header from " << addr << " announces " << announced
               << " bytes, above the limit of " << limit
               << ": connection dropped (" << count << " from this address since the last report, "
               << total << " in total)";
  }

 private:
  static constexpr std::chrono::seconds kInterval{60};
  static constexpr size_t kMaxAddresses = 4096;

  struct Entry {
    std::chrono::steady_clock::time_point last{};
    uint64_t count = 0;
  };

  std::atomic<uint64_t> total_{0};
  Mutex mux_;
  std::unordered_map<std::string, Entry> peers_;
};

OversizedHeaderLog& GetOversizedHeaderLog() {
  static OversizedHeaderLog log;
  return log;
}

} // namespace

Connection::Connection(ConnectionOwner& h, ba::io_context& io)
    : host_(h),
      socket_(io),
      active_(true),
      deadline_(io) {}

Connection::Connection(ConnectionOwner& h, ba::io_context& io, bi::tcp::socket&& s)
    : host_(h),
      socket_(std::move(s)),
      active_(false),
      deadline_(io) {}

void Connection::ResetTimer() {
  Ptr self(shared_from_this());
  auto callback = [this, self](const boost::system::error_code& e) {
                    if (e == ba::error::operation_aborted) {
                      return;
                    }

                    Drop(DropReason::kTimeout);
                  };

  size_t waiters = deadline_.expires_from_now(boost::posix_time::seconds(kTimeoutSeconds));

  if (!timer_started_) {
    timer_started_ = true;
    deadline_.async_wait(std::move(callback));
    return;
  }

  if (waiters > 0) {
    deadline_.async_wait(std::move(callback));
  }
}

void Connection::Close() {
  try {
    boost::system::error_code ec;
    socket_.shutdown(bi::tcp::socket::shutdown_both, ec);
    deadline_.cancel(ec);
    if (socket_.is_open()) socket_.close();
  } catch (...) {}
}

void Connection::Drop(DropReason reason) {
  if (dropped_) return;
  dropped_ = true;

  if (registation_passed_) {
    host_.OnConnectionDropped(remote_node_, active_, reason);
  } else if (active_) {
    host_.OnPendingConnectionError(remote_node_, reason);
  } else {
    LOG(DEBUG) << "Drop passive connection registration passed: " << registation_passed_;
  }

  Close();
}

void Connection::StartRead() {
  Ptr self(shared_from_this());
  packet_.data.resize(Packet::Header::size);
  ResetTimer();

  ba::async_read(socket_, ba::buffer(packet_.data, Packet::Header::size),
          [this, self](const boost::system::error_code& er, size_t len) {
            if (dropped_) {
              return;
            }
            ResetTimer();

            if (!CheckRead(er, Packet::Header::size, len)) {
              LOG(DEBUG) << "Header check read failded.";
              Drop(kReadError);
              return;
            }

            Unserializer u(packet_.data.data(), Packet::Header::size);
            if (!packet_.GetHeader(u)) {
              LOG(DEBUG) << "Invalid header received.";
              Drop(kProtocolCorrupted);
              return;
            }

            // Before registration only a registration packet (RegData: 7 or 19 bytes) is accepted;
            // after it, at most kMaxPacketSize. The data is read in chunks (ReadData), so the memory
            // held follows the bytes actually received, not the size announced.
            const bool registration_expected = !registation_passed_;
            const size_t limit = registration_expected ? kMaxRegistrationSize : kMaxPacketSize;
            if (registration_expected && !packet_.IsRegistration()) {
              Drop(kProtocolCorrupted);
              return;
            }
            if (packet_.header.data_size > limit) {
              boost::system::error_code ec;
              auto ep = socket_.remote_endpoint(ec);
              GetOversizedHeaderLog().Report(ec ? bi::address() : ep.address(),
                                             packet_.header.data_size, limit);
              Drop(kProtocolCorrupted);
              return;
            }

            packet_.data.clear();
            ReadData();
         }
  );
}

void Connection::ReadData() {
  const size_t total = packet_.header.data_size;
  const size_t received = packet_.data.size();
  if (received == total) {
    OnPacketRead();
    return;
  }

  const size_t chunk = std::min(total - received, kReadChunkSize);
  try {
    if (packet_.data.capacity() < received + chunk) {
      // geometric growth, never above the announced size
      packet_.data.reserve(std::min(total, std::max(received + chunk, 2 * packet_.data.capacity())));
    }
    packet_.data.resize(received + chunk);
  } catch (...) {
    LOG(DEBUG) << "Cannot allocate packet data.";
    Drop(kProtocolCorrupted);
    return;
  }

  Ptr self(shared_from_this());
  ba::async_read(socket_, ba::buffer(packet_.data.data() + received, chunk),
          [this, self, chunk](const boost::system::error_code& er, size_t len) {
            if (dropped_) {
              return;
            }
            ResetTimer();

            if (!CheckRead(er, chunk, len)) {
              LOG(DEBUG) << "Packet data check read failed.";
              Drop(kReadError);
              return;
            }

            ReadData();
          }
  );
}

void Connection::OnPacketRead() {
  Ptr self(shared_from_this());
  bool is_reg = packet_.IsRegistration();

  if (!registation_passed_) {
    if (!is_reg) {
      Drop(kProtocolCorrupted);
      return;
    } else {
      registation_passed_ = true;

      if (!active_) {
        remote_node_ = packet_.header.sender;
      }

      host_.OnConnected(std::move(packet_), self);
      packet_ = Packet();
      StartRead();
      return;
    }
  }

  if (is_reg) {
    LOG(DEBUG) << "Reg packet recieved, when registartion passed.";
    Drop(kProtocolCorrupted);
    return;
  }

  host_.OnPacketReceived(std::move(packet_));
  packet_ = Packet();
  StartRead();
}

bool Connection::CheckRead(const boost::system::error_code& er, size_t expected, size_t len) {
  if (er && er.category() != ba::error::get_misc_category() && er.value() != ba::error::eof) {
    LOG(DEBUG) << "Error reading " << er.value() << ", " << er.message();
    return false;
  }

  if (er && len < expected) {
    LOG(DEBUG) << "Error reading " << er.value() << ", " << er.message()
               << ", length " << len;
    return false;
  }

  if (len != expected) {
    LOG(ERROR) << "Wrong packet length: expected " << expected
               << ", real " << len;
    return false;
  }

  return true;
}

bool Connection::IsConnected() const {
  return (socket_.is_open() && !socket_.remote_endpoint().address().is_unspecified());
}

void Connection::Send(Packet&& pack) {
  Guard g(send_mux_);
  Serializer s;
  s.Put(pack);
  send_queue_.push_back(s.GetData());

  if (send_queue_.size() == 1) {
    StartWrite();
  }
}

void Connection::StartWrite() {
  Ptr self(shared_from_this());
  ba::async_write(socket_, ba::buffer(send_queue_[0]),
      [this, self](const boost::system::error_code& err, size_t /* written length */) {
        if (dropped_) {
          return;
        }
        ResetTimer();

        if (err) {
          LOG(DEBUG) << "Cannot send packet, reason " << err.value()
                     << ", " << err.message();
          Drop(kWriteError);
          return;
        }

        Guard g(send_mux_);
        send_queue_.pop_front();

        if (send_queue_.empty()) return;

        StartWrite();
      }
  );
}

void Connection::Connect(const Endpoint& ep, Packet&& reg_pack) {
  remote_node_ = reg_pack.header.receiver;

  Ptr self(shared_from_this());
  {
   Guard g(send_mux_);
   Serializer s;
   s.Put(reg_pack);
   send_queue_.push_back(s.GetData());
  }

  ResetTimer();
  socket_.async_connect(ep, [this, self](const boost::system::error_code& err) {
                              if (dropped_) {
                                return;
                              }
                              ResetTimer();

                              if (err) {
                                LOG(DEBUG) << "Cannot connect to peer, reason " << err.value()
                                           << ", " << err.message();
                                Drop(kConnectionError);
                                return;
                              }

                              StartWrite();
                              StartRead();
                            }
  );
}

std::string Connection::DropReasonToString(DropReason reason) {
  switch (reason) {
    case kTimeout: return "Connection timeout.";
    case kReadError: return "Read error.";
    case kWriteError: return "Write error.";
    case kProtocolCorrupted: return "Connection protocol was corrupted by remote node.";
    case kConnectionError: return "Cannot connect to remote node.";
  }
  return "Unknown error";
}
} // namespace net
