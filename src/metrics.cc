#include "metrics.h"
#include "utils.h"
#include <arpa/inet.h>
#include <charconv>
#include <cstring>
#include <glog/logging.h>
#include <limits>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace metrics {

thread_local Sink sink{};

namespace {

struct Names {
  std::unordered_map<std::string, uint16_t, TransparentHash, TransparentEqual>
      index;
  std::vector<std::string> list{""}; // id 0 = unset / none

  uint16_t intern(std::string_view s, const char *kind) {
    if (auto it = index.find(s); it != index.end())
      return it->second;
    if (list.size() > std::numeric_limits<uint16_t>::max())
      LOG(FATAL) << "metrics: too many " << kind << "s";
    auto new_id = static_cast<uint16_t>(list.size());
    list.emplace_back(s);
    if (!index.emplace(list.back(), new_id).second)
      LOG(FATAL) << "metrics: duplicate " << kind << " " << s;
    return new_id;
  }
};

struct Owner {
  std::unique_ptr<Record[]> records;
  std::string label;
  Names names;
  Names methods;
};

thread_local Owner owner;

template <class T> void append_num(std::string &out, T v) {
  char buf[32];
  auto res = std::to_chars(buf, buf + sizeof(buf), v);
  if (res.ec != std::errc())
    LOG(FATAL) << "metrics: to_chars failed";
  out.append(buf, res.ptr);
}

bool send_all(int fd, const char *p, size_t n) {
  while (n > 0) {
    ssize_t w = send(fd, p, n, MSG_NOSIGNAL);
    if (w < 0) {
      if (errno == EINTR)
        continue;
      LOG(ERROR) << "metrics: send failed: " << strerror(errno);
      return false;
    }
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

} // namespace

void init(std::string label, size_t capacity, bool prefault) {
  // new[] of a trivial type leaves memory untouched; prefault writes it once
  owner = Owner{};
  owner.records.reset(new Record[capacity]);
  if (prefault)
    std::memset(static_cast<void *>(owner.records.get()), 0,
                capacity * sizeof(Record));
  owner.label = std::move(label);
  sink = Sink{owner.records.get(), 0, capacity};
}

uint16_t id(std::string_view name) {
  if (name.empty())
    LOG(FATAL) << "metrics: empty metric name";
  return owner.names.intern(name, "name");
}

uint16_t method(std::string_view m) { return owner.methods.intern(m, "method"); }

void fatal_full(uint16_t metric_id) {
  if (sink.capacity == 0)
    LOG(FATAL) << "metrics: record before init (metric id " << metric_id
               << ")";
  const auto &list = owner.names.list;
  LOG(FATAL) << "metrics: buffer full (" << sink.capacity
             << " records) on metric "
             << (metric_id < list.size() ? list[metric_id] : "?")
             << "; raise metrics::kCapacity";
  __builtin_unreachable();
}

void dump(std::string &out) {
  out.reserve(out.size() + 64 + sink.size * 32);
  out += "# ";
  out += owner.label;
  out += '\n';
  const auto &names = owner.names.list;
  for (size_t i = 1; i < names.size(); i++) {
    out += "# name ";
    append_num(out, i);
    out += ' ';
    out += names[i];
    out += '\n';
  }
  const auto &methods = owner.methods.list;
  for (size_t i = 1; i < methods.size(); i++) {
    out += "# method ";
    append_num(out, i);
    out += ' ';
    out += methods[i];
    out += '\n';
  }
  for (size_t i = 0; i < sink.size; i++) {
    const Record &r = sink.data[i];
    append_num(out, r.ts_us);
    out += ' ';
    append_num(out, r.id);
    out += ' ';
    append_num(out, r.method);
    out += ' ';
    append_num(out, r.value);
    out += '\n';
  }
}

int listen(uint16_t port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    LOG(FATAL) << "metrics: socket failed: " << strerror(errno);
  int enable = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0)
    LOG(FATAL) << "metrics: SO_REUSEADDR failed: " << strerror(errno);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    LOG(FATAL) << "metrics: bind " << port << " failed: " << strerror(errno);
  if (::listen(fd, 8) < 0)
    LOG(FATAL) << "metrics: listen failed: " << strerror(errno);
  return fd;
}

void serve(int client_fd) {
  // Read the request first: closing with unread input sends RST, and the
  // client can lose the tail of the body.
  // a client that sends nothing must not hang the event loop
  timeval tv{2, 0};
  setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  std::string req;
  char buf[1024];
  while (req.find("\r\n\r\n") == std::string::npos && req.size() < 8192) {
    ssize_t r = recv(client_fd, buf, sizeof(buf), 0);
    if (r < 0 && errno == EINTR)
      continue;
    if (r <= 0)
      break;
    req.append(buf, static_cast<size_t>(r));
  }

  // Content-Length lets wget fail on a truncated body
  std::string body;
  dump(body);
  std::string out = "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n"
                    "Content-Length: " +
                    std::to_string(body.size()) +
                    "\r\nConnection: close\r\n\r\n";
  out += body;
  if (send_all(client_fd, out.data(), out.size()))
    shutdown(client_fd, SHUT_WR);
  close(client_fd);
  LOG(INFO) << "metrics: served " << sink.size << " records";
}

} // namespace metrics
