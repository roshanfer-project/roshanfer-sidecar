#include "sidecar_ready.h"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

void write_sidecar_ready_file() {
  const char *skip = std::getenv("SIDECAR_SKIP_READY_FILE");
  if (skip != nullptr && std::strcmp(skip, "1") == 0) {
    return;
  }
  int fd = open("/ready", O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    LOG(WARNING) << "sidecar ready: open /ready failed: " << strerror(errno);
    return;
  }
  static const char msg[] = "ok\n";
  if (write(fd, msg, sizeof(msg) - 1) < 0) {
    LOG(WARNING) << "sidecar ready: write failed: " << strerror(errno);
  }
  close(fd);
}
