#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <unistd.h>

// Global configuration
std::string deployment;
int appSize = 0;
int appPreRepeat = 0;
int appPostRepeat = 0;
int listenPort = 8080;

std::string get_env_var(const char *name, const std::string &default_val = "") {
  const char *val = std::getenv(name);
  return val ? val : default_val;
}

int str_to_int(const std::string &str, int default_val = 0) {
  if (str.empty())
    return default_val;
  try {
    return std::stoi(str);
  } catch (...) {
    return default_val;
  }
}

void busyLoop(int repeat) {
  for (int i = 0; i < repeat; ++i) {
    for (int j = 0; j < 10000; ++j) {
      // Empty loop to simulate work, similar to Go implementation
      asm(""); // Prevent optimization
    }
  }
}

std::string make_big_string(int size) {
  return std::string(static_cast<size_t>(size), 'a');
}

void handle_connection(int client_socket) {
  char buffer[1024];
  // Simple read consuming request (ignoring content for now as per Go example
  // which just wants to trigger logic) Go's http server reads headers etc. Here
  // we just read some bytes to ensure connection is established. In a real HTTP
  // server we'd parse. For this test, we assume the client sends a request and
  // we respond.
  while (true) {
    ssize_t bytes_read = read(client_socket, buffer, sizeof(buffer));
    if (bytes_read <= 0) {
      break;
    }

    if (deployment == "test1") {
      busyLoop(appPreRepeat + appPostRepeat);

      std::string response_body = make_big_string(appSize);
      std::string response = "HTTP/1.1 200 OK\r\n";
      response +=
          "Content-Length: " + std::to_string(response_body.length()) + "\r\n";
      response += "Content-Type: text/plain\r\n";
      response += "\r\n";
      response += response_body;

      write(client_socket, response.c_str(), response.length());
    } else {
      // Not implemented for other tests
      std::string response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
      write(client_socket, response.c_str(), response.length());
      // Close connection for unimplemented paths or handle gracefully?
      // Depending on requirement, but for keep-alive we might want to stay open
      // unless client closes. For 501, maybe just keep open or close. Let's
      // keep open for consistency unless error.
    }
  }

  close(client_socket);
}

int main() {
  deployment = get_env_var("deployment", "test1");
  // Go utils.GetEnvVar second arg is 'required', here we just default if
  // missing or check logic? The Go code: deployment =
  // utils.GetEnvVar("deployment", true) -> implies required. We will assume
  // environment is set correctly as per instructions to keep it simple.

  // Note: In Go, if env var is missing and required=true, it presumably
  // panics/logs error. For now we assume they are provided.

  appSize = str_to_int(get_env_var("appSize"), 0);
  appPreRepeat = str_to_int(get_env_var("appPreRepeat"), 0);
  appPostRepeat = str_to_int(get_env_var("appPostRepeat"), 0);
  listenPort = str_to_int(get_env_var("appListenPort"),
                          8080); // Go var is listenPort, env is appListenPort

  std::cout << "deployment: " << deployment << std::endl;
  std::cout << "appSize: " << appSize << std::endl;
  std::cout << "Starting server on port " << listenPort << std::endl;
  std::cout << "CPP app is running" << std::endl;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd == 0) {
    perror("socket failed");
    return 1;
  }

  int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
    return 1;
  }

  struct sockaddr_in address;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(listenPort);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    perror("bind failed");
    return 1;
  }

  if (listen(server_fd, 3000) < 0) { // 300 backlog
    perror("listen");
    return 1;
  }

  while (true) {
    int client_socket;
    struct sockaddr_in client_addr;
    int addrlen = sizeof(client_addr);

    client_socket = accept(server_fd, (struct sockaddr *)&client_addr,
                           (socklen_t *)&addrlen);
    if (client_socket < 0) {
      perror("accept");
      continue;
    }

    // Handle in a thread to support multiple connections similar to Go's
    // default http server
    std::thread(handle_connection, client_socket).detach();
  }

  return 0;
}
