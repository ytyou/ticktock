  /*
   * Minimal test for HTTP/1.1 pipelining support.
   *
   * The test starts the TickTock server in a child process, then creates a
   * plain TCP socket to the HTTP port, sends two pipelined GET requests to
   * /api/version, and verifies that two 200 responses are received in
   * order.
   */

  #include <cstdio>
  #include <cstdlib>
  #include <cstring>
  #include <unistd.h>
  #include <sys/socket.h>
  #include <netinet/in.h>
  #include <arpa/inet.h>
  #include <string>
  #include <cctype>

  /* Custom port – the default config listens on 6182, so we override to 8080. */
  // The test assumes a TickTock server is already running on the
  // configured port.  We only print the connection target.

  static int create_tcp_socket(const char *ip, uint16_t port) {
      int fd = socket(AF_INET, SOCK_STREAM, 0);
      if (fd < 0) return -1;
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port   = htons(port);
      inet_pton(AF_INET, ip, &addr.sin_addr);
      if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
          close(fd); return -1;
      }
      return fd;
  }

   // Create a TCP socket and connect to an IPv6 address.
    static int create_tcp_socket6(const char *ip, uint16_t port) {
        int fd = socket(AF_INET6, SOCK_STREAM, 0);
        if (fd < 0) return -1;
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        addr.sin6_port   = htons(port);
        inet_pton(AF_INET6, ip, &addr.sin6_addr);
        if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            close(fd); return -1;
        }
        return fd;
    }


  static void write_all(int fd, const char *data, size_t len) {
      size_t sent = 0;
      while (sent < len) {
          ssize_t n = write(fd, data + sent, len - sent);
          if (n <= 0) exit(1);
          sent += n;
      }
  }

  static std::string read_until(int fd, const std::string &delim) {
      std::string res;
      char buf[1];
      while (true) {
          ssize_t n = read(fd, buf, 1);
          if (n <= 0) break;
          res.push_back(buf[0]);
          if (res.size() >= delim.size() &&
              res.compare(res.size()-delim.size(), delim.size(), delim) == 0)
              break;
      }
      return res;
  }

  static std::string read_body(int fd, size_t len) {
      std::string body;
      body.resize(len);
      size_t total = 0;
      while (total < len) {
          ssize_t n = read(fd, &body[total], len - total);
          if (n <= 0) break;
          total += n;
      }
      body.resize(total);
      return body;
  }

  int run_pipeline_test() {
    // The test no longer starts the server; we assume one is running.
    printf("connecting to [::1]:8080\n");
      int sock = create_tcp_socket6("::1", 8080);
      if (sock < 0) {
          fprintf(stderr, "failed to connect to server\n");
          return 1;
      }

      const char *req =
          "GET /api/version HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"
          "GET /api/version HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";
      write_all(sock, req, strlen(req));

      /* first response */
      std::string resp1_header = read_until(sock, "\r\n\r\n");
      size_t len1 = 0;
      {
          std::string::size_type pos = resp1_header.find("Content-Length:");
          if (pos != std::string::npos) {
              pos += 15;
              while (pos < resp1_header.size() && resp1_header[pos] == ' ') ++pos;
              std::string::size_type end = resp1_header.find('\r', pos);
              if (end == std::string::npos) end = resp1_header.find('\n', pos);
              if (end != std::string::npos)
                  len1 = std::stoul(resp1_header.substr(pos, end-pos));
          }
      }
      std::string body1 = read_body(sock, len1);

      /* second response */
      std::string resp2_header = read_until(sock, "\r\n\r\n");
      size_t len2 = 0;
      {
          std::string::size_type pos = resp2_header.find("Content-Length:");
          if (pos != std::string::npos) {
              pos += 15;
              while (pos < resp2_header.size() && resp2_header[pos] == ' ') ++pos;
              std::string::size_type end = resp2_header.find('\r', pos);
              if (end == std::string::npos) end = resp2_header.find('\n', pos);
              if (end != std::string::npos)
                  len2 = std::stoul(resp2_header.substr(pos, end-pos));
          }
      }
      std::string body2 = read_body(sock, len2);

      if (resp1_header.find("200") == std::string::npos ||
          resp2_header.find("200") == std::string::npos) {
          fprintf(stderr, "unexpected response\n");
          return 1;
      }

      /* Verify that the body looks like a valid /api/version JSON response. */
      auto check_body = [&](const std::string &body) -> bool {
          if (body.empty() || body[0] != '{') return false;
          if (body.find("\"version\":") == std::string::npos) return false;
          if (body.find("\"repo\":") == std::string::npos) return false;
          return true;
      };

      if (!check_body(body1) || !check_body(body2)) {
          fprintf(stderr, "body mismatch\n");
          return 1;
      }

      printf("Pipeline test passed\n");
      return 0;
  }

  int main() { return run_pipeline_test(); }
