# Changelog

## HTTP/1.1 Pipelining Support

### `include/http.h`

Added move constructor and move assignment operator declarations for `HttpResponse`.

### `src/core/http.cpp`

| # | Location | Bug | Fix |
|---|----------|-----|-----|
| 1 | `find_header_end` lambda | `"\\r\\n\\r\\n"` (literal backslashes) never matches; return was absolute. Then fixed to `"\r\n\r\n"` but `strstr` still failed: `parse_header` NUL-terminates fields in-place so `strstr` stops at the first `\0` and never reaches `\r\n\r\n` | Replaced `strstr` with a NUL-safe byte-by-byte 4-char scan; return offset relative to current position |
| 2 | `header_offset` calc | `content - buff` is absolute, wrong on 2nd+ request | Changed to `content - (buff + processed)` |
| 3 | `processed`/`remaining` update | `processed = delta; remaining -= processed` subtracts too much on 2nd+ iteration | Changed to `processed += delta; remaining -= delta` (both places) |
| 4 | `recv_http_data` free_buff block | Inner `{…}` always ran even after `free_buff = false` | Combined into `if (free_buff && pending_responses.empty())` |
| 5 | `recv_http_data_cont` if-statement | `if (!process_request(...))` with misaligned `push_back`/`conn_error` — `conn_error` always set, push_back only on failure | Always call `process_request`, always push_back, removed stray `conn_error` |
| 6 | `recv_http_data_cont` `&&` vs `\|\|` | `GET && PUT && POST` is always false | Changed to `\|\|` |
| 7 | `recv_http_data_cont` free_buff block | Same brace-structure bug as #4 | Same fix |
| 8 | `send_response` dangling reference | `HttpResponse& curr_resp` becomes dangling after `pop_front()`, then reassigned (UB) | Changed to `HttpResponse *curr_resp` refreshed at top of each loop iteration |
| 9 | `HttpResponse` move semantics | No move constructor → `push_back(std::move(...))` copies; both copies share `buffer`, causing double-free | Added proper move constructor and move assignment that null out the source's pointers |

### `src/core/tcp.cpp`

| #  | Location            | Bug                                                                                                                                       | Fix                                   |
|----|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| 12 | `TcpServer::listen` | `printf`/`Logger::info` logged `fd` instead of `port`, so startup message showed fd numbers (e.g. 51, 52) instead of actual port numbers | Changed both calls to pass `port` |

### `test/pipeline_test.cpp`

- **\#10 — wrong connect address**
  `create_tcp_socket6("127.0.0.1", ...)`: `inet_pton(AF_INET6, "127.0.0.1")` silently returns 0,
  leaving `sin6_addr = ::`, which happened to reach the server by accident. A follow-up change to
  `create_tcp_socket("127.0.0.1", ...)` (AF_INET) also fails: IPv4 clients cannot reliably reach an
  `AF_INET6` server depending on `net.ipv6.bindv6only`.
  **Fix:** `create_tcp_socket6("::1", 8080)` — unambiguous IPv6 loopback, always works.

- **\#11 — `check_body` wrong expected prefix**
  The prefix ended with `}` after `"commit":""`, but the server always appends `,"timestamp":"…"`.
  **Fix:** replaced the rigid prefix check with: body starts with `{`, contains `"version":` and `"repo":":`.
