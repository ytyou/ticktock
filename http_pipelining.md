# HTTP/1.1 Pipelining Support Plan

This document outlines the steps required to add HTTP/1.1 pipelining support to TickTock's HTTP server. The goal is to allow a client to send multiple HTTP requests over a single TCP connection without waiting for the previous response to be fully received, while guaranteeing that responses are returned in order.

## Overview

The current implementation of `HttpServer` handles one request per connection cycle. It reads a buffer, parses a single request, processes it, sends the response, and then frees the buffer. For pipelining we need to:

1. Parse **all** complete requests present in the buffer.
2. Queue responses and send them sequentially.
3. Handle partial requests and partial responses across multiple reads/writes.
4. Preserve the original request/response API for the rest of the codebase.

## Planned Steps

| # | Step | Description |
|---|------|-------------|
| 1 | Review current request flow | Understand how `recv_http_data` and `recv_http_data_cont` parse and process a single request.
| 2 | Define a response queue | Extend `HttpConnection` to hold a FIFO of `HttpResponse` objects or a simple list of response buffers.
| 3 | Modify request parsing | Update `recv_http_data` to loop over the buffer, parsing and processing each request until no more complete ones remain.
| 4 | Update send logic | Adapt `send_response` to send the next queued response, re‑queueing if not all bytes are sent, and preserving any remaining data for subsequent requests.
| 5 | Handle partial data | Ensure that incomplete requests keep the buffer and offset until more data arrives, and that incomplete responses do not consume new request data.
| 6 | Add unit tests | Create tests that send two or more pipelined requests in a single connection and assert responses arrive in order.
| 7 | Update integration tests | Verify pipelining works with real HTTP clients (curl, `httpie`, or custom test client).
| 8 | Documentation & cleanup | Update README or comments to explain pipelining support, and remove any leftover debug code.

## Detailed Action Items Updated

1. **Code Review** – Inspect `HttpServer::recv_http_data` and `recv_http_data_cont` to locate parsing logic and buffer handling.
2. **Data Structure** – In `HttpConnection`, add a `std::queue<HttpResponse>` or similar to hold responses.
3. **Parsing Loop** – After the first request is processed, check if `buff + processed_len < end_of_buffer`. If so, continue parsing from that point.
4. **Send Loop** – In `send_response`, iterate over the response queue, attempting to send each until either a send fails due to `EAGAIN` or all responses are fully transmitted.
5. **State Flags** – Use flags like `conn->pending_response` or `conn->response_queue_empty` to manage flow control.
6. **Partial Requests** – If a request is incomplete after parsing the buffer, preserve the current buffer and offset for the next `recv_http_data` call.
7. **Testing** – Write a simple test harness that connects via sockets, writes two pipelined HTTP requests, reads back two responses, and checks ordering.
8. **Performance** – Benchmark to ensure pipelining does not degrade throughput or increase latency.

## Risks & Mitigations

- **Race Conditions** – Ensure that the response queue is only accessed by the responder thread to avoid data races.
- **Memory Leaks** – Manage dynamic buffers carefully; free buffers only after their response has been fully sent.
- **Back‑pressure** – If the responder is busy, new requests should still be parsed and queued, but responses should be sent only when the socket is ready.

Once all steps are implemented and tests pass, pipelining will be fully supported and documented.
