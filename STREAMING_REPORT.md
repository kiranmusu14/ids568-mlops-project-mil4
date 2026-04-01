# Streaming Pipeline Analysis

## Load Testing & Metrics

The following table demonstrates the performance of the streaming ingestion pipeline and tumbling window aggregations under varying load conditions. Each test ran for a minimum of 60 seconds; latency is measured from event `timestamp` generation in the producer to the moment the consumer successfully parses the JSON payload.

| Load Level | Target Rate | Observed Throughput | p50 Latency | p95 Latency | p99 Latency | Windows Closed (60 s) | Avg Events/Window | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Low (100 msg/s)** | 100 msg/s | ~100.00 msg/s | ~0.01 ms | ~0.02 ms | ~0.02 ms | 12 | ~500 | Stable; OS socket buffer never under pressure |
| **Medium (1K msg/s)** | 1,000 msg/s | ~1,000.00 msg/s | ~0.01 ms | ~0.02 ms | ~0.02 ms | 12 | ~5,000 | Stable; p99 unchanged from low-load baseline |
| **High (10K msg/s)** | 10,000 msg/s | ~7,780.80 msg/s | 0.02 ms | 0.02 ms | 0.03 ms | 12 | ~38,900 | OS socket buffer saturation limits effective rate to ~78% of target |
| **Breaking Point** | 1,000,000 msg/s | Socket crash | — | — | — | 0 | — | Consumer JSON parsing + window aggregation cannot drain the buffer; connection dropped |

*Note: The "High" load test was bottlenecked by OS-level socket buffer limits (default `SO_SNDBUF`/`SO_RCVBUF` ~128 KB on macOS). Burst simulation (10% chance of 5× spike) was active during all tests.*

---

## Queue Depth Monitoring

Queue depth is the number of bytes sitting unread in the OS socket receive buffer at any given moment. It is the earliest warning signal for backpressure — it starts growing before latency spikes and long before a connection drops.

| Load Level | Estimated Queue Depth (steady state) | Queue Behavior | Early Warning Signal |
| :--- | :--- | :--- | :--- |
| **Low (100 msg/s)** | Near 0 bytes | Drains instantly; buffer never accumulates | None — system fully keeping up |
| **Medium (1K msg/s)** | Near 0 bytes | Drains instantly | None |
| **High (10K msg/s)** | ~50–80 KB (approaching `SO_RCVBUF` limit of ~128 KB on macOS) | Buffer partially fills during bursts, drains between bursts | p99 latency begins to climb (0.02 ms → 0.03 ms); throughput cap at ~78% of target |
| **Breaking Point (1M msg/s)** | 128 KB (full — buffer saturated) | Buffer fills and stays full; producer's `send()` blocks then fails | Throughput flatlines → `BrokenPipeError` → socket crash |

**How to monitor queue depth on macOS/Linux:**
```bash
# Monitor socket receive buffer usage in real time while consumer is running
watch -n 1 "netstat -an | grep 9999"
# The Recv-Q column shows bytes waiting to be read by the consumer
```

A growing `Recv-Q` value that does not drain between windows is the definitive indicator that the consumer is falling behind. In a production system (e.g., Kafka), this maps directly to **consumer lag** — the number of unprocessed messages behind the latest offset — and is the primary metric watched by on-call engineers.

---

## Failure Handling Analysis

### 1. Backpressure and Degradation

During the stress test, the system's breaking point was reached by pushing the producer to emit 1,000,000 messages per second. At this extreme rate, the consumer could not parse the incoming JSON strings and compute the 5-second tumbling window aggregations fast enough. Unread data accumulated in the OS socket buffer until the buffer was exhausted, causing a complete connection drop and socket crash. This is a textbook backpressure scenario: the producer's send rate exceeds the consumer's processing capacity, and without flow control, the queue grows unbounded.

The observed graceful degradation at 10K msg/s (stabilizing at ~7,780 msg/s rather than crashing immediately) indicates the OS socket buffer acted as a natural short-term buffer, absorbing the burst before the rate became unsustainable.

### 2. Consumer Crash Scenarios

In this TCP socket-based architecture, if the consumer crashes mid-processing, any events held in the consumer's in-memory buffer waiting for the current tumbling window to close are permanently lost — there is no persistent offset or broker to replay from. The producer's `BrokenPipeError` handler logs the disconnect and waits for reconnect, but it does not buffer unsent events; those are also dropped.

In a production environment, replacing the raw TCP socket with Apache Kafka would address this entirely. Kafka persists all messages to disk with a configurable retention window. Upon consumer restart, it would resume reading from its last committed offset, providing at-least-once delivery guarantees with no data loss.

### 3. Reprocessing and Duplication

When stateful logic (such as the `total_purchase_volume` aggregation) is involved, simply restarting a crashed consumer introduces the risk of double-counting. Because this implementation uses an in-memory `state` dict that resets on each window close, any reprocessed messages from the producer would be added to the aggregation a second time, producing inflated totals and compromising data integrity.

A production-grade solution would require one or more of the following:
- **Idempotent writes**: Use a unique `event_id` to deduplicate messages before aggregating.
- **Transactional offsets**: Commit Kafka offsets only after the window result has been durably written (exactly-once semantics via Kafka transactions).
- **Watermark tracking**: Track late-arriving events against a watermark to drop duplicates that fall within already-closed windows.
