# Streaming Pipeline Analysis

## Load Testing & Metrics

[cite_start]The following table demonstrates the performance of the streaming ingestion pipeline and tumbling window aggregations under varying load conditions [cite: 168-169].

| Load Level | p50 Latency | p95 Latency | p99 Latency | Throughput |
| :--- | :--- | :--- | :--- | :--- |
| **Low (100 msg/s)** | ~0.01 ms | ~0.02 ms | ~0.02 ms | ~100.00 msg/s |
| **Medium (1K msg/s)** | ~0.01 ms | ~0.02 ms | ~0.02 ms | ~1000.00 msg/s |
| **High (10K msg/s)** | 0.02 ms | 0.02 ms | 0.03 ms | 7780.80 msg/s |
| **Breaking Point** | Socket Crash | Socket Crash | Socket Crash | Attempted 1,000,000 msg/s |

*Note: Latency is calculated from event generation to the moment the consumer successfully parses the JSON payload. The "High" load test was bottlenecked by OS-level socket limitations, stabilizing at ~7,780 msg/s.*

## Failure Handling Analysis

### 1. Backpressure and Degradation
[cite_start]During the stress test, we identified the system's breaking point by pushing the producer to emit 1,000,000 messages per second [cite: 154, 233-236]. At this extreme rate, the consumer could not parse the incoming JSON strings and compute the 5-second tumbling window aggregations fast enough. [cite_start]This caused the unread data to pile up in the OS socket buffer (backpressure), ultimately leading to a complete connection drop and socket crash[cite: 154, 235]. 

### 2. Consumer Crash Scenarios
[cite_start]In this mock streaming architecture using standard TCP sockets, if the consumer crashes mid-processing, any events that were in flight or held in the consumer's memory buffer waiting for the tumbling window to close are permanently lost [cite: 38, 142-143, 153]. [cite_start]In a production environment, implementing a durable message broker like Apache Kafka would solve this by allowing the consumer to resume reading from its last committed offset upon recovery [cite: 239-240].

### 3. Reprocessing and Duplication
[cite_start]When stateful logic (like our total purchase volume aggregation) is involved, simply restarting a crashed consumer introduces the risk of data duplication [cite: 25, 230-231, 241]. [cite_start]Because we are simulating an at-least-once delivery mechanism without idempotent writes or strict watermark tracking for late-arriving data, any reprocessed messages from the producer would be added to the aggregations a second time, compromising data integrity [cite: 232, 241, 285-286].