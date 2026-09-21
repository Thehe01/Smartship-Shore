package com.smartship.shore.benchmark;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-size benchmark outcome (benchmark-only, never a production DTO).
 *
 * <p>Clock definitions:
 * <ul>
 *   <li>publish window: first MQTT publish start → last MQTT publish return;</li>
 *   <li>history completion: first send → MySQL holding every row;</li>
 *   <li>redis converge: first send → every expected latest key holding its final event;</li>
 *   <li>history latency per row: {@code received_at - sent_at}, where {@code sent_at} is
 *   stamped at MQTT publish time and {@code received_at} at history persistence — an
 *   application-level approximation of end-to-end latency through MQTT + Kafka + MySQL,
 *   NOT a Kafka broker-internal latency.</li>
 * </ul>
 * Percentiles use nearest-rank over the real per-row samples (see README).
 */
public class BenchmarkResult {

  public String runId;
  public int messages;
  public int mmsiCount;
  public int typeCount;

  public long publishDurationMs;
  public double publishThroughputMsgS;

  public long historyCompletionMs;
  public double historyThroughputMsgS;

  public double historyLatencyP50Ms;
  public double historyLatencyP95Ms;
  public double historyLatencyP99Ms;
  public double historyLatencyMaxMs;

  public long redisCompletionMs;
  public int redisExpectedKeys;
  public int redisActualKeys;

  public long mysqlRows;
  public long distinctMsgIds;
  public long dltRecords;
  public long lostMessages;

  /** Micrometer counter deltas (after − before) around the measured case. */
  public Map<String, Double> metricsDelta = new LinkedHashMap<>();

  /** Nearest-rank percentile over ascending-sorted samples. */
  public static double percentile(double[] sortedAscending, double p) {
    if (sortedAscending.length == 0) {
      return Double.NaN;
    }
    int rank = (int) Math.ceil(p / 100.0 * sortedAscending.length);
    int idx = Math.min(Math.max(rank - 1, 0), sortedAscending.length - 1);
    return sortedAscending[idx];
  }
}
