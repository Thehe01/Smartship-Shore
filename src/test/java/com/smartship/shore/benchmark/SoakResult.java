package com.smartship.shore.benchmark;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One P2-4.1.2 sustained-pressure soak case: paced publishers (fixed per-producer
 * totals, rate-capped), a shore-link outage with broker-side queueing, then a
 * catch-up drain. Plain mutable fields so Jackson serializes without extra
 * configuration.
 */
public class SoakResult {

  public String runId;
  public int ships;
  public int pointsPerSecPerShip;
  public int steadyPerProducer;
  public int outagePerProducer;
  public int messages;
  public double achievedPublishRateMsgS;
  public long publishDurationMs;
  public double publishThroughputMsgS;
  public long historyCompletionMs;
  public double historyThroughputMsgS;
  public double historyReceiveLatencyP50Ms;
  public double historyReceiveLatencyP95Ms;
  public double historyReceiveLatencyP99Ms;
  public double historyReceiveLatencyMaxMs;
  public long outageBacklogMessages;
  public long catchupDrainMs;
  public long redisCompletionMs;
  public int redisExpectedKeys;
  public int redisActualKeys;
  public long mysqlRows;
  public long distinctMsgIds;
  public long dltRecords;
  public long lostMessages;
  public double duplicateCount;
  public Map<String, Double> metricsDelta = new LinkedHashMap<>();
}
