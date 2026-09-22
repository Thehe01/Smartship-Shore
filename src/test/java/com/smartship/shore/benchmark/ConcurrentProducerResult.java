package com.smartship.shore.benchmark;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One P2-4.1.1 concurrent-producer case: {@code producerCount} independent MQTT
 * publishers sharing a fixed total message count. Plain mutable fields so
 * Jackson serializes without extra configuration.
 */
public class ConcurrentProducerResult {

  public String runId;
  public int producerCount;
  public int messagesPerProducer;
  public int messages;
  public int typeCount;
  public long publishDurationMs;
  public double publishThroughputMsgS;
  public long historyCompletionMs;
  public double historyThroughputMsgS;
  public double historyReceiveLatencyP50Ms;
  public double historyReceiveLatencyP95Ms;
  public double historyReceiveLatencyP99Ms;
  public double historyReceiveLatencyMaxMs;
  public double maxLagMs;
  public long recoveryTimeMs;
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
