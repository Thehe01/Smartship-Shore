package com.smartship.shore.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Whole-run P2-4.1.1 document: environment + configuration + per-producer-count
 * results, written as JSON, CSV and Markdown into the shared
 * {@code target/benchmark-results/} directory (file names are
 * {@code concurrent-producer-results.*}, so the baseline
 * {@code benchmark-results.*} files are never touched).
 */
public class ConcurrentProducerReport {

  public String generatedAt;
  public Map<String, Object> environment = new LinkedHashMap<>();
  public Map<String, Object> configuration = new LinkedHashMap<>();
  public List<ConcurrentProducerResult> results = new ArrayList<>();
  public List<String> notes = new ArrayList<>();

  public static ConcurrentProducerReport create(Map<String, Object> configuration) {
    ConcurrentProducerReport report = new ConcurrentProducerReport();
    report.generatedAt = Instant.now().toString();
    report.configuration.putAll(configuration);
    report.environment.put("javaVersion", System.getProperty("java.version"));
    report.environment.put("javaVendor", System.getProperty("java.vendor"));
    report.environment.put("os", System.getProperty("os.name") + " "
        + System.getProperty("os.version") + " " + System.getProperty("os.arch"));
    report.environment.put("availableProcessors", Runtime.getRuntime().availableProcessors());
    report.environment.put("jvmMaxMemoryMb",
        Runtime.getRuntime().maxMemory() / 1024 / 1024);
    report.environment.put("kafkaImage", "apache/kafka:3.8.0");
    report.environment.put("mysqlImage", "mysql:8.0");
    report.environment.put("redisImage", "redis:7-alpine");
    report.environment.put("mosquittoImage", "eclipse-mosquitto:2.0");
    return report;
  }

  /** Writes {@code concurrent-producer-results.json}, {@code .csv} and {@code .md}. */
  public void writeTo(Path directory) throws IOException {
    Files.createDirectories(directory);
    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    Files.writeString(directory.resolve("concurrent-producer-results.json"),
        mapper.writeValueAsString(this), StandardCharsets.UTF_8);
    Files.writeString(directory.resolve("concurrent-producer-results.csv"),
        toCsv(), StandardCharsets.UTF_8);
    Files.writeString(directory.resolve("concurrent-producer-results.md"),
        toMarkdown(), StandardCharsets.UTF_8);
  }

  private String toCsv() {
    StringBuilder sb = new StringBuilder(
        "producer_count,messages,type_count,publish_duration_ms,publish_throughput_msg_s,"
            + "history_completion_ms,history_throughput_msg_s,"
            + "history_receive_latency_p50_ms,history_receive_latency_p95_ms,"
            + "history_receive_latency_p99_ms,history_receive_latency_max_ms,"
            + "redis_completion_ms,redis_expected_keys,"
            + "redis_actual_keys,mysql_rows,distinct_msg_ids,dlt_records,lost_messages,"
            + "duplicate_count\n");
    for (ConcurrentProducerResult r : results) {
      sb.append(r.producerCount).append(',')
          .append(r.messages).append(',')
          .append(r.typeCount).append(',')
          .append(r.publishDurationMs).append(',')
          .append(fmt(r.publishThroughputMsgS)).append(',')
          .append(r.historyCompletionMs).append(',')
          .append(fmt(r.historyThroughputMsgS)).append(',')
          .append(fmt(r.historyReceiveLatencyP50Ms)).append(',')
          .append(fmt(r.historyReceiveLatencyP95Ms)).append(',')
          .append(fmt(r.historyReceiveLatencyP99Ms)).append(',')
          .append(fmt(r.historyReceiveLatencyMaxMs)).append(',')
          .append(r.redisCompletionMs).append(',')
          .append(r.redisExpectedKeys).append(',')
          .append(r.redisActualKeys).append(',')
          .append(r.mysqlRows).append(',')
          .append(r.distinctMsgIds).append(',')
          .append(r.dltRecords).append(',')
          .append(r.lostMessages).append(',')
          .append(fmt(r.duplicateCount)).append('\n');
    }
    return sb.toString();
  }

  private String toMarkdown() {
    StringBuilder sb = new StringBuilder();
    sb.append("# Smartship-Shore concurrent-producer benchmark\n\n");
    sb.append("- Generated: `").append(generatedAt).append("`\n");
    sb.append("- Java: `").append(environment.get("javaVersion")).append("` / OS: `")
        .append(environment.get("os")).append("` / CPUs: `")
        .append(environment.get("availableProcessors")).append("`\n");
    sb.append("- Config: `").append(configuration).append("`\n\n");
    sb.append("| Producers | Messages | Publish msg/s | History msg/s | P50 ms | P95 ms | P99 ms"
        + " | Redis converge ms | MySQL rows | Redis keys | DLT | Lost | Duplicates |\n");
    sb.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for (ConcurrentProducerResult r : results) {
      sb.append("| ").append(r.producerCount)
          .append(" | ").append(r.messages)
          .append(" | ").append(fmt(r.publishThroughputMsgS))
          .append(" | ").append(fmt(r.historyThroughputMsgS))
          .append(" | ").append(fmt(r.historyReceiveLatencyP50Ms))
          .append(" | ").append(fmt(r.historyReceiveLatencyP95Ms))
          .append(" | ").append(fmt(r.historyReceiveLatencyP99Ms))
          .append(" | ").append(r.redisCompletionMs)
          .append(" | ").append(r.mysqlRows)
          .append(" | ").append(r.redisActualKeys).append('/').append(r.redisExpectedKeys)
          .append(" | ").append(r.dltRecords)
          .append(" | ").append(r.lostMessages)
          .append(" | ").append(fmt(r.duplicateCount))
          .append(" |\n");
    }
    sb.append('\n');
    for (String note : notes) {
      sb.append("- ").append(note).append('\n');
    }
    return sb.toString();
  }

  private static String fmt(double v) {
    if (Double.isNaN(v)) {
      return "n/a";
    }
    return String.format(Locale.ROOT, "%.2f", v);
  }
}
