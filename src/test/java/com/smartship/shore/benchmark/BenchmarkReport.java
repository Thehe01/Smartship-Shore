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
 * Whole-run benchmark document: environment + configuration + per-size results,
 * written as JSON, CSV and Markdown. No machine-specific numbers are hardcoded
 * anywhere in sources; everything here is measured at runtime.
 */
public class BenchmarkReport {

  public String generatedAt;
  public Map<String, Object> environment = new LinkedHashMap<>();
  public Map<String, Object> configuration = new LinkedHashMap<>();
  public List<BenchmarkResult> results = new ArrayList<>();
  public List<String> notes = new ArrayList<>();

  public static BenchmarkReport create(Map<String, Object> configuration) {
    BenchmarkReport report = new BenchmarkReport();
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
    report.environment.put("testcontainersVersion",
        packageVersion("org.testcontainers.Testcontainers"));
    report.environment.put("kafkaClientsVersion",
        packageVersion("org.apache.kafka.clients.producer.KafkaProducer"));
    return report;
  }

  private static String packageVersion(String className) {
    try {
      Package pkg = Class.forName(className).getPackage();
      String v = pkg == null ? null : pkg.getImplementationVersion();
      return v == null ? "unknown" : v;
    } catch (Exception e) {
      return "unknown";
    }
  }

  /** Writes {@code benchmark-results.json}, {@code .csv} and {@code .md} into the directory. */
  public void writeTo(Path directory) throws IOException {
    Files.createDirectories(directory);
    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    Files.writeString(directory.resolve("benchmark-results.json"),
        mapper.writeValueAsString(this), StandardCharsets.UTF_8);
    Files.writeString(directory.resolve("benchmark-results.csv"),
        toCsv(), StandardCharsets.UTF_8);
    Files.writeString(directory.resolve("benchmark-results.md"),
        toMarkdown(), StandardCharsets.UTF_8);
  }

  private String toCsv() {
    StringBuilder sb = new StringBuilder(
        "messages,mmsi_count,type_count,publish_duration_ms,publish_throughput_msg_s,"
            + "history_completion_ms,history_throughput_msg_s,"
            + "history_latency_p50_ms,history_latency_p95_ms,history_latency_p99_ms,"
            + "history_latency_max_ms,redis_completion_ms,redis_expected_keys,"
            + "redis_actual_keys,mysql_rows,distinct_msg_ids,dlt_records,lost_messages\n");
    for (BenchmarkResult r : results) {
      sb.append(r.messages).append(',')
          .append(r.mmsiCount).append(',')
          .append(r.typeCount).append(',')
          .append(r.publishDurationMs).append(',')
          .append(fmt(r.publishThroughputMsgS)).append(',')
          .append(r.historyCompletionMs).append(',')
          .append(fmt(r.historyThroughputMsgS)).append(',')
          .append(fmt(r.historyLatencyP50Ms)).append(',')
          .append(fmt(r.historyLatencyP95Ms)).append(',')
          .append(fmt(r.historyLatencyP99Ms)).append(',')
          .append(fmt(r.historyLatencyMaxMs)).append(',')
          .append(r.redisCompletionMs).append(',')
          .append(r.redisExpectedKeys).append(',')
          .append(r.redisActualKeys).append(',')
          .append(r.mysqlRows).append(',')
          .append(r.distinctMsgIds).append(',')
          .append(r.dltRecords).append(',')
          .append(r.lostMessages).append('\n');
    }
    return sb.toString();
  }

  private String toMarkdown() {
    StringBuilder sb = new StringBuilder();
    sb.append("# Smartship-Shore baseline benchmark\n\n");
    sb.append("- Generated: `").append(generatedAt).append("`\n");
    sb.append("- Java: `").append(environment.get("javaVersion")).append("` / OS: `")
        .append(environment.get("os")).append("` / CPUs: `")
        .append(environment.get("availableProcessors")).append("`\n");
    sb.append("- Config: `").append(configuration).append("`\n\n");
    sb.append("| Messages | Publish msg/s | History msg/s | P50 ms | P95 ms | P99 ms"
        + " | Redis converge ms | MySQL rows | Redis keys | DLT | Lost |\n");
    sb.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for (BenchmarkResult r : results) {
      sb.append("| ").append(r.messages)
          .append(" | ").append(fmt(r.publishThroughputMsgS))
          .append(" | ").append(fmt(r.historyThroughputMsgS))
          .append(" | ").append(fmt(r.historyLatencyP50Ms))
          .append(" | ").append(fmt(r.historyLatencyP95Ms))
          .append(" | ").append(fmt(r.historyLatencyP99Ms))
          .append(" | ").append(r.redisCompletionMs)
          .append(" | ").append(r.mysqlRows)
          .append(" | ").append(r.redisActualKeys).append('/').append(r.redisExpectedKeys)
          .append(" | ").append(r.dltRecords)
          .append(" | ").append(r.lostMessages)
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
