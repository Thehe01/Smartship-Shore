package com.smartship.shore.kafka;

import com.smartship.shore.config.ShoreProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * Startup durability cross-check: compares the ACTUAL topic shape (replicas,
 * {@code min.insync.replicas}) against the configured expectation.
 *
 * <p>Why a verifier and not enforcement: the {@code NewTopic} beans only create
 * MISSING topics — they never expand replicas of EXISTING single-copy topics.
 * Expanding old topics is an operator runbook
 * ({@code docs/KAFKA_TOPIC_EXPANSION.md}). This verifier only ever logs WARN
 * (never fails the boot), so the expansion window doesn't CrashLoop the fleet.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TopicDurabilityVerifier {

  private final ShoreProperties properties;
  private final KafkaAdmin kafkaAdmin;

  /** Observed topic shape, kept minimal for testability. */
  record TopicInfo(String name, int partitions, int replicationFactor, String minInsync) {
  }

  @EventListener(ApplicationReadyEvent.class)
  public void verifyOnStartup() {
    Thread checker = new Thread(this::verifyQuietly, "shore-topic-durability-check");
    checker.setDaemon(true);
    checker.start();
  }

  private void verifyQuietly() {
    try (AdminClient admin =
        AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
      String raw = properties.getKafka().getRawTopic();
      String dlt = properties.getKafka().getDltTopic();
      DescribeTopicsResult topics =
          admin.describeTopics(List.of(raw, dlt));
      Map<String, TopicDescription> described =
          topics.allTopicNames().get(10, TimeUnit.SECONDS);

      DescribeConfigsResult configs = admin.describeConfigs(List.of(
          new ConfigResource(ConfigResource.Type.TOPIC, raw),
          new ConfigResource(ConfigResource.Type.TOPIC, dlt)));
      Map<ConfigResource, Config> configMap = configs.all().get(10, TimeUnit.SECONDS);

      List<TopicInfo> observed = new ArrayList<>();
      for (String name : List.of(raw, dlt)) {
        TopicDescription desc = described.get(name);
        int rf = desc.partitions().isEmpty() ? 0
            : desc.partitions().get(0).replicas().size();
        Config cfg = configMap.get(new ConfigResource(ConfigResource.Type.TOPIC, name));
        String minInsync = cfg == null ? null
            : cfg.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG) == null ? null
            : cfg.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value();
        observed.add(new TopicInfo(name, desc.partitions().size(), rf, minInsync));
      }
      for (String warning : verify(observed,
          name -> name.equals(dlt)
              ? properties.getKafka().getDltTopicReplicas()
              : properties.getKafka().getRawTopicReplicas(),
          properties.getKafka().getMinInsyncReplicas())) {
        log.warn("[Shore-Durability] {}", warning);
      }
    } catch (Exception e) {
      log.debug("[Shore-Durability] cross-check skipped (broker unreachable?): {}",
          e.getMessage());
    }
  }

  /**
   * Pure comparison, unit-tested. Every mismatch is one WARN line pointing at
   * the expansion runbook.
   */
  static List<String> verify(List<TopicInfo> observed,
      java.util.function.ToIntFunction<String> expectedRf, int expectedMinInsync) {
    List<String> warnings = new ArrayList<>();
    for (TopicInfo t : observed) {
      int wantRf = expectedRf.applyAsInt(t.name());
      if (t.replicationFactor() < wantRf) {
        warnings.add("topic " + t.name() + " replication-factor=" + t.replicationFactor()
            + " < expected " + wantRf
            + ": acks=all is NOT quorum-durable — expand replicas per"
            + " docs/KAFKA_TOPIC_EXPANSION.md");
      }
      if (t.minInsync() != null) {
        try {
          if (Integer.parseInt(t.minInsync().trim()) != expectedMinInsync) {
            warnings.add("topic " + t.name() + " min.insync.replicas=" + t.minInsync()
                + " != expected " + expectedMinInsync + ": align via"
                + " docs/KAFKA_TOPIC_EXPANSION.md");
          }
        } catch (NumberFormatException ignored) {
          // Broker default applies; the RF check above already covers durability.
        }
      }
    }
    return warnings;
  }
}
