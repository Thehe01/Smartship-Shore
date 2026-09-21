package com.smartship.shore.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Shore-side tunables (prefix {@code shore}).
 *
 * <p>Secrets and environment-specific endpoints stay in {@code application*.yml} /
 * environment variables, never in Java source.
 */
@Data
@Component
@ConfigurationProperties(prefix = "shore")
public class ShoreProperties {

  private Mqtt mqtt = new Mqtt();
  private Kafka kafka = new Kafka();
  private History history = new History();

  /** Fail-fast shape check at startup; illegal Kafka tuning stops the boot, not the data. */
  @PostConstruct
  public void validate() {
    kafka.validateDltTimeouts();
  }

  @Data
  public static class Mqtt {
    /** Master switch; {@code false} disables the subscriber (used by tests). */
    private boolean enabled = true;
    private String brokerUrl = "tcp://localhost:1883";
    /** Single shore instance in P2-1; keep stable so the broker can queue QoS1 while offline. */
    private String clientId = "smartship-shore";
    /**
     * Subscription filter matching the Edge publisher convention
     * {@code zncb/{mmsi}/{topicSuffix}} (see Edge {@code MqttPublisher}).
     */
    private String topics = "zncb/+/+";
    /** Edge publishes with QoS 1; shore subscribes with the same level. */
    private int qos = 1;
    /** Durable session: broker queues QoS1 messages for us while we are offline. */
    private boolean cleanSession = false;
    private boolean autoReconnect = true;
    private int keepAliveInterval = 60;
    private int connectionTimeout = 10;
    private String username;
    private String password;
    /** Delay between reconnect attempts when the broker is unreachable at startup. */
    private long connectRetryDelayMs = 5000L;
    /**
     * Bounded wait for the Kafka broker acknowledgment inside the MQTT callback
     * ({@code shore.mqtt.kafka-handoff-timeout-ms}, default 5s). The Paho callback thread
     * is serial, so this wait also keeps MQTT acknowledgments in arrival order. Never
     * infinite: on timeout the message stays unacknowledged and a reconnect is forced
     * so the broker redelivers it.
     */
    private long kafkaHandoffTimeoutMs = 5000L;
  }

  @Data
  public static class Kafka {
    private String bootstrapServers = "localhost:9092";
    /** P2-1 owns exactly one topic. */
    private String rawTopic = "ship.telemetry.raw";
    /** Partition count for the raw topic (key = MMSI keeps one ship ordered). */
    private int rawTopicPartitions = 6;
    /** P2-2 dead-letter topic for exhausted retries and poison records. */
    private String dltTopic = "ship.telemetry.raw.DLT";
    /**
     * P2-2.2 DLT producer timeouts. Kafka requires
     * {@code delivery.timeout.ms >= request.timeout.ms + linger.ms}; the defaults
     * (5000 >= 4000 + 0) satisfy it. Any violation fails fast at startup.
     */
    private int dltRequestTimeoutMs = 4000;
    private int dltDeliveryTimeoutMs = 5000;
    private int dltLingerMs = 0;
    private int dltMaxBlockMs = 5000;
    /** P2-1 history consumer group. */
    private String groupId = "smartship-history";

    /**
     * Fail-fast invariant for the dead-letter producer tuning. Throws
     * {@code IllegalStateException} on violation so the application refuses to boot
     * with an incoherent timeout triple instead of failing DLT sends at runtime.
     */
    public void validateDltTimeouts() {
      if (dltDeliveryTimeoutMs < dltRequestTimeoutMs + dltLingerMs) {
        throw new IllegalStateException(
            "Illegal DLT producer timeouts: delivery.timeout.ms (" + dltDeliveryTimeoutMs
                + ") must be >= request.timeout.ms (" + dltRequestTimeoutMs
                + ") + linger.ms (" + dltLingerMs + ")");
      }
    }
  }

  @Data
  public static class History {
    /** Fallback group id alias kept for readability in docs; same value as kafka.group-id. */
    private String groupId = "smartship-history";
  }

  @Data
  public static class Redis {
    /**
     * TTL (seconds) for latest-state keys, refreshed on every effective update.
     * Stale events never touch it, so old data cannot extend a state's lifetime.
     */
    private long latestStateTtlSeconds = 86400L;
    /** Consumer group projecting the raw topic into Redis latest-state hashes. */
    private String latestStateGroupId = "smartship-latest-state";
  }

  private Redis redis = new Redis();
}
