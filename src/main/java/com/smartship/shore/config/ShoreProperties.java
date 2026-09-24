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
  private Redis redis = new Redis();

  /** Fail-fast shape check at startup; illegal Kafka tuning stops the boot, not the data. */
  @PostConstruct
  public void validate() {
    kafka.validateDltTimeouts();
    kafka.validateTopicDurability();
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
    /**
     * Application ACK master switch ({@code shore.mqtt.ack-enabled}, default true).
     * When false, shore never publishes {@code ship/{mmsi}/ack} (used by tests and
     * by deployments whose Edge fleet has not opted into Kafka-durable ACKs yet).
     */
    private boolean ackEnabled = true;
    /**
     * Bounded wait for one Application ACK publish ({@code shore.mqtt.ack-timeout-ms},
     * default 3s). Best-effort only: expiry is logged and counted, never thrown —
     * the Kafka record is already durable, and Edge resends on ACK timeout with the
     * duplicate absorbed by {@code UNIQUE(msg_id)}.
     */
    private long ackTimeoutMs = 3000L;
    /**
     * Bounded queue for the dedicated ACK publisher executor
     * ({@code shore.mqtt.ack-queue-capacity}, default 200). One slow broker may hold
     * at most 1 in-flight publish plus this many queued ACKs; overflow is rejected
     * fast (counted, never thrown) and Edge resends on ACK timeout with the
     * duplicate absorbed by {@code UNIQUE(msg_id)}. Bounds the pileup that an
     * unbounded {@code CompletableFuture.runAsync} pool would accumulate.
     */
    private int ackQueueCapacity = 200;
    /**
     * Consecutive PUBACK timeouts that trigger an ACK client rebuild
     * ({@code shore.mqtt.ack-client-rebuild-threshold}, default 3). Bounds the
     * damage of a wedged connection: after this many timeouts the client is
     * dropped and rebuilt instead of stalling the worker at one ACK per timeout
     * forever.
     */
    private int ackClientRebuildThreshold = 3;
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
    /** P2-3 consumer group projecting the raw topic into Redis latest-state hashes. */
    private String latestStateGroupId = "smartship-latest-state";
    /**
     * Topic replication factor for the raw topic. Local single-broker default is 1;
     * production must set {@code KAFKA_RAW_TOPIC_REPLICAS=3} (or more) so that
     * {@code acks=all} is a real quorum confirmation, not a single-copy write.
     */
    private int rawTopicReplicas = 1;
    /**
     * Topic replication factor for the DLT. Same rule as {@link #rawTopicReplicas}.
     */
    private int dltTopicReplicas = 1;
    /**
     * Topic-level {@code min.insync.replicas} enforced on both raw and DLT topics.
     * Production: {@code KAFKA_MIN_INSYNC_REPLICAS=2} with replicas=3. Must be
     * {@code >= 1} and {@code <= min(rawTopicReplicas, dltTopicReplicas)}.
     */
    private int minInsyncReplicas = 1;

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

    /**
     * Fail-fast invariant for topic durability tuning. The Application ACK contract
     * ("KAFKA_COMMITTED means quorum-durable") only holds when the raw topic has
     * real replicas; a single-copy {@code acks=all} is acknowledged here by refusing
     * incoherent combinations, not by refusing single-copy outright (local dev stays
     * bootable with replicas=1).
     *
     * <p>Rules: every factor {@code >= 1}; {@code minInsyncReplicas <=
     * min(rawTopicReplicas, dltTopicReplicas)} so the broker can ever elect a
     * writable ISR set.
     */
    public void validateTopicDurability() {
      if (rawTopicReplicas < 1 || dltTopicReplicas < 1 || minInsyncReplicas < 1) {
        throw new IllegalStateException(
            "Illegal Kafka topic durability: replicas and min.insync.replicas must all be >= 1 "
                + "(raw=" + rawTopicReplicas + ", dlt=" + dltTopicReplicas
                + ", min.insync=" + minInsyncReplicas + ")");
      }
      int minReplicas = Math.min(rawTopicReplicas, dltTopicReplicas);
      if (minInsyncReplicas > minReplicas) {
        throw new IllegalStateException(
            "Illegal Kafka topic durability: min.insync.replicas (" + minInsyncReplicas
                + ") must be <= min(rawTopicReplicas, dltTopicReplicas) (" + minReplicas
                + "), otherwise no ISR set can ever satisfy a write");
      }
    }
  }

  @Data
  public static class Redis {
    /**
     * TTL (seconds) for latest-state keys, refreshed on every effective update.
     * Stale events and duplicate redeliveries never touch it, so old data cannot
     * extend a state's lifetime.
     */
    private long latestStateTtlSeconds = 86400L;
  }
}
