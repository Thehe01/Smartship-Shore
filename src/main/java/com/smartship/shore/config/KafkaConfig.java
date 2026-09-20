package com.smartship.shore.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.model.TelemetryEnvelope;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka wiring for P2-1.
 *
 * <p>Producer reliability is delegated to Kafka itself: {@code acks=all}, idempotence and
 * bounded retries — no hand-rolled retry thread. Values are JSON; keys are plain MMSI strings.
 *
 * <p>Consumer semantics are at-least-once with manual offset confirmation: the offset of a
 * record is acknowledged only after MySQL has accepted it, and {@code UNIQUE(msg_id)} absorbs
 * the redelivery that follows a crash between the insert and the commit.
 */
@Configuration
public class KafkaConfig {

  /** Single P2-1 topic; key = MMSI keeps one ship on one partition (partition-local order). */
  public static final String RAW_TOPIC_BEAN = "shipTelemetryRawTopic";

  @Bean
  public KafkaAdmin shoreKafkaAdmin(ShoreProperties properties) {
    Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", properties.getKafka().getBootstrapServers());
    return new KafkaAdmin(configs);
  }

  @Bean(name = RAW_TOPIC_BEAN)
  public NewTopic shipTelemetryRawTopic(ShoreProperties properties) {
    // Single-broker local default replica factor; raise for real clusters.
    return new NewTopic(
        properties.getKafka().getRawTopic(),
        properties.getKafka().getRawTopicPartitions(),
        (short) 1);
  }

  @Bean
  public ProducerFactory<String, TelemetryEnvelope> shoreProducerFactory(
      ShoreProperties properties, ObjectMapper objectMapper) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        properties.getKafka().getBootstrapServers());
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    // ---- reliable producer: broker mechanisms only ----
    configs.put(ProducerConfig.ACKS_CONFIG, "all");
    configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    configs.put(ProducerConfig.RETRIES_CONFIG, 5);
    configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    // Pure JSON on the wire; the consumer parses it explicitly (simple, debuggable).
    configs.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

    DefaultKafkaProducerFactory<String, TelemetryEnvelope> factory =
        new DefaultKafkaProducerFactory<>(configs);
    factory.setValueSerializer(new JsonSerializer<>(objectMapper));
    return factory;
  }

  @Bean
  public KafkaTemplate<String, TelemetryEnvelope> shoreKafkaTemplate(
      ProducerFactory<String, TelemetryEnvelope> shoreProducerFactory) {
    return new KafkaTemplate<>(shoreProducerFactory);
  }

  @Bean
  public ConsumerFactory<String, String> shoreConsumerFactory(ShoreProperties properties) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        properties.getKafka().getBootstrapServers());
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getKafka().getGroupId());
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    // Raw JSON string; parsing/validation stays inside HistoryConsumer for a clear policy.
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    // A brand-new group replays the raw topic from the beginning instead of skipping history;
    // UNIQUE(msg_id) downstream absorbs anything already stored. Safe under at-least-once.
    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new DefaultKafkaConsumerFactory<>(configs);
  }

  /**
   * Manual-confirm container: offsets commit only via the {@code Acknowledgment} passed to
   * {@code HistoryConsumer}. Transient DB failures are redelivered with backoff (no DLT in
   * P2-1); poison records are filtered inside the listener itself.
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String>
      shoreKafkaListenerContainerFactory(ConsumerFactory<String, String> shoreConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(shoreConsumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    // One thread in P2-1: preserves per-partition order into MySQL.
    factory.setConcurrency(1);
    factory.setCommonErrorHandler(
        new DefaultErrorHandler(new FixedBackOff(2000L, FixedBackOff.UNLIMITED_ATTEMPTS)));
    return factory;
  }
}
