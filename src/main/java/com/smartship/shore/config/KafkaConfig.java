package com.smartship.shore.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.ingest.InvalidTelemetryException;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka wiring for P2-1 / P2-2.
 *
 * <p>Producer reliability is delegated to Kafka itself: {@code acks=all}, idempotence and
 * bounded retries — no hand-rolled retry thread. Values are JSON; keys are plain MMSI strings.
 *
 * <p>Consumer semantics are at-least-once with manual offset confirmation: the offset of a
 * record is acknowledged only after MySQL has accepted it, and {@code UNIQUE(msg_id)} absorbs
 * the redelivery that follows a crash between the insert and the commit.
 *
 * <p>P2-2 adds bounded retry + dead-letter handling via Spring Kafka's
 * {@code DefaultErrorHandler} / {@code DeadLetterPublishingRecoverer} / {@code FixedBackOff}:
 * transient DB failures retry 3 times at 1s intervals, then go to the DLT; poison goes to
 * the DLT immediately. Exactly-once is never claimed.
 */
@Configuration
public class KafkaConfig {

  /** Single P2-1 topic; key = MMSI keeps one ship on one partition (partition-local order). */
  public static final String RAW_TOPIC_BEAN = "shipTelemetryRawTopic";
  /** P2-2 dead-letter topic bean. */
  public static final String DLT_TOPIC_BEAN = "shipTelemetryDltTopic";
  /** P2-2: fixed interval between bounded retries. */
  public static final long HISTORY_RETRY_INTERVAL_MS = 1000L;
  /** P2-2: bounded retry attempts before a record goes to the DLT. Never unlimited. */
  public static final long HISTORY_MAX_RETRIES = 3L;

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

  @Bean(name = DLT_TOPIC_BEAN)
  public NewTopic shipTelemetryDltTopic(ShoreProperties properties) {
    // Same partition count as raw so the default recoverer keeps the original partition
    // (and therefore per-MMSI order) inside the DLT.
    return new NewTopic(
        properties.getKafka().getDltTopic(),
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
   * P2-2: plain string template for dead-letter publishing. The DLT carries the original
   * key (MMSI) and the original JSON value verbatim, plus Spring Kafka's DLT headers.
   */
  @Bean
  public KafkaTemplate<String, String> shoreDltKafkaTemplate(ShoreProperties properties) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        properties.getKafka().getBootstrapServers());
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.ACKS_CONFIG, "all");
    configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    configs.put(ProducerConfig.RETRIES_CONFIG, 5);
    configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    // P2-2.1: bound the DLT send itself — Spring waits
    // max(delivery.timeout.ms + buffer, waitForSendResultTimeout), so the producer-side
    // cap is what makes the 5s recovery bound real. A sick broker fails fast here and
    // the record is redelivered instead of parking the recovery thread.
    configs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
    configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs));
  }

  /**
   * P2-2 consumer error handling, built only from Spring Kafka primitives.
   * Public static so contract tests drive the exact production wiring.
   * <ul>
   *   <li>retryable (transient DB failures): {@code FixedBackOff(1000ms, 3 attempts)}, then DLT;</li>
   *   <li>anything else reaching the handler (poison, validation, deterministic SQL):
   *   straight to the DLT with no pointless retries;</li>
   *   <li>{@code DuplicateKeyException} never reaches this handler — the listener treats
   *   it as success and acknowledges (idempotency unchanged);</li>
   *   <li>{@code commitRecovered} advances the offset only after the DLT publish lands.</li>
   * </ul>
   */
  public static DefaultErrorHandler historyErrorHandler(
      KafkaTemplate<String, String> dltTemplate, ShoreMetrics metrics) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(dltTemplate);
    // P2-2.1 DLT publish safety: a failed or timed-out DLT send must surface as a
    // recovery failure (offset stays put, redelivery/recovery continues) instead of
    // silently passing with commitRecovered=true advancing the original offset.
    // The wait is bounded — never an infinite join on the send future. Note the
    // effective wait is max(delivery.timeout.ms + buffer, this timeout): with the
    // producer capped at 5s and a zero buffer the recovery bound is exactly 5s.
    recoverer.setFailIfSendResultIsError(true);
    recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(5));
    recoverer.setTimeoutBuffer(0);
    // Spring's default DLT headers already carry original topic/partition/offset/key plus
    // exception type/message/stacktrace; append only what is missing: failure time and a
    // closed-bucket reason for operators.
    recoverer.addHeadersFunction((ConsumerRecord<?, ?> record, Exception ex) ->
        new RecordHeaders(new org.apache.kafka.common.header.Header[] {
            new RecordHeader("shore-dlt-failed-at",
                Instant.now().toString().getBytes(StandardCharsets.UTF_8)),
            new RecordHeader("shore-dlt-reason",
                ShoreMetrics.classify(ex).getBytes(StandardCharsets.UTF_8))
        }));

    DefaultErrorHandler handler = new DefaultErrorHandler(
        (record, ex) -> {
          // historyDlt increments only after accept() returns: a failed or timed-out
          // DLT send throws first, so "attempted DLT" is never counted as "in DLT".
          recoverer.accept(record, ex);
          metrics.historyDlt(ShoreMetrics.classify(ex));
        },
        new FixedBackOff(HISTORY_RETRY_INTERVAL_MS, HISTORY_MAX_RETRIES));
    handler.addRetryableExceptions(
        TransientDataAccessException.class,
        DataAccessResourceFailureException.class,
        CannotGetJdbcConnectionException.class);
    handler.addNotRetryableExceptions(
        InvalidTelemetryException.class,
        DataIntegrityViolationException.class,
        BadSqlGrammarException.class,
        InvalidDataAccessApiUsageException.class);
    handler.setRetryListeners(
        (record, ex, deliveryAttempt) -> metrics.historyRetry(ShoreMetrics.classify(ex)));
    // The original offset advances only after the DLT publish succeeds.
    handler.setCommitRecovered(true);
    return handler;
  }

  @Bean
  public CommonErrorHandler shoreHistoryErrorHandler(
      KafkaTemplate<String, String> shoreDltKafkaTemplate, ShoreMetrics metrics) {
    return historyErrorHandler(shoreDltKafkaTemplate, metrics);
  }

  /**
   * Manual-confirm container: offsets commit only via the {@code Acknowledgment} passed to
   * {@code HistoryConsumer} (or via the error handler after a recovered DLT publish).
   * Failures never commit early; {@code concurrency=1} preserves per-partition order into MySQL.
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String>
      shoreKafkaListenerContainerFactory(ConsumerFactory<String, String> shoreConsumerFactory,
          CommonErrorHandler shoreHistoryErrorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(shoreConsumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    // One thread: preserves per-partition order into MySQL.
    factory.setConcurrency(1);
    factory.setCommonErrorHandler(shoreHistoryErrorHandler);
    return factory;
  }
}
