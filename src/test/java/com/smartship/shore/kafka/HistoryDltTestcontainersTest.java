package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smartship.shore.EdgeFixtures;
import com.smartship.shore.persistence.TelemetryHistoryRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * P2-2 integration: real Kafka (KRaft) + real MySQL prove the wiring —
 * a valid envelope lands in MySQL with nothing in the DLT, while poison lands in the DLT
 * with its original topic/key/payload intact.
 *
 * <p>Runs in CI with Docker; skips gracefully without it.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
    "shore.mqtt.enabled=false",
    "spring.flyway.enabled=true",
    "spring.kafka.listener.auto-startup=true"
})
class HistoryDltTestcontainersTest {

  private static final String RAW = "ship.telemetry.raw";
  private static final String DLT = "ship.telemetry.raw.DLT";

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

  @Container
  @SuppressWarnings("resource")
  static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>("mysql:8.0")
          .withDatabaseName("smartship_shore")
          .withUsername("shore")
          .withPassword("shore123");

  @DynamicPropertySource
  static void infra(DynamicPropertyRegistry registry) {
    registry.add("shore.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.datasource.url",
        () -> MYSQL.getJdbcUrl()
            + "?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=Asia/Shanghai");
    registry.add("spring.datasource.username", MYSQL::getUsername);
    registry.add("spring.datasource.password", MYSQL::getPassword);
    registry.add("spring.datasource.driver-class-name", () -> "com.mysql.cj.jdbc.Driver");
  }

  @Autowired
  private TelemetryHistoryRepository repository;

  @Autowired
  private KafkaListenerEndpointRegistry listenerRegistry;

  private static String envelope(String mmsi, String type, String rowId, String eventInstant) {
    String msgId = EdgeFixtures.edgeStyleMsgId(mmsi, type, rowId, eventInstant);
    return "{\"msg_id\":\"" + msgId + "\","
        + "\"mmsi\":\"" + mmsi + "\","
        + "\"type\":\"" + type + "\","
        + "\"timestamp\":\"" + eventInstant + "\","
        + "\"sent_at\":\"2026-09-19T02:00:05.123Z\","
        + "\"data\":{\"speed_knots\":12.5}}";
  }

  private KafkaProducer<String, String> rawProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(props);
  }

  private KafkaConsumer<String, String> dltConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "shore-dlt-it");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of(DLT));
    return consumer;
  }

  private static String header(ConsumerRecord<String, String> rec, String name) {
    var h = rec.headers().lastHeader(name);
    assertTrue(h != null, "DLT header missing: " + name);
    return new String(h.value(), StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("IT: valid envelope → MySQL row, poison → DLT with original payload")
  void rawToDbAndPoisonToDlt() throws Exception {
    // P2-3: the latest-state group would route the same poison to the DLT a second time
    // (independent groups handle failures independently) and has no Redis here — park it
    // so this suite asserts exactly the history group's single DLT record.
    listenerRegistry.getListenerContainer("smartship-latest-state").stop();

    String validJson = envelope("413999999", "nmea_gps", "9001", "2026-09-19T02:00:00Z");
    String poisonJson = "{not-json-at-all}";

    try (KafkaProducer<String, String> producer = rawProducer();
        KafkaConsumer<String, String> consumer = dltConsumer()) {
      producer.send(new ProducerRecord<>(RAW, "413999999", validJson)).get();
      producer.send(new ProducerRecord<>(RAW, "413999999", poisonJson)).get();
      producer.flush();

      // Valid envelope lands in MySQL…
      long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
      while (repository.countAll() != 1L) {
        if (System.nanoTime() > deadline) {
          throw new AssertionError("timed out waiting for the history row");
        }
        Thread.sleep(500);
      }
      assertTrue(repository.findByMsgId(
          EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "9001", "2026-09-19T02:00:00Z"))
          .isPresent());

      // …while poison lands in the DLT with everything needed to diagnose it.
      ConsumerRecords<String, String> dlt = null;
      deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
      while (dlt == null || dlt.count() == 0) {
        dlt = consumer.poll(Duration.ofSeconds(2));
        if (System.nanoTime() > deadline) {
          throw new AssertionError("timed out waiting for the DLT record");
        }
      }
      assertEquals(1, dlt.count());
      ConsumerRecord<String, String> rec = dlt.iterator().next();
      assertEquals("413999999", rec.key(), "key=MMSI preserved");
      assertEquals(poisonJson, rec.value(), "original payload preserved verbatim");
      assertEquals(RAW, header(rec, "kafka_dlt-original-topic"));
      assertTrue(header(rec, "kafka_dlt-exception-fqcn").contains("InvalidTelemetryException"));
      assertTrue(header(rec, "shore-dlt-failed-at").contains("T"), "failure time present");
      assertEquals("poison", header(rec, "shore-dlt-reason"));

      // And nothing else arrives: exactly one DLT record total.
      ConsumerRecords<String, String> extra =
          consumer.poll(Duration.ofSeconds(5));
      assertEquals(0, extra.count(), "valid envelope must never reach the DLT");
      assertEquals(1L, repository.countAll(), "poison must never reach MySQL");
    }
  }
}
