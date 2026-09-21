package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smartship.shore.config.KafkaConfig;
import com.smartship.shore.config.ShoreProperties;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

/**
 * P2-2.2 DLT producer tuning legitimacy: the timeout triple must satisfy
 * {@code delivery.timeout.ms >= request.timeout.ms + linger.ms}; legal triples
 * build a real producer without {@code ConfigException}, illegal ones fail fast at
 * validation time instead of failing DLT sends at runtime.
 */
class DltProducerConfigTest {

  @Test
  @DisplayName("Default DLT timeouts satisfy the invariant and build a real producer")
  void validDefaultsCreateProducer() {
    ShoreProperties properties = new ShoreProperties();

    assertDoesNotThrow(() -> properties.getKafka().validateDltTimeouts());

    Map<String, Object> configs = KafkaConfig.dltProducerConfigs(properties);
    int request = (Integer) configs.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
    int delivery = (Integer) configs.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
    int linger = (Integer) configs.get(ProducerConfig.LINGER_MS_CONFIG);
    assertEquals(4000, request);
    assertEquals(5000, delivery);
    assertEquals(0, linger);
    assertTrue(delivery >= request + linger, "producer tuning must stay legal");

    // A real KafkaProducer validates the full map: ConfigException here means illegal.
    // Construction never contacts a broker; the producer is closed immediately.
    try (Producer<String, String> producer =
        new DefaultKafkaProducerFactory<String, String>(configs).createProducer()) {
      assertTrue(producer != null);
    }
  }

  @Test
  @DisplayName("request=30000 with delivery=5000 fails fast instead of failing DLT at runtime")
  void invalidTimeoutsFailFast() {
    ShoreProperties properties = new ShoreProperties();
    properties.getKafka().setDltRequestTimeoutMs(30000);
    properties.getKafka().setDltDeliveryTimeoutMs(5000);

    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> properties.getKafka().validateDltTimeouts());
    assertTrue(e.getMessage().contains("delivery.timeout.ms"));
  }

  @Test
  @DisplayName("Boundary delivery == request + linger passes and flows into the map")
  void boundaryPasses() {
    ShoreProperties properties = new ShoreProperties();
    properties.getKafka().setDltRequestTimeoutMs(2000);
    properties.getKafka().setDltLingerMs(100);
    properties.getKafka().setDltDeliveryTimeoutMs(2100);

    assertDoesNotThrow(() -> properties.getKafka().validateDltTimeouts());

    Map<String, Object> configs = KafkaConfig.dltProducerConfigs(properties);
    assertEquals(2000, configs.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
    assertEquals(2100, configs.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
    assertEquals(100, configs.get(ProducerConfig.LINGER_MS_CONFIG));
  }
}
