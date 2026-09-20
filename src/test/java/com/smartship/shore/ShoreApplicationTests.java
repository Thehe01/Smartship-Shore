package com.smartship.shore;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Wiring smoke test: full Spring context starts with H2, MQTT ingest disabled and the Kafka
 * listener parked (see {@code src/test/resources/application.yml}).
 */
@SpringBootTest
class ShoreApplicationTests {

  @Test
  @DisplayName("Spring context loads with test defaults")
  void contextLoads() {
  }
}
