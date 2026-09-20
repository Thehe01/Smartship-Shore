package com.smartship.shore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * SmartShip shore-side entry point (P2-1).
 *
 * <p>Minimum reliable link only: MQTT ingest -&gt; Kafka -&gt; MySQL history.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ShoreApplication {

  public static void main(String[] args) {
    SpringApplication.run(ShoreApplication.class, args);
  }
}
