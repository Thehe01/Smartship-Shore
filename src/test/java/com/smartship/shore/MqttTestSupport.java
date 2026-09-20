package com.smartship.shore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Small helpers for the Mosquitto-based E2E tests: one-shot QoS1 publishing with connect
 * retries (the broker may still be starting) plus deadline polling.
 */
public final class MqttTestSupport {

  private MqttTestSupport() {
  }

  /** Publishes one QoS1 message and disconnects; retries connect while the broker starts. */
  public static void publishOnce(String host, int port, String topic, String json) throws Exception {
    MqttClient publisher = new MqttClient(
        "tcp://" + host + ":" + port,
        "shore-test-pub-" + UUID.randomUUID().toString().substring(0, 8),
        new MemoryPersistence());
    MqttConnectOptions options = new MqttConnectOptions();
    options.setCleanSession(true);
    options.setConnectionTimeout(10);
    options.setAutomaticReconnect(false);

    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (true) {
      try {
        publisher.connect(options);
        break;
      } catch (Exception e) {
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException("test publisher could not connect to "
              + host + ":" + port, e);
        }
        Thread.sleep(500);
      }
    }
    try {
      MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
      message.setQos(1);
      publisher.publish(topic, message);
    } finally {
      try {
        publisher.disconnect();
      } catch (Exception ignored) {
        // Best effort; the test assertions decide.
      }
      try {
        publisher.close();
      } catch (Exception ignored) {
        // Best effort.
      }
    }
  }

  /** Polls until the condition holds or the timeout expires. */
  public static void waitUntil(String what, Duration timeout, BooleanSupplier condition)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for: " + what);
      }
      Thread.sleep(500);
    }
  }
}
