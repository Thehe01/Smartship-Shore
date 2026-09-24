package com.smartship.shore.ingest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.observability.ShoreMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ACK publisher backpressure: one slow broker may hold at most 1 in-flight plus
 * a bounded queue; overflow rejects fast (Edge resends) instead of piling up
 * unbounded threads.
 */
class MqttAckPublisherBoundedTest {

  private ShoreProperties properties;
  private ShoreMetrics metrics;
  private MqttAckPublisher publisher;
  private MqttClient client;
  private ExecutorService callers;

  @BeforeEach
  void setUp() {
    properties = new ShoreProperties();
    properties.getMqtt().setAckTimeoutMs(10_000L);
    properties.getMqtt().setAckQueueCapacity(1);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    publisher = new MqttAckPublisher(properties, new ObjectMapper(), metrics);
    client = mock(MqttClient.class);
    when(client.isConnected()).thenReturn(true);
    publisher.setClientForTests(client);
    callers = Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "ack-test-caller");
      t.setDaemon(true);
      return t;
    });
  }

  @AfterEach
  void tearDown() {
    publisher.shutdownAckExecutor();
    callers.shutdownNow();
  }

  @Test
  @DisplayName("Queue full rejects fast instead of piling up threads")
  void queueFullRejectsFast() throws Exception {
    double failedBefore = metrics.getKafkaAckFailedTotal().count();
    CountDownLatch enteredPublish = new CountDownLatch(1);
    CountDownLatch releasePublish = new CountDownLatch(1);
    doAnswer(inv -> {
      enteredPublish.countDown();
      assertTrue(releasePublish.await(15, TimeUnit.SECONDS), "test must release the broker");
      return null;
    }).when(client).publish(anyString(), any());

    // Occupy the single worker with a hung broker.
    Future<Boolean> hung = callers.submit(() -> publisher.publishAck("m1", "msg-1", "1"));
    assertTrue(enteredPublish.await(5, TimeUnit.SECONDS), "worker must pick up the hung publish");

    // Fill the single queue slot.
    Future<Boolean> queued = callers.submit(() -> publisher.publishAck("m1", "msg-2", "2"));
    long deadline = System.currentTimeMillis() + 5_000;
    while (publisher.ackQueueSizeForTests() != 1 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }

    // Overflow must reject fast (well under the 10s publish timeout).
    long t0 = System.nanoTime();
    assertFalse(publisher.publishAck("m1", "msg-3", "3"), "overflow must drop fast, Edge resends");
    long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
    assertTrue(elapsedMs < 2_000, "rejection must be fast, was " + elapsedMs + "ms");
    assertTrue(metrics.getKafkaAckFailedTotal().count() > failedBefore, "rejection must be counted");

    // Drain: hung broker recovers, queued ACK still lands.
    releasePublish.countDown();
    assertTrue(hung.get(10, TimeUnit.SECONDS), "first ACK lands after recovery");
    assertTrue(queued.get(10, TimeUnit.SECONDS), "queued ACK lands after recovery");
  }
}
