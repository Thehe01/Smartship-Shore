package com.smartship.shore.ingest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.observability.ShoreMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ACK publisher backpressure + liveness: one slow broker may hold at most 1
 * in-flight plus a bounded queue; overflow rejects fast (Edge resends) instead
 * of piling up threads, and repeated PUBACK timeouts rebuild the client.
 */
class MqttAckPublisherBoundedTest {

  private ShoreProperties properties;
  private ShoreMetrics metrics;
  private MqttAckPublisher publisher;
  private IMqttAsyncClient client;
  private ExecutorService callers;

  @BeforeEach
  void setUp() {
    properties = new ShoreProperties();
    properties.getMqtt().setAckTimeoutMs(10_000L);
    properties.getMqtt().setAckQueueCapacity(1);
    properties.getMqtt().setAckClientRebuildThreshold(2);
    metrics = new ShoreMetrics(new SimpleMeterRegistry());
    publisher = new MqttAckPublisher(properties, new ObjectMapper(), metrics);
    client = mock(IMqttAsyncClient.class);
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

  private IMqttDeliveryToken hangingToken(CountDownLatch entered, CountDownLatch release) throws Exception {
    IMqttDeliveryToken token = mock(IMqttDeliveryToken.class);
    doAnswer(inv -> {
      entered.countDown();
      assertTrue(release.await(15, TimeUnit.SECONDS), "test must release the broker");
      return null;
    }).when(token).waitForCompletion(anyLong());
    return token;
  }

  @Test
  @DisplayName("Queue full rejects fast instead of piling up threads")
  void queueFullRejectsFast() throws Exception {
    double failedBefore = metrics.getKafkaAckFailedTotal().count();
    CountDownLatch enteredPublish = new CountDownLatch(1);
    CountDownLatch releasePublish = new CountDownLatch(1);
    IMqttDeliveryToken hanging = hangingToken(enteredPublish, releasePublish);
    when(client.publish(anyString(), any())).thenReturn(hanging);

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

  @Test
  @DisplayName("Consecutive PUBACK timeouts rebuild the wedged client")
  void consecutiveTimeoutsRebuildClient() throws Exception {
    IMqttDeliveryToken timeoutToken = mock(IMqttDeliveryToken.class);
    doAnswer(inv -> {
      throw new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT);
    }).when(timeoutToken).waitForCompletion(anyLong());
    when(client.publish(anyString(), any())).thenReturn(timeoutToken);

    // Threshold is 2: two timeouts → client dropped + rebuild counted.
    assertFalse(publisher.publishAck("m1", "msg-1", "1"));
    assertFalse(publisher.publishAck("m1", "msg-2", "2"));
    assertTrue(metrics.getAckClientRebuildTotal().count() >= 1.0,
        "wedged client must be rebuilt after threshold timeouts");
  }
}
