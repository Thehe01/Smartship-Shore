package com.smartship.shore.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.observability.ShoreMetrics;
import jakarta.annotation.PreDestroy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Publishes Application ACKs ({@code ship/{mmsi}/ack}) after Kafka {@code acks=all}.
 *
 * <p>Contract, mirrored with the Edge {@code UploadAckTracker}:
 * <ul>
 *   <li>one ACK per Kafka-committed envelope: {@code {msg_id, seq, status}},
 *   with {@code status = "KAFKA_COMMITTED"};</li>
 *   <li>QoS 1: an ACK may be lost or duplicated, never invented — Edge resends on
 *   timeout and matches on {@code msg_id} (plus {@code seq} when present);</li>
 *   <li>best-effort and bounded: failures/timeouts are logged and counted, never
 *   thrown — the Kafka record is already durable, and a lost ACK only costs Edge
 *   one resend absorbed by {@code UNIQUE(msg_id)};</li>
 *   <li>never called on the Kafka-failure path: no ACK, no MQTT acknowledgment,
 *   the existing disconnect + QoS1 redelivery path runs unchanged.</li>
 * </ul>
 *
 * <p>Uses its own Paho client (never the subscriber's): publishing from inside the
 * subscriber callback would tangle manual ACKs with outbound inflight.
 *
 * <p>Liveness: the blocking {@code MqttClient.publish} is deliberately NOT used.
 * {@code MqttAsyncClient.publish} returns immediately and
 * {@code IMqttToken.waitForCompletion(timeout)} bounds the wait inside Paho itself,
 * so the single worker can never wedge forever on one slow broker. After
 * {@code ackClientRebuildThreshold} consecutive PUBACK timeouts the client is
 * dropped and rebuilt on the next call (a late duplicate ACK stays harmless:
 * Edge matches on {@code msg_id}).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqttAckPublisher {

  /** Application ACK status: the envelope is durable in Kafka (acks=all). */
  public static final String STATUS_KAFKA_COMMITTED = "KAFKA_COMMITTED";

  private final ShoreProperties properties;
  private final ObjectMapper objectMapper;
  private final ShoreMetrics metrics;

  private volatile IMqttAsyncClient client;
  private final java.util.concurrent.atomic.AtomicInteger consecutiveTimeouts =
      new java.util.concurrent.atomic.AtomicInteger(0);

  /**
   * Dedicated bounded ACK publisher: a single worker so outbound publishes never
   * contend with the subscriber callback, and a bounded queue so one slow broker
   * piles up at most {@code 1 + ackQueueCapacity} tasks instead of an unbounded
   * common-pool backlog. Overflow is rejected fast (counted, Edge resends).
   */
  private volatile ExecutorService ackExecutor;

  /**
   * Publishes one Application ACK. Never throws: returns {@code true} on PUBACK,
   * {@code false} on any failure, timeout, rejection, or disabled configuration.
   */
  public boolean publishAck(String mmsi, String msgId, String seq) {
    ShoreProperties.Mqtt mqtt = properties.getMqtt();
    if (!mqtt.isEnabled() || !mqtt.isAckEnabled()) {
      return false;
    }
    if (!StringUtils.hasText(mmsi) || !StringUtils.hasText(msgId)) {
      log.warn("[Shore-ACK] refusing ACK with blank mmsi/msg_id");
      return false;
    }
    try {
      IMqttAsyncClient c = ensureConnected(mqtt);
      if (c == null) {
        metrics.kafkaAckFailed();
        return false;
      }
      Map<String, Object> ack = new LinkedHashMap<>();
      ack.put("msg_id", msgId);
      if (StringUtils.hasText(seq)) {
        ack.put("seq", seq);
      }
      ack.put("status", STATUS_KAFKA_COMMITTED);
      byte[] payload = objectMapper.writeValueAsBytes(ack);
      MqttMessage message = new MqttMessage(payload);
      message.setQos(1);
      // Async publish + token timeout on the dedicated bounded worker: the wait is
      // bounded inside Paho itself (no permanent wedge), the caller wait only frees
      // the serial ingest callback, and a full queue rejects fast. A timed-out or
      // late duplicate ACK is harmless by design (Edge matches on msg_id, resends
      // are idempotent downstream).
      String topic = "ship/" + mmsi + "/ack";
      final Future<Boolean> future;
      try {
        future = ackExecutor(mqtt).submit(() -> publishAndWait(c, topic, message, mmsi, msgId));
      } catch (RejectedExecutionException rejected) {
        metrics.kafkaAckFailed();
        log.warn("[Shore-ACK] queue full, dropping ACK fast (Edge will resend): mmsi={} msg_id={}",
            mmsi, msgId);
        return false;
      }
      try {
        return future.get(mqtt.getAckTimeoutMs() + 1000L, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        future.cancel(true);
        throw e;
      } catch (java.util.concurrent.TimeoutException e) {
        // Practically unreachable: the worker bounds itself via waitForCompletion.
        // Kept as a second fence so the ingest callback can never park forever.
        metrics.kafkaAckFailed();
        log.warn("[Shore-ACK] worker overran its own timeout (Edge will resend): mmsi={} msg_id={}",
            mmsi, msgId);
        return false;
      } catch (java.util.concurrent.ExecutionException e) {
        // publishAndWait never throws (all paths return boolean), defensive only.
        metrics.kafkaAckFailed();
        log.warn("[Shore-ACK] worker failed (Edge will resend): mmsi={} msg_id={} err={}",
            mmsi, msgId, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        return false;
      }
    } catch (Exception e) {
      metrics.kafkaAckFailed();
      log.warn("[Shore-ACK] publish failed (Edge will resend): mmsi={} msg_id={} err={}",
          mmsi, msgId, e.getMessage());
      dropClient();
      return false;
    }
  }

  /**
   * One async publish fenced by the token timeout. Never throws: every outcome maps
   * to a boolean, consecutive PUBACK timeouts trigger a client rebuild so a wedged
   * connection heals itself instead of stalling the worker forever.
   */
  private boolean publishAndWait(IMqttAsyncClient c, String topic, MqttMessage message,
      String mmsi, String msgId) {
    ShoreProperties.Mqtt mqtt = properties.getMqtt();
    final IMqttToken token;
    try {
      token = c.publish(topic, message);
    } catch (Exception e) {
      metrics.kafkaAckFailed();
      log.warn("[Shore-ACK] async submit failed (Edge will resend): mmsi={} msg_id={} err={}",
          mmsi, msgId, e.getMessage());
      dropClient();
      return false;
    }
    try {
      token.waitForCompletion(mqtt.getAckTimeoutMs());
    } catch (MqttException e) {
      metrics.kafkaAckFailed();
      if (e.getReasonCode() == MqttException.REASON_CODE_CLIENT_TIMEOUT) {
        int n = consecutiveTimeouts.incrementAndGet();
        log.warn("[Shore-ACK] PUBACK timeout {}/{} (Edge will resend): mmsi={} msg_id={}",
            n, Math.max(1, mqtt.getAckClientRebuildThreshold()), mmsi, msgId);
        if (n >= Math.max(1, mqtt.getAckClientRebuildThreshold())) {
          consecutiveTimeouts.set(0);
          metrics.recordAckClientRebuild();
          dropClient();
          log.warn("[Shore-ACK] rebuilding wedged ACK client after {} consecutive timeouts",
              mqtt.getAckClientRebuildThreshold());
        }
      } else {
        consecutiveTimeouts.set(0);
        log.warn("[Shore-ACK] publish failed (Edge will resend): mmsi={} msg_id={} err={}",
            mmsi, msgId, e.getMessage());
        dropClient();
      }
      return false;
    }
    consecutiveTimeouts.set(0);
    metrics.kafkaAckPublished();
    return true;
  }

  /** Lazily builds the single-worker bounded ACK executor. */
  private ExecutorService ackExecutor(ShoreProperties.Mqtt mqtt) {
    ExecutorService exec = ackExecutor;
    if (exec == null || exec.isShutdown()) {
      synchronized (this) {
        exec = ackExecutor;
        if (exec == null || exec.isShutdown()) {
          int capacity = Math.max(1, mqtt.getAckQueueCapacity());
          AtomicInteger counter = new AtomicInteger(1);
          exec = new ThreadPoolExecutor(
              1, 1, 0L, TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<>(capacity),
              r -> {
                Thread t = new Thread(r, "shore-ack-pub-" + counter.getAndIncrement());
                t.setDaemon(true);
                return t;
              },
              new ThreadPoolExecutor.AbortPolicy());
          ackExecutor = exec;
        }
      }
    }
    return exec;
  }

  /** Stops the ACK worker; in-flight blocking publishes are left to finish. */
  @PreDestroy
  void shutdownAckExecutor() {
    ExecutorService exec = ackExecutor;
    ackExecutor = null;
    if (exec != null) {
      exec.shutdown();
    }
  }

  /** Test hook: injects the client used for publishing. */
  void setClientForTests(IMqttAsyncClient client) {
    this.client = client;
  }

  /** Test hook: current bounded-queue depth, or -1 when the worker is not built yet. */
  int ackQueueSizeForTests() {
    ExecutorService exec = ackExecutor;
    if (exec instanceof ThreadPoolExecutor tpe) {
      return tpe.getQueue().size();
    }
    return -1;
  }

  private synchronized IMqttAsyncClient ensureConnected(ShoreProperties.Mqtt mqtt) {
    IMqttAsyncClient c = client;
    if (c != null && c.isConnected()) {
      return c;
    }
    dropClient();
    try {
      MqttAsyncClient fresh =
          new MqttAsyncClient(mqtt.getBrokerUrl(), mqtt.getClientId() + "-ack", new MemoryPersistence());
      MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(true);
      options.setCleanSession(true);
      options.setKeepAliveInterval(mqtt.getKeepAliveInterval());
      options.setConnectionTimeout((int) Math.min(mqtt.getConnectionTimeout(), 10));
      if (StringUtils.hasText(mqtt.getUsername())) {
        options.setUserName(mqtt.getUsername());
        options.setPassword(mqtt.getPassword() == null ? new char[0] : mqtt.getPassword().toCharArray());
      }
      fresh.connect(options)
          .waitForCompletion(TimeUnit.SECONDS.toMillis(Math.max(1, mqtt.getConnectionTimeout())));
      client = fresh;
      log.info("[Shore-ACK] connected: broker={}", mqtt.getBrokerUrl());
      return fresh;
    } catch (Exception e) {
      log.warn("[Shore-ACK] connect failed: {}", e.getMessage());
      return null;
    }
  }

  private synchronized void dropClient() {
    IMqttAsyncClient c = client;
    client = null;
    if (c != null) {
      try {
        c.close();
      } catch (Exception ignored) {
        // Best effort.
      }
    }
  }
}
