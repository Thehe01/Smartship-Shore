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
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
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

  private volatile MqttClient client;

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
      MqttClient c = ensureConnected(mqtt);
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
      // MqttClient.publish is blocking with no timeout of its own: time-box it on
      // the dedicated bounded executor so one slow broker cannot stall the serial
      // ingest callback NOR pile up unbounded common-pool threads. A timed-out call
      // may still land late as a duplicate ACK — harmless by design (Edge matches
      // on msg_id and resends are idempotent downstream). A full queue rejects fast
      // instead of queueing forever: same harmless duplicate cost.
      String topic = "ship/" + mmsi + "/ack";
      final Future<?> future;
      try {
        future = ackExecutor(mqtt).submit(() -> {
          try {
            c.publish(topic, message);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      } catch (RejectedExecutionException rejected) {
        metrics.kafkaAckFailed();
        log.warn("[Shore-ACK] queue full, dropping ACK fast (Edge will resend): mmsi={} msg_id={}",
            mmsi, msgId);
        return false;
      }
      try {
        future.get(mqtt.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        future.cancel(true);
        throw e;
      } catch (java.util.concurrent.TimeoutException e) {
        // Do NOT cancel the underlying publish: Paho has no safe interrupt, and a
        // late duplicate ACK is harmless. The single worker serializes the damage;
        // the timeout only frees the ingest callback thread.
        metrics.kafkaAckFailed();
        log.warn("[Shore-ACK] publish timed out, worker continues in background "
            + "(Edge will resend): mmsi={} msg_id={}", mmsi, msgId);
        return false;
      }
      metrics.kafkaAckPublished();
      return true;
    } catch (Exception e) {
      metrics.kafkaAckFailed();
      log.warn("[Shore-ACK] publish failed (Edge will resend): mmsi={} msg_id={} err={}",
          mmsi, msgId, e.getMessage());
      dropClient();
      return false;
    }
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
  void setClientForTests(MqttClient client) {
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

  private synchronized MqttClient ensureConnected(ShoreProperties.Mqtt mqtt) {
    MqttClient c = client;
    if (c != null && c.isConnected()) {
      return c;
    }
    dropClient();
    try {
      MqttClient fresh =
          new MqttClient(mqtt.getBrokerUrl(), mqtt.getClientId() + "-ack", new MemoryPersistence());
      MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(true);
      options.setCleanSession(true);
      options.setKeepAliveInterval(mqtt.getKeepAliveInterval());
      options.setConnectionTimeout((int) Math.min(mqtt.getConnectionTimeout(), 10));
      if (StringUtils.hasText(mqtt.getUsername())) {
        options.setUserName(mqtt.getUsername());
        options.setPassword(mqtt.getPassword() == null ? new char[0] : mqtt.getPassword().toCharArray());
      }
      fresh.connect(options);
      client = fresh;
      log.info("[Shore-ACK] connected: broker={}", mqtt.getBrokerUrl());
      return fresh;
    } catch (Exception e) {
      log.warn("[Shore-ACK] connect failed: {}", e.getMessage());
      return null;
    }
  }

  private synchronized void dropClient() {
    MqttClient c = client;
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
