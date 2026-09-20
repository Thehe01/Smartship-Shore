package com.smartship.shore.ingest;

import com.smartship.shore.config.ShoreProperties;
import com.smartship.shore.kafka.TelemetryKafkaProducer;
import com.smartship.shore.model.TelemetryEnvelope;
import com.smartship.shore.observability.ShoreMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * Subscribes to the real Edge topics ({@code zncb/+/+}) and forwards validated envelopes to Kafka.
 *
 * <p>Pipeline per MQTT delivery: raw payload -&gt; JSON parse -&gt; required-field validation
 * -&gt; {@link TelemetryEnvelope} -&gt; Kafka producer with a <b>bounded synchronous
 * handoff</b> (P2-1.2).
 *
 * <p><b>Reliable, ordered handoff (P2-1.2):</b> the Paho client runs with
 * {@code setManualAcks(true)}, so the broker keeps every QoS1 message until shore explicitly
 * completes it. Inside {@code messageArrived} shore waits for the Kafka send future with a
 * configured timeout ({@code shore.mqtt.kafka-handoff-timeout-ms}, default 5s — never
 * infinite). The Paho callback thread is serial, so this bounded wait additionally guarantees
 * MQTT acknowledgments leave in arrival order. On Kafka success the delivery is completed via
 * {@code messageArrivedComplete}. On Kafka failure or timeout shore stays silent (metric +
 * log, no ACK) and <b>actively drops the current MQTT connection</b>; the durable session
 * ({@code cleanSession=false}, stable client id) makes the broker redeliver the original QoS1
 * message on reconnect, while downstream {@code UNIQUE(msg_id)} absorbs any duplicate.
 * Deliberately dropped poison payloads are still acknowledged: one bad message must neither
 * kill the subscriber thread nor loop on the broker forever. Every failure path is caught.
 *
 * <p>Startup never fails because the broker is down: a background task retries connect +
 * subscribe until it succeeds, and Paho automatic-reconnect plus re-subscribe covers later drops.
 */
@Slf4j
@Service
public class MqttIngestService implements MqttCallbackExtended, IMqttMessageListener {

  private final ShoreProperties properties;
  private final TelemetryMessageParser parser;
  private final TelemetryKafkaProducer producer;
  private final ShoreMetrics metrics;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile MqttClient client;
  private ScheduledExecutorService starter;
  /** Test observability hook: notified after every successful broker acknowledgment. */
  private volatile AckListener ackListener;
  /** Coalesces forced reconnects when consecutive handoffs fail. */
  private final AtomicBoolean redeliveryReconnectPending = new AtomicBoolean(false);

  public MqttIngestService(
      ShoreProperties properties,
      TelemetryMessageParser parser,
      TelemetryKafkaProducer producer,
      ShoreMetrics metrics) {
    this.properties = properties;
    this.parser = parser;
    this.producer = producer;
    this.metrics = metrics;
    // Eager executor so the redelivery-reconnect path never depends on start() timing.
    this.starter = newStarter();
  }

  private static ScheduledExecutorService newStarter() {
    return Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "shore-mqtt-starter");
      t.setDaemon(true);
      return t;
    });
  }

  @PostConstruct
  public void start() {
    if (!properties.getMqtt().isEnabled()) {
      log.info("[Shore-MQTT] disabled by configuration (shore.mqtt.enabled=false), not subscribing");
      return;
    }
    running.set(true);
    if (starter == null || starter.isShutdown()) {
      starter = newStarter();
    }
    // First attempt immediately; retry forever on the configured delay until subscribed.
    starter.execute(this::connectOnce);
  }

  @PreDestroy
  public void stop() {
    running.set(false);
    ScheduledExecutorService exec = starter;
    starter = null;
    if (exec != null) {
      exec.shutdownNow();
    }
    disconnectQuietly();
  }

  private void connectOnce() {
    if (!running.get()) {
      return;
    }
    try {
      connectAndSubscribe();
    } catch (Exception e) {
      log.warn(
          "[Shore-MQTT] broker unreachable ({}), retry in {} ms",
          e.getMessage(), properties.getMqtt().getConnectRetryDelayMs());
      if (running.get() && starter != null && !starter.isShutdown()) {
        starter.schedule(this::connectOnce, properties.getMqtt().getConnectRetryDelayMs(),
            TimeUnit.MILLISECONDS);
      }
    }
  }

  synchronized void connectAndSubscribe() throws Exception {
    ShoreProperties.Mqtt mqtt = properties.getMqtt();
    disconnectQuietly();

    MqttClient newClient = new MqttClient(mqtt.getBrokerUrl(), mqtt.getClientId(), new MemoryPersistence());
    // Manual acknowledgments: nothing is ACKed to the broker until the Kafka handoff succeeds
    // (see messageArrived). Must be set before connect.
    newClient.setManualAcks(true);
    // Publish the reference BEFORE connect: with a durable session the broker may redeliver
    // unacked QoS1 immediately after CONNACK, and completeDelivery() must already see a client
    // instead of a null reference.
    client = newClient;
    try {
      MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(mqtt.isAutoReconnect());
      options.setCleanSession(mqtt.isCleanSession());
      options.setKeepAliveInterval(mqtt.getKeepAliveInterval());
      options.setConnectionTimeout(mqtt.getConnectionTimeout());
      if (StringUtils.hasText(mqtt.getUsername())) {
        options.setUserName(mqtt.getUsername());
        options.setPassword(mqtt.getPassword() == null ? new char[0] : mqtt.getPassword().toCharArray());
      }
      newClient.setCallback(this);
      newClient.connect(options);
      newClient.subscribe(mqtt.getTopics(), mqtt.getQos(), this);
    } catch (Exception e) {
      // Never leak a half-connected client: the next attempt rebuilds from scratch.
      if (client == newClient) {
        client = null;
      }
      try {
        newClient.close();
      } catch (Exception closeNoise) {
        log.debug("[Shore-MQTT] client close noise: {}", closeNoise.getMessage());
      }
      throw e;
    }
    log.info("[Shore-MQTT] subscribed: broker={}, filter={}, qos={}",
        mqtt.getBrokerUrl(), mqtt.getTopics(), mqtt.getQos());
  }

  private synchronized void disconnectQuietly() {
    if (client != null) {
      try {
        if (client.isConnected()) {
          client.disconnect();
        }
        client.close();
      } catch (Exception e) {
        log.debug("[Shore-MQTT] disconnect noise: {}", e.getMessage());
      } finally {
        client = null;
      }
    }
  }

  // ------------------------------------------------------------------
  // MqttCallbackExtended: connection events (Paho thread — never throw)
  // ------------------------------------------------------------------

  @Override
  public void connectComplete(boolean reconnect, String serverURI) {
    log.info("[Shore-MQTT] {}: {}", reconnect ? "reconnected" : "connected", serverURI);
    if (reconnect) {
      try {
        ShoreProperties.Mqtt mqtt = properties.getMqtt();
        MqttClient c = client;
        if (c != null && c.isConnected()) {
          c.subscribe(mqtt.getTopics(), mqtt.getQos(), this);
          log.info("[Shore-MQTT] re-subscribed after reconnect: {}", mqtt.getTopics());
        }
      } catch (Exception e) {
        log.warn("[Shore-MQTT] re-subscribe failed after reconnect: {}", e.getMessage());
      }
    }
  }

  @Override
  public void connectionLost(Throwable cause) {
    log.warn("[Shore-MQTT] connection lost: {}",
        cause != null ? cause.getMessage() : "unknown");
  }

  @Override
  public void deliveryComplete(org.eclipse.paho.client.mqttv3.IMqttDeliveryToken token) {
    // Subscriber never publishes; nothing to do.
  }

  // ------------------------------------------------------------------
  // IMqttMessageListener: one Edge delivery (Paho thread — never throw).
  // This single implementation also satisfies MqttCallbackExtended, whose
  // messageArrived has the identical signature.
  // ------------------------------------------------------------------

  @Override
  public void messageArrived(String topic, MqttMessage message) {
    metrics.mqttReceived();
    final byte[] payload = message.getPayload();
    final TelemetryEnvelope envelope;
    try {
      envelope = parser.parse(payload);
    } catch (InvalidTelemetryException e) {
      metrics.mqttInvalid();
      log.warn("[Shore-MQTT] dropped invalid payload: topic={}, err={}, raw={}",
          topic, e.getMessage(), preview(payload));
      // Deliberate drop: acknowledge so the poison message does not redeliver forever.
      completeDelivery(message);
      return;
    } catch (Exception e) {
      // Defensive: the subscriber thread must survive even unexpected parser failures.
      metrics.mqttInvalid();
      log.warn("[Shore-MQTT] dropped payload on unexpected parse error: topic={}, err={}",
          topic, e.toString());
      completeDelivery(message);
      return;
    }
    final java.util.concurrent.CompletableFuture<?> sendFuture;
    try {
      sendFuture = producer.send(properties.getKafka().getRawTopic(), envelope);
    } catch (Exception e) {
      // Synchronous rejection (e.g. serializer/buffer failure): no ACK, broker will redeliver.
      metrics.kafkaProduceFailed();
      log.warn("[Shore-MQTT] Kafka handoff rejected, forcing redelivery:"
          + " mmsi={}, msg_id={}, err={}",
          envelope.getMmsi(), envelope.getMsgId(), e.getMessage());
      triggerRedeliveryReconnect("sync-reject:" + e.getMessage());
      return;
    }
    // Bounded synchronous handoff on the (serial) Paho callback thread: the wait always ends
    // — success, broker failure, or timeout — so acknowledgments leave in arrival order and
    // the callback can never block forever.
    try {
      sendFuture.get(properties.getMqtt().getKafkaHandoffTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      handoffFailed(envelope, "interrupted");
      return;
    } catch (java.util.concurrent.TimeoutException e) {
      handoffFailed(envelope, "kafka-ack-timeout");
      return;
    } catch (java.util.concurrent.ExecutionException e) {
      // Async Kafka failure is already counted/logged by TelemetryKafkaProducer; just
      // hold the MQTT acknowledgment here so QoS1 redelivers the original message.
      handoffFailed(envelope, e.getCause() != null ? e.getCause().toString() : e.toString());
      return;
    }
    completeDelivery(message);
  }

  /**
   * Failed-handoff path: no MQTT acknowledgment, then force a reconnect so the durable
   * session makes the broker redeliver the original QoS1 message. Consumption of later
   * messages is not halted — each delivery still runs the same bounded handoff.
   */
  private void handoffFailed(TelemetryEnvelope envelope, String cause) {
    log.warn("[Shore-MQTT] Kafka handoff failed, holding MQTT for broker redelivery:"
        + " mmsi={}, msg_id={}, cause={}",
        envelope.getMmsi(), envelope.getMsgId(), cause);
    triggerRedeliveryReconnect(cause);
  }

  /**
   * Drops the current MQTT connection on a background thread (never the Paho callback
   * thread, which must not block on disconnect) and reconnects immediately with the same
   * client id and {@code cleanSession=false}, so the broker redelivers unacknowledged QoS1.
   * Concurrent failures coalesce into a single reconnect.
   */
  private void triggerRedeliveryReconnect(String reason) {
    ScheduledExecutorService exec = starter;
    if (exec == null || exec.isShutdown()) {
      return;
    }
    if (!redeliveryReconnectPending.compareAndSet(false, true)) {
      log.debug("[Shore-MQTT] redelivery reconnect already pending");
      return;
    }
    exec.execute(() -> {
      try {
        log.warn("[Shore-MQTT] forcing reconnect to trigger broker redelivery: {}", reason);
        disconnectForciblyQuietly();
      } finally {
        redeliveryReconnectPending.set(false);
      }
      // connectOnce re-checks running itself; after stop() it returns immediately.
      connectOnce();
    });
  }

  /** Immediate, non-quiescing disconnect: safe from any thread, never blocks the callback. */
  private synchronized void disconnectForciblyQuietly() {
    MqttClient c = client;
    client = null;
    if (c != null) {
      try {
        c.disconnectForcibly(1000);
      } catch (Exception e) {
        log.debug("[Shore-MQTT] forced disconnect noise: {}", e.getMessage());
      }
      try {
        c.close();
      } catch (Exception e) {
        log.debug("[Shore-MQTT] client close noise: {}", e.getMessage());
      }
    }
  }

  /**
   * Completes one MQTT delivery towards the broker. Called only for validated envelopes whose
   * Kafka send succeeded, or for deliberately dropped poison payloads — never for failed
   * handoffs. A failed acknowledgment is NOT success: it forces the same redelivery
   * reconnect as any other failure exit. Package-visible for contract tests.
   */
  void completeDelivery(MqttMessage message) {
    MqttClient c = client;
    if (c == null) {
      log.warn("[Shore-MQTT] no client to acknowledge message id={}", message.getId());
      return;
    }
    try {
      if (message.getQos() > 0) {
        c.messageArrivedComplete(message.getId(), message.getQos());
        AckListener listener = ackListener;
        if (listener != null) {
          listener.onAck(message.getId(), message.getQos());
        }
      }
    } catch (Exception e) {
      // The delivery stays unconfirmed AND the connection is recycled, so the durable
      // session redelivers it: Kafka failure / timeout / MQTT ACK failure all converge
      // on forced reconnect + broker redelivery of the original QoS1.
      log.warn("[Shore-MQTT] MQTT acknowledgment failed, forcing redelivery:"
          + " id={}, err={}", message.getId(), e.getMessage());
      triggerRedeliveryReconnect("mqtt-ack-failure:" + e.getMessage());
    }
  }

  /** Test hook: injects the client used by {@link #completeDelivery}. */
  void setClientForTests(MqttClient client) {
    this.client = client;
  }

  /** Test hook: observes the currently published client reference. */
  MqttClient getClientForTests() {
    return client;
  }

  /** Acknowledgment observer, used by handoff contract tests. */
  @FunctionalInterface
  public interface AckListener {
    void onAck(int messageId, int qos);
  }

  /**
   * Test hook: observes successful broker acknowledgments without interfering with them.
   * The live subscription and the real acknowledgment path stay untouched.
   */
  public void setAckListener(AckListener ackListener) {
    this.ackListener = ackListener;
  }

  /** True once the subscriber holds a live broker connection (used by E2E gates). */
  public boolean isReady() {
    MqttClient c = client;
    return c != null && c.isConnected();
  }

  private static String preview(byte[] payload) {
    if (payload == null) {
      return "null";
    }
    String s = new String(payload, StandardCharsets.UTF_8);
    return s.length() <= 300 ? s : s.substring(0, 300) + "...";
  }
}
