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
 * -&gt; {@link TelemetryEnvelope} -&gt; Kafka producer (async, never blocking the Paho thread).
 *
 * <p><b>Reliable handoff (P2-1.1):</b> the Paho client runs with {@code setManualAcks(true)}, so
 * the broker keeps every QoS1 message until shore explicitly completes it. The MQTT acknowledgment
 * ({@code messageArrivedComplete}) fires <b>only after the Kafka send future succeeds</b>. When
 * the Kafka send fails, shore stays silent (metric + log) and the broker redelivers later, so
 * at-least-once holds end to end and downstream {@code UNIQUE(msg_id)} absorbs the duplicate.
 * Deliberately dropped poison payloads are still acknowledged: one bad message must neither kill
 * the subscriber thread nor loop on the broker forever. Every failure path is caught.
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

  public MqttIngestService(
      ShoreProperties properties,
      TelemetryMessageParser parser,
      TelemetryKafkaProducer producer,
      ShoreMetrics metrics) {
    this.properties = properties;
    this.parser = parser;
    this.producer = producer;
    this.metrics = metrics;
  }

  @PostConstruct
  public void start() {
    if (!properties.getMqtt().isEnabled()) {
      log.info("[Shore-MQTT] disabled by configuration (shore.mqtt.enabled=false), not subscribing");
      return;
    }
    running.set(true);
    starter = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "shore-mqtt-starter");
      t.setDaemon(true);
      return t;
    });
    // First attempt immediately; retry forever on the configured delay until subscribed.
    starter.execute(this::connectOnce);
  }

  @PreDestroy
  public void stop() {
    running.set(false);
    if (starter != null) {
      starter.shutdownNow();
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
    client = newClient;
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
      // Async handoff: never block the Paho thread waiting for Kafka, and never swallow
      // the outcome — the MQTT acknowledgment below is driven by the send future itself.
      sendFuture = producer.send(properties.getKafka().getRawTopic(), envelope);
    } catch (Exception e) {
      // Synchronous rejection (e.g. serializer/buffer failure): no ACK, broker will redeliver.
      metrics.kafkaProduceFailed();
      log.warn("[Shore-MQTT] Kafka handoff rejected, awaiting broker redelivery:"
          + " mmsi={}, msg_id={}, err={}",
          envelope.getMmsi(), envelope.getMsgId(), e.getMessage());
      return;
    }
    sendFuture.whenComplete((result, ex) -> {
      if (ex != null) {
        // Kafka-side failure is already counted/logged by TelemetryKafkaProducer.
        // Stay silent towards the broker: the message remains unacknowledged and
        // QoS1 redelivers it later.
        log.warn("[Shore-MQTT] Kafka send failed, awaiting broker redelivery:"
            + " mmsi={}, msg_id={}",
            envelope.getMmsi(), envelope.getMsgId());
        return;
      }
      completeDelivery(message);
    });
  }

  /**
   * Completes one MQTT delivery towards the broker. Called only for validated envelopes whose
   * Kafka send succeeded, or for deliberately dropped poison payloads — never for failed
   * handoffs. Package-visible for contract tests.
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
      // The message stays unacknowledged; the broker will redeliver it.
      log.warn("[Shore-MQTT] MQTT acknowledgment failed, awaiting redelivery:"
          + " id={}, err={}", message.getId(), e.getMessage());
    }
  }

  /** Test hook: injects the client used by {@link #completeDelivery}. */
  void setClientForTests(MqttClient client) {
    this.client = client;
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
