package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.smartship.shore.config.KafkaConfig;
import com.smartship.shore.config.ShoreProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Topic durability contract: {@code acks=all} behind the Application ACK is only a
 * real quorum write when the topic has replicas and a coherent
 * {@code min.insync.replicas}. Illegal triples must fail fast at startup.
 */
class TopicDurabilityTest {

  @Test
  @DisplayName("Defaults stay single-broker bootable and carry min.insync=1")
  void defaultsStayLocalBootable() {
    ShoreProperties properties = new ShoreProperties();

    assertDoesNotThrow(() -> properties.getKafka().validateTopicDurability());

    KafkaConfig config = new KafkaConfig();
    NewTopic raw = config.shipTelemetryRawTopic(properties);
    assertEquals(1, raw.replicationFactor());
    assertEquals("1", raw.configs().get("min.insync.replicas"));
  }

  @Test
  @DisplayName("Production triple 3/3/2 flows into both topic beans")
  void productionTripleFlowsIntoBeans() {
    ShoreProperties properties = new ShoreProperties();
    properties.getKafka().setRawTopicReplicas(3);
    properties.getKafka().setDltTopicReplicas(3);
    properties.getKafka().setMinInsyncReplicas(2);

    assertDoesNotThrow(() -> properties.getKafka().validateTopicDurability());

    KafkaConfig config = new KafkaConfig();
    NewTopic raw = config.shipTelemetryRawTopic(properties);
    NewTopic dlt = config.shipTelemetryDltTopic(properties);
    assertEquals(3, raw.replicationFactor());
    assertEquals(3, dlt.replicationFactor());
    assertEquals("2", raw.configs().get("min.insync.replicas"));
    assertEquals("2", dlt.configs().get("min.insync.replicas"));
  }

  @Test
  @DisplayName("min.insync above available replicas fails fast")
  void minInsyncAboveReplicasFailsFast() {
    ShoreProperties properties = new ShoreProperties();
    properties.getKafka().setRawTopicReplicas(1);
    properties.getKafka().setDltTopicReplicas(1);
    properties.getKafka().setMinInsyncReplicas(2);

    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> properties.getKafka().validateTopicDurability());
    assertTrue(e.getMessage().contains("min.insync.replicas"));
  }

  @Test
  @DisplayName("Zero or negative factors fail fast")
  void nonPositiveFactorsFailFast() {
    ShoreProperties properties = new ShoreProperties();
    properties.getKafka().setRawTopicReplicas(0);

    assertThrows(IllegalStateException.class,
        () -> properties.getKafka().validateTopicDurability());
  }
}
