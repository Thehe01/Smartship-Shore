package com.smartship.shore.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The durability cross-check must flag under-replicated and misconfigured
 * topics, and stay silent when reality matches expectation.
 */
class TopicDurabilityVerifierTest {

  private static List<String> verify(List<TopicDurabilityVerifier.TopicInfo> observed) {
    return TopicDurabilityVerifier.verify(observed, name -> 3, 2);
  }

  @Test
  @DisplayName("Matching reality stays silent")
  void matchingIsSilent() {
    assertTrue(verify(List.of(
        new TopicDurabilityVerifier.TopicInfo("raw", 6, 3, "2"),
        new TopicDurabilityVerifier.TopicInfo("dlt", 6, 3, "2"))).isEmpty());
  }

  @Test
  @DisplayName("Single-copy legacy topic is flagged with runbook pointer")
  void singleCopyFlagged() {
    List<String> warnings = verify(List.of(
        new TopicDurabilityVerifier.TopicInfo("raw", 6, 1, "1")));
    assertEquals(2, warnings.size(), "RF and min.insync must each warn, got: " + warnings);
    assertTrue(warnings.stream().anyMatch(w -> w.contains("KAFKA_TOPIC_EXPANSION")));
  }

  @Test
  @DisplayName("Missing min.insync config degrades to the RF check only")
  void missingMinInsyncTolerated() {
    List<String> warnings = verify(List.of(
        new TopicDurabilityVerifier.TopicInfo("raw", 6, 3, null)));
    assertTrue(warnings.isEmpty());
  }
}
