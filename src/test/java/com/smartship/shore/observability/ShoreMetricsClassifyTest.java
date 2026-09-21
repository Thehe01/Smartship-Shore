package com.smartship.shore.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.smartship.shore.ingest.InvalidTelemetryException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

/**
 * Closed-bucket classification for the P2-2 retry/DLT metrics: the cause chain is walked
 * because real listener failures arrive wrapped in
 * {@code ListenerExecutionFailedException} — only the outer layer would mislabel
 * transient DB outages as poison.
 */
class ShoreMetricsClassifyTest {

  @Test
  @DisplayName("Bare transient DB failure → transient")
  void bareTransient() {
    assertEquals("transient",
        ShoreMetrics.classify(new TransientDataAccessResourceException("lock timeout")));
  }

  @Test
  @DisplayName("Wrapped transient DB failure → transient (the production shape)")
  void wrappedTransient() {
    assertEquals("transient",
        ShoreMetrics.classify(new ListenerExecutionFailedException("listener failed",
            new TransientDataAccessResourceException("connection reset"))));
  }

  @Test
  @DisplayName("Wrapped poison → poison")
  void wrappedPoison() {
    assertEquals("poison",
        ShoreMetrics.classify(new ListenerExecutionFailedException("listener failed",
            new InvalidTelemetryException("not an envelope"))));
  }

  @Test
  @DisplayName("Bare poison and null stay poison")
  void barePoisonAndNull() {
    assertEquals("poison",
        ShoreMetrics.classify(new InvalidTelemetryException("not an envelope")));
    assertEquals("poison", ShoreMetrics.classify(null));
    assertEquals("poison",
        ShoreMetrics.classify(new RuntimeException("plain bug")));
  }
}
