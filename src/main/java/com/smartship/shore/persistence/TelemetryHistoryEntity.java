package com.smartship.shore.persistence;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One row of the generic shore history table {@code ship_telemetry_history}.
 *
 * <p>P2-1 keeps a single table for every telemetry type; per-type business columns travel
 * inside {@link #payloadJson} as the original envelope JSON.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TelemetryHistoryEntity {

  private Long id;

  /** Deterministic Edge message id; UNIQUE — the idempotency key. */
  private String msgId;

  private String mmsi;

  private String type;

  /** Original Edge business event time; null when the source row carried none. */
  private Instant eventTime;

  /** Edge network send time. */
  private Instant sentAt;

  /** Shore receive time (consumer wall clock); never null. */
  private Instant receivedAt;

  /** Full envelope JSON (envelope fields + business data). */
  private String payloadJson;

  private Integer kafkaPartition;

  private Long kafkaOffset;
}
