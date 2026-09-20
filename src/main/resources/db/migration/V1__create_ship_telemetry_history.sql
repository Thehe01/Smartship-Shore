-- P2-1 shore history table: one generic table for every telemetry type.
-- Per-type business columns travel inside `payload` as JSON; no per-type tables in this phase.
--
-- Idempotency key: UNIQUE(msg_id). Edge msg_id is a deterministic SHA-256 over the business
-- natural key + event time (see Edge MqttPublisher), so at-least-once redelivery is absorbed here.
--
-- Time columns: event_time = Edge original `timestamp` (never overwritten by shore time),
-- sent_at = Edge `sent_at`, received_at = shore consumer wall clock.

CREATE TABLE IF NOT EXISTS ship_telemetry_history (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  msg_id VARCHAR(64) NOT NULL COMMENT 'deterministic Edge message id (SHA-256 hex)',
  mmsi VARCHAR(32) NOT NULL COMMENT 'ship identity, also the Kafka record key',
  type VARCHAR(64) NOT NULL COMMENT 'telemetry stream, e.g. nmea_gps/engine',
  event_time DATETIME(3) NULL COMMENT 'Edge original business event time',
  sent_at DATETIME(3) NULL COMMENT 'Edge network send time',
  received_at DATETIME(3) NOT NULL COMMENT 'shore receive time',
  payload JSON NOT NULL COMMENT 'full envelope JSON incl. business data',
  kafka_partition INT NULL,
  kafka_offset BIGINT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT uk_history_msg_id UNIQUE (msg_id),
  INDEX idx_history_mmsi (mmsi),
  INDEX idx_history_type (type),
  INDEX idx_history_event_time (event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='shore generic telemetry history (P2-1)';
