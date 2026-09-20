package com.smartship.shore.ingest;

/**
 * A single MQTT payload that must not enter Kafka: unparseable JSON or missing
 * one of the required fields ({@code msg_id}, {@code mmsi}, {@code type}).
 */
public class InvalidTelemetryException extends RuntimeException {

  public InvalidTelemetryException(String message) {
    super(message);
  }

  public InvalidTelemetryException(String message, Throwable cause) {
    super(message, cause);
  }
}
