package com.smartship.shore.persistence;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

/**
 * JDBC persistence for {@code ship_telemetry_history} (schema owned by Flyway V1).
 *
 * <p>Idempotency note: {@code insert} lets a {@code UNIQUE(msg_id)} violation propagate as
 * {@code DuplicateKeyException} so the caller can treat "already stored" as success. An
 * explicit {@link #existsByMsgId} pre-check is offered for readability, but the unique
 * constraint — not the pre-check — is the correctness mechanism under races.
 */
@Repository
public class TelemetryHistoryRepository {

  private static final String INSERT_SQL =
      "INSERT INTO ship_telemetry_history"
          + " (msg_id, mmsi, type, event_time, sent_at, received_at, payload,"
          + " kafka_partition, kafka_offset)"
          + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String SELECT_BY_MSG_ID =
      "SELECT id, msg_id, mmsi, type, event_time, sent_at, received_at,"
          + " payload, kafka_partition, kafka_offset"
          + " FROM ship_telemetry_history WHERE msg_id = ?";

  private final JdbcTemplate jdbcTemplate;

  public TelemetryHistoryRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  /**
   * Inserts one row and fills the generated id.
   *
   * @throws org.springframework.dao.DuplicateKeyException when {@code msg_id} already exists
   */
  public TelemetryHistoryEntity insert(TelemetryHistoryEntity entity) {
    KeyHolder keys = new GeneratedKeyHolder();
    jdbcTemplate.update(
        connection -> {
          var ps = connection.prepareStatement(INSERT_SQL, new String[] {"id"});
          ps.setString(1, entity.getMsgId());
          ps.setString(2, entity.getMmsi());
          ps.setString(3, entity.getType());
          setInstant(ps, 4, entity.getEventTime());
          setInstant(ps, 5, entity.getSentAt());
          setInstant(ps, 6, entity.getReceivedAt());
          ps.setString(7, entity.getPayloadJson());
          if (entity.getKafkaPartition() == null) {
            ps.setNull(8, java.sql.Types.INTEGER);
          } else {
            ps.setInt(8, entity.getKafkaPartition());
          }
          if (entity.getKafkaOffset() == null) {
            ps.setNull(9, java.sql.Types.BIGINT);
          } else {
            ps.setLong(9, entity.getKafkaOffset());
          }
          return ps;
        },
        keys);
    Number id = keys.getKey();
    if (id != null) {
      entity.setId(id.longValue());
    }
    return entity;
  }

  public boolean existsByMsgId(String msgId) {
    Long n =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM ship_telemetry_history WHERE msg_id = ?", Long.class, msgId);
    return n != null && n > 0;
  }

  public Optional<TelemetryHistoryEntity> findByMsgId(String msgId) {
    try {
      return Optional.ofNullable(
          jdbcTemplate.queryForObject(SELECT_BY_MSG_ID, this::mapRow, msgId));
    } catch (EmptyResultDataAccessException e) {
      return Optional.empty();
    }
  }

  public long countAll() {
    Long n = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM ship_telemetry_history", Long.class);
    return n == null ? 0L : n;
  }

  public long countByMmsi(String mmsi) {
    Long n =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM ship_telemetry_history WHERE mmsi = ?", Long.class, mmsi);
    return n == null ? 0L : n;
  }

  public int deleteAll() {
    return jdbcTemplate.update("DELETE FROM ship_telemetry_history");
  }

  private TelemetryHistoryEntity mapRow(ResultSet rs, int rowNum) throws java.sql.SQLException {
    return TelemetryHistoryEntity.builder()
        .id(rs.getLong("id"))
        .msgId(rs.getString("msg_id"))
        .mmsi(rs.getString("mmsi"))
        .type(rs.getString("type"))
        .eventTime(toInstant(rs.getTimestamp("event_time")))
        .sentAt(toInstant(rs.getTimestamp("sent_at")))
        .receivedAt(toInstant(rs.getTimestamp("received_at")))
        .payloadJson(rs.getString("payload"))
        .kafkaPartition((Integer) rs.getObject("kafka_partition"))
        .kafkaOffset((Long) rs.getObject("kafka_offset"))
        .build();
  }

  private static void setInstant(java.sql.PreparedStatement ps, int index, Instant instant)
      throws java.sql.SQLException {
    if (instant == null) {
      ps.setNull(index, java.sql.Types.TIMESTAMP);
    } else {
      ps.setTimestamp(index, Timestamp.from(instant));
    }
  }

  private static Instant toInstant(Timestamp ts) {
    return ts == null ? null : ts.toInstant();
  }
}
