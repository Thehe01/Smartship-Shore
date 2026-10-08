package com.smartship.shore.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class MyBatisHistoryTest {
  private TelemetryHistoryRepository repository;

  @BeforeEach
  void setup() {
    var ds = new DriverManagerDataSource("jdbc:h2:mem:history_mapper_" + System.nanoTime()
        + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "");
    var jdbc = new JdbcTemplate(ds);
    jdbc.execute("""
        CREATE TABLE ship_telemetry_history (
          id BIGINT AUTO_INCREMENT PRIMARY KEY, msg_id VARCHAR(64) NOT NULL UNIQUE,
          mmsi VARCHAR(32) NOT NULL, type VARCHAR(64) NOT NULL, event_time TIMESTAMP(3),
          sent_at TIMESTAMP(3), received_at TIMESTAMP(3) NOT NULL, payload TEXT NOT NULL,
          kafka_partition INT, kafka_offset BIGINT)
        """);
    repository = MyBatisTestSupport.repository(jdbc);
  }

  @Test
  void generatedIdInstantNullAndPayloadRoundTrip() {
    var entity = entity("single");
    entity.setSentAt(Instant.parse("2026-10-08T16:28:58.123Z"));
    entity.setKafkaOffset(123456789012L);
    repository.insert(entity);
    assertNotNull(entity.getId());
    var stored = repository.findByMsgId("single").orElseThrow();
    assertEquals(entity, stored);
    assertNull(stored.getEventTime());
    assertNull(stored.getKafkaPartition());
    assertTrue(repository.findByMsgId("missing").isEmpty());
    assertEquals(1, repository.countByMmsi("413999999"));
    assertEquals(Set.of("single"), repository.existingMsgIds(List.of("single", "single", "missing")));
    assertEquals(Set.of(), repository.existingMsgIds(List.of()));
  }

  @Test
  void duplicateBatchLeavesNoPartialRowsAndPerRecordFallbackCanCommit() {
    repository.insert(entity("existing"));
    assertThrows(DuplicateKeyException.class,
        () -> repository.insertBatch(List.of(entity("new"), entity("existing"))));
    assertEquals(1, repository.countAll());
    assertFalse(repository.existsByMsgId("new"));
    repository.insert(entity("new"));
    assertThrows(DuplicateKeyException.class, () -> repository.insert(entity("existing")));
    assertEquals(2, repository.countAll());
  }

  @Test
  void successfulBatchAndEmptyBatchRetainRepositoryContract() {
    repository.insertBatch(List.of());
    repository.insertBatch(null);
    repository.insertBatch(List.of(entity("a"), entity("b")));
    assertEquals(2, repository.countAll());
    assertEquals(Set.of("a", "b"), repository.existingMsgIds(List.of("a", "b")));
    assertEquals(2, repository.deleteAll());
    assertEquals(0, repository.countAll());
  }

  private TelemetryHistoryEntity entity(String id) {
    return TelemetryHistoryEntity.builder().msgId(id).mmsi("413999999").type("engine")
        .receivedAt(Instant.parse("2026-10-08T16:28:58.456Z"))
        .payloadJson("{\"rpm\":1500,\"note\":\"中文\"}").build();
  }
}
