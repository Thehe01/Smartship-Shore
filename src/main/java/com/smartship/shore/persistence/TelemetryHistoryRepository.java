package com.smartship.shore.persistence;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** MyBatis history access; UNIQUE(msg_id) remains the concurrency-safe dedup mechanism. */
@Repository
public class TelemetryHistoryRepository {
  private final TelemetryHistoryMapper mapper;
  private final TransactionTemplate transaction;

  public TelemetryHistoryRepository(TelemetryHistoryMapper mapper, PlatformTransactionManager manager) {
    this.mapper = mapper;
    this.transaction = new TransactionTemplate(manager);
    this.transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
  }

  /** Insert one row, fill its generated id, and propagate DuplicateKeyException. */
  public TelemetryHistoryEntity insert(TelemetryHistoryEntity entity) {
    mapper.insert(entity);
    return entity;
  }

  public boolean existsByMsgId(String msgId) { return mapper.existsByMsgId(msgId); }

  /** Whole batch commits or rolls back before the consumer's per-record fallback. */
  public void insertBatch(List<TelemetryHistoryEntity> entities) {
    if (entities == null || entities.isEmpty()) return;
    transaction.executeWithoutResult(status -> {
      int rows = mapper.insertBatch(entities);
      if (rows != entities.size()) throw new IllegalStateException("Incomplete history batch");
    });
  }

  public Set<String> existingMsgIds(Collection<String> msgIds) {
    Set<String> ids = new HashSet<>(msgIds);
    return ids.isEmpty() ? Set.of() : new HashSet<>(mapper.existingMsgIds(ids));
  }

  public Optional<TelemetryHistoryEntity> findByMsgId(String msgId) {
    return Optional.ofNullable(mapper.findByMsgId(msgId));
  }
  public long countAll() { return mapper.countAll(); }
  public long countByMmsi(String mmsi) { return mapper.countByMmsi(mmsi); }
  public int deleteAll() { return mapper.deleteAll(); }
}
