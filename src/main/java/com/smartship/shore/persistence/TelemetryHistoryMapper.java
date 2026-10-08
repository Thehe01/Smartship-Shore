package com.smartship.shore.persistence;

import java.util.Collection;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface TelemetryHistoryMapper {
  int insert(TelemetryHistoryEntity entity);
  int insertBatch(@Param("entities") List<TelemetryHistoryEntity> entities);
  boolean existsByMsgId(@Param("msgId") String msgId);
  List<String> existingMsgIds(@Param("ids") Collection<String> ids);
  TelemetryHistoryEntity findByMsgId(@Param("msgId") String msgId);
  long countAll();
  long countByMmsi(@Param("mmsi") String mmsi);
  int deleteAll();
}
