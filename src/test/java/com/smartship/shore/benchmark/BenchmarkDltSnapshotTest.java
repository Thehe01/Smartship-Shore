package com.smartship.shore.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit contract for the DLT snapshot helper (plain JUnit + Mockito, no broker):
 * multi-partition end offsets, partial progress, completion, the empty-topic case
 * and the bounded-timeout failure. Runs in the normal suite, not the benchmark profile.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class BenchmarkDltSnapshotTest {

  private static final TopicPartition TP0 = new TopicPartition("dlt", 0);
  private static final TopicPartition TP1 = new TopicPartition("dlt", 1);

  @Test
  @DisplayName("Partial positions across partitions are not done")
  void partialIsNotDone() {
    assertFalse(BenchmarkDltSnapshot.reachedSnapshotEnd(
        Map.of(TP0, 5L, TP1, 3L),
        Map.of(TP0, 5L, TP1, 2L)));
  }

  @Test
  @DisplayName("Every partition at/above its end offset completes")
  void allReachedCompletes() {
    assertTrue(BenchmarkDltSnapshot.reachedSnapshotEnd(
        Map.of(TP0, 5L, TP1, 3L),
        Map.of(TP0, 5L, TP1, 3L)));
    assertTrue(BenchmarkDltSnapshot.reachedSnapshotEnd(
        Map.of(TP0, 5L),
        Map.of(TP0, 9L)));
  }

  @Test
  @DisplayName("All-zero end offsets complete immediately with an empty count")
  void allZeroEndsAreEmpty() {
    assertTrue(BenchmarkDltSnapshot.reachedSnapshotEnd(
        Map.of(TP0, 0L, TP1, 0L),
        Map.of(TP0, 0L, TP1, 0L)));
    assertTrue(BenchmarkDltSnapshot.reachedSnapshotEnd(Map.of(), Map.of()));
  }

  @Test
  @DisplayName("Missing position for a partition is not done")
  void missingPositionIsNotDone() {
    assertFalse(BenchmarkDltSnapshot.reachedSnapshotEnd(
        Map.of(TP0, 1L),
        Map.of()));
  }

  private static KafkaConsumer<String, String> mockConsumer(long end0, long end1, long pos) {
    KafkaConsumer<String, String> consumer = mock(KafkaConsumer.class);
    Node node = new Node(0, "mock", 9092);
    doReturn(List.of(
        new PartitionInfo("dlt", 0, node, new Node[] {node}, new Node[] {node}),
        new PartitionInfo("dlt", 1, node, new Node[] {node}, new Node[] {node})))
        .when(consumer).partitionsFor("dlt");
    doReturn(Map.of(TP0, end0, TP1, end1)).when(consumer).endOffsets(anyCollection());
    doReturn(pos).when(consumer).position(TP0);
    doReturn(pos).when(consumer).position(TP1);
    doReturn(new ConsumerRecords<String, String>(Map.of()))
        .when(consumer).poll(any(Duration.class));
    return consumer;
  }

  @Test
  @DisplayName("Snapshot loop counts exactly what the captured ends hold")
  void loopCountsSnapshot() {
    // Ends {0:0, 1:0}: completes on the first check with zero records.
    assertEquals(0L, BenchmarkDltSnapshot.countSnapshot(
        mockConsumer(0L, 0L, 0L), "dlt", Duration.ofSeconds(5)));
  }

  @Test
  @DisplayName("Unreachable snapshot end fails loudly instead of defaulting to zero")
  void timeoutFails() {
    // Ends beyond positions that never advance: must time out, never return 0 silently.
    AssertionError e = assertThrows(AssertionError.class, () ->
        BenchmarkDltSnapshot.countSnapshot(
            mockConsumer(50L, 30L, 0L), "dlt", Duration.ofMillis(300)));
    assertTrue(e.getMessage().contains("timed out"));
  }
}
