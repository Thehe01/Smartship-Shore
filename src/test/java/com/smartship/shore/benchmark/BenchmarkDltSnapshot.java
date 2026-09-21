package com.smartship.shore.benchmark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Deterministic DLT snapshot reader (benchmark-only helper).
 *
 * <p>Why not {@code subscribe()} + "empty poll means empty": the first polls of a fresh
 * subscriber routinely return nothing while partition assignment is still in flight,
 * so an empty poll proves nothing and a {@code DLT == 0} verdict built on it is a
 * false negative waiting to happen.
 *
 * <p>Instead this helper pins the topic state first and then reads exactly that
 * snapshot:
 * <ol>
 *   <li>resolve every partition of the topic,</li>
 *   <li>{@code assign()} them explicitly (no group rebalance involved),</li>
 *   <li>{@code seekToBeginning()} + capture {@code endOffsets()} once — later writes
 *   can never extend this snapshot, so the loop always terminates,</li>
 *   <li>poll until every partition's position reaches its captured end, then return
 *   the exact record count.</li>
 * </ol>
 * A bounded timeout guards the loop; expiry fails the benchmark loudly instead of
 * defaulting the DLT to zero.
 */
public final class BenchmarkDltSnapshot {

  private BenchmarkDltSnapshot() {
  }

  /**
   * Counts every record in the topic snapshot visible right now.
   *
   * <p>Two strictness rules keep a {@code 0} verdict honest:
   * <ul>
   *   <li>an empty partition list fails instead of returning {@code 0}, so a missing
   *   topic can never be confused with an empty DLT;</li>
   *   <li>only records with {@code offset < captured end} count: anything appended
   *   after the snapshot was pinned belongs to a later world, not to this verdict.</li>
   * </ul>
   *
   * @param consumer plain consumer (ownership stays with the caller)
   * @param topic DLT topic name
   * @param timeout upper bound for reaching the snapshot end
   * @return exact record count inside the captured snapshot
   * @throws AssertionError when the topic has no partitions or the snapshot end
   *     is not reached in time
   */
  public static long countSnapshot(KafkaConsumer<String, String> consumer, String topic,
      Duration timeout) {
    List<TopicPartition> partitions = new ArrayList<>();
    consumer.partitionsFor(topic).forEach(info ->
        partitions.add(new TopicPartition(topic, info.partition())));
    if (partitions.isEmpty()) {
      throw new AssertionError("DLT topic has no partitions (missing topic?): " + topic);
    }
    consumer.assign(partitions);
    consumer.seekToBeginning(partitions);
    Map<TopicPartition, Long> endOffsets = new LinkedHashMap<>(consumer.endOffsets(partitions));

    long total = 0;
    Map<TopicPartition, Long> positions = new LinkedHashMap<>();
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
      for (TopicPartition tp : partitions) {
        long end = endOffsets.getOrDefault(tp, 0L);
        for (ConsumerRecord<String, String> record : records.records(tp)) {
          if (record.offset() < end) {
            total++;
          }
        }
        positions.put(tp, consumer.position(tp));
      }
      if (reachedSnapshotEnd(endOffsets, positions)) {
        return total;
      }
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for DLT snapshot end of topic "
            + topic + " (endOffsets=" + endOffsets + ", positions=" + positions + ")");
      }
    }
  }

  /**
   * Pure snapshot-end predicate: every captured partition has been read up to (at
   * least) its captured end offset. An empty topic (all ends zero, positions zero)
   * completes immediately with an empty count.
   */
  static boolean reachedSnapshotEnd(Map<TopicPartition, Long> endOffsets,
      Map<TopicPartition, Long> positions) {
    for (Map.Entry<TopicPartition, Long> e : endOffsets.entrySet()) {
      Long pos = positions.get(e.getKey());
      if (pos == null || pos < e.getValue()) {
        return false;
      }
    }
    return true;
  }
}
