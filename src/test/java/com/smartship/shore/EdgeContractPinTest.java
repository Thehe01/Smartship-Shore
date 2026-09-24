package com.smartship.shore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 线路协议契约 <b>edge-mqtt-contract-v1</b> 的岸仓冻结端。
 *
 * <p>与边仓 {@code MqttPublisherContractTest} pin 住同一组向量。本测试只断言冻结
 * 常量，不重新实现算法：边仓改算法会先让它自己的契约测试变红，而任何“只改一边就
 * 想上线”的协议 bump 必须同步更新这两个文件。单边绿不代表兼容。
 */
class EdgeContractPinTest {

  /** 与边仓 MqttPublisherContractTest.FROZEN_MSG_ID_V1 逐字相同。 */
  private static final String FROZEN_MSG_ID_V1 =
      "7b20b18ae89960f8f6cd04a198af268de85b67ca72e3adc4b7cfd224c960bc69";
  /** 与边仓 MqttPublisherContractTest.FROZEN_TOPIC_V1 逐字相同。 */
  private static final String FROZEN_TOPIC_V1 = "zncb/413999999/nmea_gps";

  @Test
  @DisplayName("契约v1：镜像算法输出与冻结 msg_id 一致")
  void mirroredAlgorithmMatchesFrozenVector() {
    assertEquals(FROZEN_MSG_ID_V1,
        EdgeFixtures.edgeStyleMsgId("413999999", "nmea_gps", "1001", "2026-09-19T10:00:00"));
  }

  @Test
  @DisplayName("契约v1：主题格式与黄金载荷一致")
  void topicAndGoldenPayloadMatchFrozenVector() {
    assertEquals(FROZEN_TOPIC_V1, EdgeFixtures.topicFor("413999999", "nmea_gps"));
    String payload = EdgeFixtures.gpsPayload();
    assertTrue(payload.contains("\"msg_id\":\"" + FROZEN_MSG_ID_V1 + "\""),
        "黄金载荷的 msg_id 必须等于冻结向量");
    assertTrue(payload.contains("\"mmsi\":\"413999999\""));
    assertTrue(payload.contains("\"type\":\"nmea_gps\""));
    assertTrue(payload.contains("sent_at"));
  }
}
