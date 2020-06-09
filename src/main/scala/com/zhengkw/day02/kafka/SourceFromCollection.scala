package com.zhengkw.day02.kafka

import com.zhengkw.day02.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SourceFromCollection
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 1:03
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SourceFromCollection {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718199, 15.402984393403084),
        SensorReading("sensor_7", 1547718199, 6.720945201171228),
        SensorReading("sensor_10", 1547718199, 38.101067604893444)
      )).print()
    env.execute()
  }
}
