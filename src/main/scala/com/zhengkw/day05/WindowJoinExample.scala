package com.zhengkw.day05

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName:WindowJoinExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/15下午 3:33
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WindowJoinExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置语义！
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //??????
    val orangeStream = env
      .fromElements((1, 999L), (1, 1000L))
      .assignAscendingTimestamps(_._2)

    val greenStream = env
      .fromElements((1, 1001L), (1, 1002L))
      .assignAscendingTimestamps(_._2)
    orangeStream.join(greenStream)
      //第一条流分组
      .where(r => r._1)
      //第二条流分组
      .equalTo(t => t._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply({
        (e1, e2) => e1 + "---" + e2
      })
      .print
    env.execute()
  }
}
