package com.zhengkw.day03

import com.zhengkw.day02.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName:MinTempWindowExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 11:57
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MinTempWindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    stream.map(r => (r.id, r.temperature))
      .keyBy(_._1)
      //开窗滑动窗口
     .timeWindow(Time.seconds(5), Time.seconds(2))
      //固定窗口 滚动窗口
     // .timeWindow(Time.seconds(10))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))
      .print()
    env.execute()
  }
}
