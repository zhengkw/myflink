package com.zhengkw.day03

import com.zhengkw.day02.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName:MinMaxTempByReduceAndProcess
 * @author: zhengkw
 * @description: reduce结合process 完成minmax
 * @date: 20/06/10下午 4:12
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MinMaxTempByReduceAndProcess {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      //因为要利用reduce所以先构造（id，temp，temp） 复制一份！
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      //reduce聚合
      .reduce(
        (r1: (String, Double, Double), r2: (String, Double, Double)) => {
          (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
        },
        new WindowResult
      )
      .print()

    env.execute()
  }
}

