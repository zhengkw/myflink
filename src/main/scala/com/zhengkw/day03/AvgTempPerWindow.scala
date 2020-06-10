package com.zhengkw.day03

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName:AvgTempPerWindow
 * @author: zhengkw
 * @description: 增量聚合
 * @date: 20/06/10下午 2:12
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AvgTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
    stream
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTempFunction)
      .print()
    env.execute()
  }

  class AvgTempFunction extends AggregateFunction[(String, Double), (String, Double, Long), (String, Double)] {
    //创建1个累加器
    override def createAccumulator(): (String, Double, Long) = ("", 0.0, 0L)

    //累加逻辑
    override def add(value: (String, Double), accumulator: (String, Double, Long)): (String, Double, Long) = {
     //每来一条加一次
      (value._1, accumulator._2 + value._2, accumulator._3 + 1)
    }
    //输出逻辑
    override def getResult(accumulator: (String, Double, Long)): (String, Double) = {
      (accumulator._1, accumulator._2 / accumulator._3)
    }

    //会话窗口会调用merge
    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }

}
