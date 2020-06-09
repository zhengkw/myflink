package com.zhengkw.day02

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:FlatMapExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 10:50
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new SensorSource)
    env.setParallelism(1)
    // 用flatMap实现 map操作
    stream.flatMap(new FlatMapFunction[SensorReading, String] {
      override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
        out.collect(value.id)
      }
    }).print
    // 用flatMap实现 filter操作
    stream.flatMap(new FlatMapFunction[SensorReading, String] {
      override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
        if (value.id.equals("sensor_1")) out.collect(value.id)
      }
    }).print

    env.execute()
  }
}
