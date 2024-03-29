package com.zhengkw.day02

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:MapExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 11:02
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    // `MyMapFunction`实现了`MapFunction`接口
    stream.map(new MyMapFunction).print()

    // 使用匿名类的方式实现`MapFunction`接口
    stream
      .map(
        new MapFunction[SensorReading, String] {
          override def map(value: SensorReading): String = value.id
        }
      )
      .print()

    // 使用匿名函数的方式抽取传感器ID
    stream.map(r => r.id).print()

    env.execute()
  }

  class MyMapFunction extends MapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = value.id
  }
}
