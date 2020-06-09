package com.zhengkw.day02

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:FilterExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:32
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object FilterExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    // 将传感器ID为`sensor_1`的过滤出来
    stream
      .filter(
        new FilterFunction[SensorReading] {
          override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
        }
      )
      .print()

    // 使用匿名函数
    stream.filter(r => r.id.equals("sensor_1")).print()

    stream.filter(new MyFilterFunction).print()

    env.execute()
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
  }

}
