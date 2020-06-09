package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:KeyByExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 11:50
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KeyByExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream  = env
      .addSource(new SensorSource) // --> DataStream[T]
      .keyBy(r => r.id) // --> KeyedStream[T, K]
      .min(2)  // --> DataStream[T]

    stream.print()
    env.execute()
  }
}
