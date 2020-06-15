package com.zhengkw.day05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @ClassName:MyCoProcessFunction
 * @author: zhengkw
 * @description:
 * @date: 20/06/15上午 10:16
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MyCoProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    env.execute()

  }
}
