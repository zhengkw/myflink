package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:KeyByReduceExampleFromDoc
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:30
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KeyByReduceExampleFromDoc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env
      .fromElements(
        ("en", List("tea")),
        ("fr", List("vin")),
        ("en", List("cake"))
      )

    inputStream
      .keyBy(_._1)
      // `:::`用来拼接列表
      .reduce((r1, r2) => (r1._1, r1._2 ::: r2._2))
      .print()

    env.execute()
  }
}
