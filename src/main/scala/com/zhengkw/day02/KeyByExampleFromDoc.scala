package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:KeyByExampleFromDoc
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:28
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KeyByExampleFromDoc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    val resultStream: DataStream[(Int, Int, Int)] = inputStream
      .keyBy(0) // 使用元组的第一个元素进行分组
      .sum(1)   // 累加元组第二个元素

    resultStream.print()
    env.execute()
  }
}
