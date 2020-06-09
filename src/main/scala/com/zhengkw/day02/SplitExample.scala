package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SplitExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:37
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SplitExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[(Int, String)] = env
      .fromElements(
        (1001, "1001"),
        (999, "999")
      )

    val splitted: SplitStream[(Int, String)] = inputStream
      .split(t => if (t._1 > 1000) Seq("large") else Seq("small"))

    val large: DataStream[(Int, String)] = splitted.select("large")
    val small: DataStream[(Int, String)] = splitted.select("small")
    val all: DataStream[(Int, String)] = splitted.select("small", "large")

    all.print()

    env.execute()
  }
}
