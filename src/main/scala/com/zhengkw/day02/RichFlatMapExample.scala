package com.zhengkw.day02

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:RichFlatMapExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 4:21
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object RichFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(1, 2, 3)
    stream
      .flatMap(new MyFlatMap)
      .print()

    env.execute()
  }

  class MyFlatMap extends RichFlatMapFunction[Int, Int] {
    override def flatMap(value: Int, out: Collector[Int]): Unit = {
      println(s"index：${getRuntimeContext.getIndexOfThisSubtask}")
      out.collect(value + 1)
    }

    override def open(parameters: Configuration): Unit = println("this is init")

    override def close(): Unit = println("it is closed!")
  }

}
