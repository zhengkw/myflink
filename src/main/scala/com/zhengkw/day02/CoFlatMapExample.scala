package com.zhengkw.day02

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:CoFlatMapExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 2:36
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CoFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val one: DataStream[(Int, Long)] = env.fromElements((1, 1L))
    val two: DataStream[(Int, String)] = env.fromElements((1, "two"))

    // 将key相同的联合到一起
    val connected: ConnectedStreams[(Int, Long), (Int, String)] = one.keyBy(_._1)
      .connect(two.keyBy(_._1))

    val printed: DataStream[String] = connected
      .flatMap(new MyCoFlatMap)

    printed.print

    env.execute()
  }

  class MyCoFlatMap extends CoFlatMapFunction[(Int, Long), (Int, String), String] {
    override def flatMap1(value: (Int, Long), out: Collector[String]): Unit = {
      out.collect(value._2.toString + "来自第一条流")
      out.collect(value._2.toString + "来自第一条流")
    }

    override def flatMap2(value: (Int, String), out: Collector[String]): Unit = {
      out.collect(value._2 + "来自第二条流")
    }
  }
}
