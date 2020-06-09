package com.zhengkw.day02

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:FlatMapExampleFromDoc
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 11:00
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object FlatMapExampleFromDoc {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "white", "black", "white", "gray", "black", "white"
      )

    stream
      .flatMap(
        new FlatMapFunction[String, String] {
          override def flatMap(value: String, out: Collector[String]): Unit = {
            if (value.equals("white")) {
              out.collect(value)
            } else if (value.equals("black")) {
             //复制操作！
              out.collect(value)
              out.collect(value)
            }
          }
        }
      )
      .print()

    env.execute()
  }

}
