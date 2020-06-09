package com.zhengkw.day01

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:MyWC4Socket
 * @author: zhengkw
 * @description:
 * @date: 20/06/08下午 12:26
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MyWC4Socket {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop102", 9999, '\n')
    val wc = stream.flatMap(line => {
      val strings = line.split(" ")
      val tuples = strings.map(word => WordCount(word, 1))
      tuples
    })
    val text = wc.keyBy("word")
      .sum("count")
    text.print()
    //执行
    env.execute()
  }
}

case class WordCount(word: String, count: Int)
