package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SourceFromFile
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 9:47
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SourceFromFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .readTextFile("E:\\IdeaWorkspace\\myflink\\src\\main\\resources\\sensor.txt")
      .map(r => {
        // 使用逗号切割字符串
        val arr = r.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    stream.print()
    env.execute()
  }
}
