package com.zhengkw.day02

import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SourceFromCustomDataSource
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 9:43
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SourceFromCustomDataSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //添加自定义流
    val source = env.addSource(new SensorSource)
    source.print

    env.execute()

  }
}
