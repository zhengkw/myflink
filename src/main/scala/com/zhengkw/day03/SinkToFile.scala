package com.zhengkw.day03

import com.zhengkw.StreamingJob
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @ClassName:SinkToFile
 * @author: zhengkw
 * @description:
 * @date: 20/06/18下午 3:46
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SinkToFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从txt文件中获取数据源
    val stream = env.readTextFile("source/sensor.txt")
    //直接输出到另一个文件中
    stream.addSink(
      StreamingFileSink.forRowFormat(
        //给定文件路径
        new Path("source/out"),//文件夹名字 文件名自动生成！
        new SimpleStringEncoder[String]() //空参默认UTF-8
      ).build()
    )
    env.execute()

  }
}
