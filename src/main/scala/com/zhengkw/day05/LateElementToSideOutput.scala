package com.zhengkw.day05

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ClassName:LateElementToSideOutput
 * @author: zhengkw
 * @description:
 * @date: 20/06/15下午 4:36
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object LateElementToSideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val input = env
      .socketTextStream("hadoop102", 9999, '\n')
      .map(line => {
        var arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .sideOutputLateData(new OutputTag[(String, Long)]("late"))
      .process(new PrintFunction)
    input.print
    input.getSideOutput(new OutputTag[(String, Long)]("late")).print()
    env.execute()
  }

  class PrintFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(context.window.getStart + "到" + context.window.getEnd + "的窗口闭合了！")
    }
  }

}
