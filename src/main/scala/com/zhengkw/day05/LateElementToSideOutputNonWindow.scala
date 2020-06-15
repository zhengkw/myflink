package com.zhengkw.day05

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:LateElementToSideOutputNonWindow
 * @author: zhengkw
 * @description:
 * @date: 20/06/15下午 4:55
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object LateElementToSideOutputNonWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input = env
      .socketTextStream("hadoop102", 9999, '\n')
      .map(line => {
        var arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .process(new LateToSideOutput)
    input.print()
    input.getSideOutput(new OutputTag[String]("late")).print()
    env.execute()
  }

  class LateToSideOutput extends ProcessFunction[(String, Long), String] {
    //侧输出标签
    val lateReadingOutPut = new OutputTag[String]("late")

    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), String]#Context, out: Collector[String]): Unit = {
      if (value._2 < ctx.timerService().currentWatermark()) {
        ctx.output(lateReadingOutPut, "迟到时间来了！")
      } else {
        out.collect("没有迟到的事件来了！")
      }
    }
  }

}
