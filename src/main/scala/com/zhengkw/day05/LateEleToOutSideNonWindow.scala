package com.zhengkw.day05

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

import javafx.scene.input.DataFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:TriggerEventTimeExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/15上午 11:29
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object LateEleToOutSideNonWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("hadoop102", 9999, '\n')
    val result = stream.map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1).toLong * 1000L)
    })
      .assignAscendingTimestamps(_._2)
      .process(new MyProcessFunction)

    result.print()
    //打印侧输出
    result.getSideOutput(new OutputTag[String]("late")).print()


    env.execute()
  }

  class MyProcessFunction extends ProcessFunction[(String, Long), String] {
    val data = new OutputTag[String]("late")

    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), String]#Context, out: Collector[String]): Unit = {
      println(s"${value._2}--------${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ctx.timerService().currentWatermark())}-----------${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ctx.timerService().currentProcessingTime())}")
      //  println(ctx.timerService().currentWatermark())

      if (value._2 < ctx.timerService().currentWatermark()) {
        //   out.collect(" data late!")
        //  "侧输出打印，这个数据是迟到数据"
        ctx.output(data, s"${value._1}是迟到数据")
      } else {
        out.collect("没有数据迟到")
      }
    }
  }

}





