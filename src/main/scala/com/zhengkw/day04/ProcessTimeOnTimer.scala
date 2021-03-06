package com.zhengkw.day04


import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:ProcessTimeOnTimer
 * @author: zhengkw
 * @description:
 * @date: 20/06/11下午 2:45
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object ProcessTimeOnTimer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val stream = env
      .socketTextStream("hadoop102", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1))
      })
      .keyBy(_._1)
      .process(new MyKeyedProcess)

    stream.print()
    env.execute()
  }

  class MyKeyedProcess extends KeyedProcessFunction[String, (String, String), String] {
    // 来一条数据调用一次
    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, out: Collector[String]): Unit = {
      // 当前机器时间
      val curTime = ctx.timerService().currentProcessingTime()
      // 当前机器时间10s之后，触发定时器
      ctx.timerService().registerProcessingTimeTimer(curTime + 10 * 1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("位于时间戳：" + new Timestamp(timestamp) + "的定时器触发了！")
    }
  }
}
