package com.zhengkw.day04

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:KeyedProcessFunctionExample
 * @author: zhengkw
 * @description: 基于处理时间语义 完成10s内温度连续上升 报警！
 * @date: 20/06/18下午 10:38
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KeyedProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      //如果keyby传入的是字符串那么process里面key的类型必须是javaTuple
      .keyBy("id")
      .process(new TempIncreWarning(10 * 1000L))

    stream.print()
    env.execute()
  }

  class TempIncreWarning(time: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {
    //需要与上一个温度进行对比，看是否上升，所以需要将温度保存为状态
    lazy val lastTempState = getRuntimeContext.getState(
      //温度是double
      new ValueStateDescriptor[Double]("last-temp", classOf[Double])
    )
    //需要调用删除定时器的方法，该方法需要传入一个time，
    // 这个time是之前注册的时间戳！所以需要做成一个状态
    lazy val lastTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("last-timer", classOf[Long])
    )

    /**
     * @descrption: 每来一个元素处理一次！
     * @return: void
     * @date: 20/06/18 下午 10:43
     * @author: zhengkw
     */
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
      //首先取出状态
      val lastTemp = lastTempState.value()
      val lastTimer = lastTimerState.value()
      //将上次温度的状态进行更新
      lastTempState.update(value.temperature)
      //判断当前温度，如果比之前温度高，并且没有定时器的话，注册10s后的定时器
      if (value.temperature > lastTemp && lastTimer == 0L) {
        //创建一个定时器 获取一个定时器 time为TempIncreWarning构造器传入的参数！
        val ts = ctx.timerService().currentProcessingTime() + time
        ctx.timerService().registerProcessingTimeTimer(ts)
        //更新时间状态！
        lastTimerState.update(ts)
      } else if (value.temperature < lastTemp) {
        //如果温度下降删除定时器！
        ctx.timerService().deleteProcessingTimeTimer(lastTimer)
        //清空时间状态
        lastTimerState.clear()
      }

    }

    /**
     * @descrption: 定时器触发以后执行逻辑
     * @param timestamp
     * @param ctx
     * @param out
     * @return: void
     * @date: 20/06/18 下午 10:59
     * @author: zhengkw
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

    }
  }

}
