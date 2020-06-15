package com.zhengkw.day05

import java.sql.Timestamp

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ClassName:TriggerExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/15上午 11:29
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new SensorSource)
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger)
      .process(new MyWindowResult)
      .print
    env.execute()
  }
}

class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
  override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val firstSeen = ctx.getPartitionedState(
      new ValueStateDescriptor[Boolean]("first-seen", classOf[Boolean])
    )
    if (!firstSeen.value()) {
      //取整数秒！假设第一条时间来时，机器时间是1234ms
      // 得到t为 1000+（1234-234）
      val t = ctx.getCurrentProcessingTime + (1000 - ctx.getCurrentProcessingTime % 1000)
      ctx.registerProcessingTimeTimer(t) //2000ms注册定时器
      ctx.registerProcessingTimeTimer(window.getEnd) //窗口结束设置定时器
      firstSeen.update(true)
    }
    TriggerResult.CONTINUE

  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("回调函数触发时间：" + new Timestamp(time))
    if (time == window.getEnd) {
      TriggerResult.FIRE_AND_PURGE
    } else {
      val t = ctx.getCurrentProcessingTime + (1000 - ctx.getCurrentProcessingTime % 1000)
      if (t < window.getEnd) {
        ctx.registerProcessingTimeTimer(t)
      }
      TriggerResult.FIRE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    //如果远端有 first-seen 那么clear中的firstSeen和clear中的指向同一个引用
    val firstSeen = ctx.getPartitionedState(
      new ValueStateDescriptor[Boolean]("first-seen", classOf[Boolean])
    )
    firstSeen.clear()
  }
}

class MyWindowResult extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
    out.collect("传感器ID为 " + key + " 的传感器窗口中元素的数量是 " + elements.size)
  }
}

