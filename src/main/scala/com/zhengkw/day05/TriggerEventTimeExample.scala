package com.zhengkw.day05

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ClassName:TriggerEventTimeExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/15下午 9:08
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object TriggerEventTimeExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val socketStream = env.socketTextStream("hadoop102", 9999, '\n')
    socketStream.map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1).toLong * 1000)
    })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger)
      .process(new MyWindowResult)
      .print
    env.execute()
  }

  class OneSecondIntervalTrigger extends Trigger[(String, Long), TimeWindow] {
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      // 如果firstSeen为false，也就是当碰到第一条元素的时候
      if (!firstSeen.value()) {
        // 假设第一条事件来的时候，机器时间是1234ms，t是多少？t是2000ms
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        //        val t = element._2 + (1000 - (element._2 % 1000))
        ctx.registerEventTimeTimer(t) // 在2000ms注册一个定时器
        ctx.registerEventTimeTimer(window.getEnd) // 在窗口结束时间注册一个定时器
        firstSeen.update(true)
      }

      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println("回调函数触发时间：" + time)
      if (time == window.getEnd) {
        TriggerResult.FIRE_AND_PURGE
      } else {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        if (t < window.getEnd) {
          ctx.registerEventTimeTimer(t)
        }
        TriggerResult.FIRE
      }
    }
/**
* @descrption: 当窗口闭合执行clear逻辑！
 * @param window
 * @param ctx 
* @return: void
* @date: 20/06/16 上午 8:49
* @author: zhengkw
*/
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      // SingleTon, 单例模式，只会被初始化一次
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }

  class MyWindowResult extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("key为 " + key + " 的窗口中元素的数量是 " + elements.size)
    }
  }

}
