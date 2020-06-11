package com.zhengkw.day04

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * @ClassName:PeriodicInsertWatermarks
 * @author: zhengkw
 * @description:
 * @date: 20/06/11上午 11:57
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object PeriodicInsertWatermarks {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(
        new MyAssigner
      )
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new MyProcess)

    stream.print()
    env.execute()
  }

  // `BoundedOutOfOrdernessTimestampExtractor`的底层实现
  class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
    val bound: Long = 1000L // 最大延迟时间
    var maxTs: Long = Long.MinValue + bound // 观察到的最大时间戳

    // 每来一条元素就要调用一次
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
      maxTs = maxTs.max(element.timestamp)
      element.timestamp
    }

    // 产生水位线的函数，默认200ms调用一次
    override def getCurrentWatermark: Watermark = {
      // 水位线 = 观察到的最大时间戳 - 最大延迟时间
      new Watermark(maxTs - bound)
    }
  }

  class MyProcess extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect(elements.size.toString)
    }
  }
}
