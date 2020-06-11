package com.zhengkw.day04

import java.lang
import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ClassName:WatermarkTest
 * @author: zhengkw
 * @description:
 * @date: 20/06/11上午 9:56
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 应用程序使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 系统每隔一分钟的机器时间插入一次水位线
    // env.getConfig.setAutoWatermarkInterval(60000)
    val stream = env.socketTextStream("47.240.72.95", 9999, '\n')
    stream.map((line => {
      val arr = line.split(" ")
      // 第二个元素是时间戳，必须转换成毫秒单位
      (arr(0), arr(1).toLong * 1000)
    }))
      // 抽取时间戳和插入水位线
      // 插入水位线的操作一定要紧跟source算子
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
        /**
         * @descrption: 提取数据里的时间戳！
         * @param element
         * @return: long
         * @date: 20/06/11 上午 10:04
         * @author: zhengkw
         */
        override def extractTimestamp(element: (String, Long)): Long = element._2
      }).keyBy(_._1)
      // 10s的滚动窗口
      .timeWindow(Time.seconds(10))
      .process(new MyProcess)
      .print()
    env.execute()
  }
}

class MyProcess extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + " 的窗口中共有 " + elements.size + " 条数据")
}

