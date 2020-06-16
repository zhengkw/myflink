package com.zhengkw.day06

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ClassName:UpdateWindowResultWithLateElement
 * @author: zhengkw
 * @description:
 * @date: 20/06/16上午 9:18
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object UpdateWindowResultWithLateElement {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("hadoop102", 9999, '\n')
    stream.map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1).toLong * 1000)
    })
      .assignTimestampsAndWatermarks(
        //传入允许最大延迟时间！
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          //提取事件中的时间戳
          override def extractTimestamp(element: (String, Long)): Long = element._2

        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(5))
      .process(new UpdateCountFunction)
      .print
    env.execute()
  }

  class UpdateCountFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      val count = elements.size //获取元素数量
      // 获取状态变量
      //状态变量作用域 该窗口
      val isUpdate = context.windowState.getState(
        new ValueStateDescriptor[Boolean]("is_update", classOf[Boolean])
      )
      //第一次状态变量为默认false，需要进入判断条件
      //更新状态变量
      if (!isUpdate.value()) {
        out.collect("当水位线超过窗口结束时间的时候，窗口第一次触发计算！元素数量是 " + count + " 个！")
        isUpdate.update(true)
      } else {
        // 迟到元素到来以后，更新窗口的计算结果
        out.collect("迟到元素来了！元素数量是 " + count + " 个！")
      }

    }
  }

}
