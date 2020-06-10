package com.zhengkw.day03

import com.zhengkw.day02.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ClassName:AvgTempPerWindowByProcessWindowFunction
 * @author: zhengkw
 * @description: 全窗口函数
 * @date: 20/06/10下午 2:49
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object AvgTempPerWindowByProcessWindowFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
    stream
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new AvgTempFunc)
      .print()

    env.execute()
  }

  /**
   * @descrption: 全窗口聚合函数
   * @return:
   * @date: 20/06/10 下午 2:51
   * @author: zhengkw
   */
  class AvgTempFunc extends ProcessWindowFunction[(String, Double), (String, Double), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double)], out: Collector[(String, Double)]): Unit = {
      val size = elements.size
      var sum = 0.0
      for (r <- elements) sum += r._2
      out.collect((key, sum / size))
    }

  }

}
