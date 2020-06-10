package com.zhengkw.day03

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ClassName:MinMaxTempByAggregateAndProcess
 * @author: zhengkw
 * @description: 增量聚合窗口函数和全窗口函数结合使用 实现minmax
 *               用全窗口函数只是为了获取 窗口信息
 *               全窗口函数in 为 增量聚合窗口函数的结果
 * @date: 20/06/10下午 3:33
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MinMaxTempByAggregateAndProcess {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      // 第一个参数：增量聚合，第二个参数：全窗口聚合
      .aggregate(new Agg, new WindowResult)
      .print()

    env.execute()
  }
}

class Agg extends AggregateFunction[SensorReading, (String, Double, Double), (String, Double, Double)] {
  /**
   * @descrption: 初始化 (id,min,max)
   * @return: scala.Tuple3<java.lang.String,java.lang.Object,java.lang.Object>
   * @date: 20/06/10 下午 3:39
   * @author: zhengkw
   */
  override def createAccumulator(): (String, Double, Double) = ("", Double.MaxValue, Double.MinValue)

  override def add(value: SensorReading, accumulator: (String, Double, Double)): (String, Double, Double) = {
    (value.id, value.temperature.min(accumulator._2), value.temperature.max(accumulator._3))
  }

  override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = accumulator

  override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
    (a._1, a._2.min(b._2), a._3.max(b._3))
  }
}

class WindowResult extends ProcessWindowFunction[(String, Double, Double),
  MinMaxTemp, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
    val end = context.window.getEnd
    val result = elements.head
    out.collect(MinMaxTemp(key, result._2, result._3, end))
  }
}

/*case class MinMaxTemp(id: String,
                      min: Double,
                      max: Double,
                      endTs: Long)*/

