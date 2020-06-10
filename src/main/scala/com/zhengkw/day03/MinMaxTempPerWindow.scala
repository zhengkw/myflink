package com.zhengkw.day03

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ClassName:MinMaxTempPerWindow
 * @author: zhengkw
 * @description:
 * @date: 20/06/10下午 3:27
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object MinMaxTempPerWindow {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
    stream
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempPerWindow)
      .print()

    env.execute()

  }
}

class HighAndLowTempPerWindow extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {
    //只要迭代器中的温度
    val temps = elements.map(_.temperature)
   //获取窗口信息
    val windowEnd = context.window.getEnd
    out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))

  }
}
/**
* @descrption:
*  id: String,  sensorid
 * min: Double, 最小温度
 * max: Double, 最大温度
 * endTs: Long 窗口结束时间！
 * @return:
* @date: 20/06/10 下午 3:32
* @author: zhengkw
*/
case class MinMaxTemp(id: String,
                      min: Double,
                      max: Double,
                      endTs: Long)