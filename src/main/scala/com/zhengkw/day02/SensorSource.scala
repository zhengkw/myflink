package com.zhengkw.day02

import java.util.Calendar
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import scala.util.Random

/**
 * @ClassName:SensorSource
 * @author: zhengkw
 * @description:
 * @date: 20/06/09上午 9:30
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
class SensorSource extends RichParallelSourceFunction[SensorReading] {
  // 表示数据源是否正在运行，`true`表示正在运行
  var running: Boolean = true

  // `run`函数会连续不断的发送`SensorReading`数据
  // 使用`SourceContext`来发送数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化随机数发生器，用来产生随机的温度读数
    val rand = new Random

    // 初始化10个(温度传感器ID，温度读数)元组
    // `(1 to 10)`从1遍历到10
    var curFTemp = (1 to 10).map(
      // 使用高斯噪声产生温度读数
      i => ("sensor_" + i, 65 + (rand.nextGaussian() * 20))
    )

    // 无限循环，产生数据流
    while (running) {
      // 更新温度
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))

      // 获取当前的时间戳，单位是ms
      val curTime = Calendar.getInstance.getTimeInMillis

      // 调用`SourceContext`的`collect`方法来发射出数据
      // Flink的算子向下游发送数据，基本都是`collect`方法
      curFTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      // 100ms发送一次数据
      Thread.sleep(100)
    }
  }

  /**
   * @descrption:   当取消任务时，关闭无限循环
   * @return: void
   * @date: 20/06/11 下午 5:04
   * @author: zhengkw
   */

  override def cancel(): Unit = running = false
}