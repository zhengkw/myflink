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
  var running = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    //随机数参数器
    val random = new Random
    //模拟10个传感器采集数据
    val curFTemper = (1 to 10).map(i => (
      //使用高斯噪声参数温度读数（华氏温度）
      s"sensor_$i", 65 + (random.nextGaussian())
    ))

    //无线循环，产生数据流
    while (running) {
      //更新温度
      curFTemper.map(t => (t._1, t._2 + (random.nextGaussian() * 0.5)))
      // 获取当前的时间戳，单位是ms
      val curTime = Calendar.getInstance.getTimeInMillis

      // 调用`SourceContext`的`collect`方法来发射出数据
      // Flink的算子向下游发送数据，基本都是`collect`方法
      curFTemper.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      // 100ms发送一次数据
      Thread.sleep(100)

    }

  }

  override def cancel(): Unit = running = false
}
