package com.zhengkw.day06

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:FlatMapWithStateExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/16下午 6:08
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object FlatMapWithStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
      .keyBy(_.id)
      // 第一个参数是输出的元素的类型，第二个参数是状态变量的类型
      .flatMapWithState[(String, Double, Double), Double] {
        case (r: SensorReading, None) => {
          (List.empty, Some(r.temperature))
        }
        case (r: SensorReading, lastTemp: Some[Double]) => {
          val tempDiff = (r.temperature - lastTemp.get).abs
          if (tempDiff > 1.7) {
            (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
          } else {
            (List.empty, Some(r.temperature))
          }
        }
      }

    stream.print()
    env.execute()
  }
}
