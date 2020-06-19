package com.zhengkw.day08

import com.zhengkw.day02.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

/**
 * @ClassName:TableAggregateFunctionExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/19下午 2:33
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object TableAggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    stream
      .filter(_.id.equals("sensor_1"))
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val table = tableEnv.fromDataStream(stream, 'id, 'timestamp as 'ts, 'temperature as 'temp)

  }

  /**
   * @descrption: 定义累加器
   *
   * @return:
   * @date: 20/06/19 下午 2:46
   * @author: zhengkw
   */
  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue //最高温度
    var secondHighestTemp: Double = Double.MinValue //第二高温度
  }

}
