package com.zhengkw.day08

import com.zhengkw.day02.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}

/**
 * @ClassName:CountTempBySQL
 * @author: zhengkw
 * @description: 10S统计一次对应id 的个数
 * @date: 20/06/19上午 10:34
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CountTempBySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.addSource(
      new SensorSource
    ).assignAscendingTimestamps(_.timestamp)
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val dataTable = tableEnv.fromDataStream(stream, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)
    tableEnv.sqlQuery(
      "select id,count(id) from " + dataTable + " Group by id,tumble(ts,interval '10' second)"
    ).toRetractStream[(String, Long)]
      .print()
    env.execute()
  }
}
