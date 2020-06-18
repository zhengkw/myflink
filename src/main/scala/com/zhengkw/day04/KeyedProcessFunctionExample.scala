package com.zhengkw.day04

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @ClassName:KeyedProcessFunctionExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/18下午 10:38
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KeyedProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
     //如果keyby传入的是字符串那么process里面key的类型必须是javaTuple
      .keyBy("id")
      .process(new TempIncreWarning(10 * 1000L))

    stream.print()
    env.execute()
  }

  class TempIncreWarning(time: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {
   /**
   * @descrption: 每来一个元素处理一次！
   * @return: void
   * @date: 20/06/18 下午 10:43
   * @author: zhengkw
   */
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {

    }
  }

}
