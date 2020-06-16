package com.zhengkw.day06

import java.text.SimpleDateFormat

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @ClassName:ListStateExample
 * @author: zhengkw
 * @description:
 * 每隔10s打印一次sensor1的个数！
 * @date: 20/06/16上午 11:14
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new MyProcess)
      .print()

    env.execute()
  }

  /**
   * @descrption: 创建两个状态变量。
   *              1将sensor1保存到状态变量中
   *              2将定时器的定时时间存放到状态变量中
   * @date: 20/06/16 上午 11:21
   * @author: zhengkw
   */
  class MyProcess extends KeyedProcessFunction[String, SensorReading, String] {

    var listState: ListState[SensorReading] = _
    var timerTs: ValueState[Long] = _


    /**
     * @descrption: //在open声明周期中进行初始化！
     * @return: void
     * @date: 20/06/16 下午 2:06
     * @author: zhengkw
     */
    override def open(parameters: Configuration): Unit = {
      //init
      listState = getRuntimeContext.getListState(
        new ListStateDescriptor[SensorReading]("list_state", classOf[SensorReading])
      )
      timerTs = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("timer", classOf[Long])
      )
    }

    /**
     * @descrption: 定时器 定时触发process方法
     * @param timestamp
     * @param ctx
     * @param out
     * @return: void
     * @date: 20/06/16 下午 2:07
     * @author: zhengkw
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp))
      val list: ListBuffer[SensorReading] = ListBuffer()
      import scala.collection.JavaConversions._
      for (l <- listState.get()) {
        list += l
      }
      listState.clear() //GC
      out.collect("列表状态变量中的元素数量有 " + list.size + " 个")
      timerTs.clear()
    }

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      listState.add(value) // 将到来的传感器数据添加到列表状态变量
      if (timerTs.value() == 0L) {
        val ts = ctx.timerService().currentProcessingTime() + 10 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        timerTs.update(ts)
      }
    }


  }

}
